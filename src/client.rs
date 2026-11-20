macro_rules! client_log {
    ($client:expr, $level:expr, $fmt:expr) => {
        $client.log(file!(), module_path!(), line!(), $level, format_args!($fmt))
    };
    ($client:expr, $level:expr, $fmt:expr, $($args:tt)*) => {
        $client.log(
            file!(),
            module_path!(),
            line!(),
            $level,
            format_args!($fmt, $($args)*),
        )
    };
}

use crate::command::{self, rx::RxCommand, tx::TxCommand};
use futures::sink::SinkExt;
use std::{fmt, net::SocketAddr, time::Duration};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_stream::StreamExt;
use tokio_util::{codec::Framed, sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

pub type SessionID = u32;

#[derive(sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "faction", rename_all = "lowercase")]
pub enum Faction {
    None = 0,
    Hsd = 1,
    Prf = 2,
    Rb = 3,
    Jar = 4,
}

impl fmt::Display for Faction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Faction::None => "N/A",
            Faction::Hsd => "HSD",
            Faction::Prf => "PRF",
            Faction::Rb => " RB",
            Faction::Jar => "JAR",
        })
    }
}

#[derive(sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "language", rename_all = "lowercase")]
pub enum Language {
    Ja = 0,
    En = 1,
}

impl fmt::Display for Language {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Language::Ja => "ja",
            Language::En => "en",
        })
    }
}

pub struct Client {
    codec: Framed<TcpStream, command::CampaignCodec>,
    session_id: SessionID,

    pub xuid: Uuid,
    pub xname: String,

    pub faction: Faction,
    pub language: Option<Language>,
    pub banned: bool,
}

impl Client {
    pub async fn send_command(&mut self, cmd: TxCommand) -> Result<(), ClientError> {
        self.codec
            .send(cmd)
            .await
            .map_err(|err| ClientError::CodecError(err))
    }

    pub async fn recv_command(&mut self) -> Result<RxCommand, ClientError> {
        let rx_timeout = Duration::from_secs(3);

        let command_opt = timeout(rx_timeout, self.codec.next())
            .await
            .map_err(|_| ClientError::Timeout)?;

        match command_opt {
            Some(cmd) => cmd.map_err(|err| ClientError::CodecError(err)),
            None => Err(ClientError::Disconnected),
        }
    }

    async fn close(&mut self) -> Result<(), ClientError> {
        self.codec
            .close()
            .await
            .map_err(|err| ClientError::CodecError(err))
    }

    pub fn log(&self, file: &str, module: &str, line: u32, level: usize, args: fmt::Arguments<'_>) {
        let mut fields = Vec::from([
            format!("CODE_LINE={}", line),
            format!("CODE_FILE={}", file),
            format!("CODE_MODULE={}", module),
            format!("PRIORITY={}", level),
            format!("SESSION_ID={:08x}", self.session_id),
        ]);

        if !self.xuid.is_nil() {
            fields.push(format!("XUID={}", self.xuid.simple()));
        }

        if !self.xname.is_empty() {
            fields.push(format!("XNAME={}", self.xname));
            fields.push(format!("TEAM={}", self.faction));
        }

        // The message should be the last argument
        fields.push(format!("MESSAGE={args}"));

        // TODO: Do something if this results in an error?
        let _ = systemd::journal::send(&fields.iter().map(|f| f.as_str()).collect::<Vec<&str>>());
    }
}

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Client did not send a command in time")]
    Timeout,

    #[error("Client disconnected")]
    Disconnected,

    #[error("Bad response")]
    BadResponse,

    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Codec error occured")]
    CodecError(#[from] command::CampaignCodecError),

    #[error("SQLx error occured")]
    SQLError(#[from] sqlx::Error),

    #[error("SystemD error occured")]
    SystemDError(#[from] systemd::Error),
}

async fn main_inner(client: &mut Client) -> Result<(), ClientError> {
    client_log!(client, 6, "Ping");
    client.send_command(TxCommand::Ping).await?;

    let rx_command_pong = client.recv_command().await?;
    client_log!(client, 6, "Recieved command: {}", rx_command_pong);

    if !matches!(rx_command_pong, RxCommand::Pong) {
        return Err(ClientError::BadResponse);
    }

    client_log!(client, 6, "XUID");
    client.send_command(TxCommand::XUID).await?;

    let rx_command_xuid = client.recv_command().await?;
    client_log!(client, 6, "Recieved command: {}", rx_command_xuid);

    client.xuid = match rx_command_xuid {
        RxCommand::XUID(xuid) => Uuid::from_u64_pair(0, xuid),
        _ => return Err(ClientError::BadResponse),
    };

    client_log!(client, 6, "Account lookup");
    {
        // Insert account into database if it doesn't already exist. Otherwise return return the account info
        let pg = crate::get_pool();

        #[derive(sqlx::FromRow)]
        struct Account {
            xname: Option<String>,
            faction: Faction,
            language: Option<Language>,
            banned: bool,
        }

        let account: Account = sqlx::query_as("
            WITH inserted AS (
                INSERT INTO accounts (xuid) VALUES ($1) ON CONFLICT (xuid) DO NOTHING RETURNING xname, faction, \"language\", banned
            ) SELECT * FROM inserted
            UNION ALL
            SELECT xname, faction, \"language\", banned FROM accounts WHERE xuid = $1
            ").bind(client.xuid)
            .fetch_one(pg)
            .await?;

        if let Some(xname) = account.xname {
            client.xname = xname;
        }
        client.faction = account.faction;
        client.language = account.language;
        client.banned = account.banned;
    }

    client_log!(client, 6, "Connection accepted");
    client.send_command(TxCommand::Accept).await?;

    loop {
        let rx_command = client.recv_command().await?;
        client_log!(client, 6, "Recieved command: {}", rx_command);

        let tx_command = match rx_command {
            // War Management
            RxCommand::TurnDays => command::turn_days(client).await,
            RxCommand::CampaignPeriod => command::campaign_period(client).await,
            RxCommand::WarTide { round, turn, map } => {
                command::war_tide(client, round, turn, map).await
            }
            RxCommand::WarInfluence { round, turn } => {
                command::war_influence(client, round, turn).await
            }
            RxCommand::RoundPoints { round } => command::round_points(client, round).await,

            // Mission Management
            RxCommand::Analyze { data } => command::analyze(client, data).await,
            RxCommand::AddUserPoint {
                round,
                turn,
                map,
                points,
            } => command::add_user_point(client, round, turn, map, points).await,
            RxCommand::AddMissionPoint {
                round,
                turn,
                map,
                points,
                faction,
            } => command::add_mission_point(client, round, turn, map, points, faction).await,

            // Message of the Day
            RxCommand::FreeMessage { language } => command::free_message(client, language).await,
            RxCommand::FixedMessage => command::fixed_message(client).await,

            // Pilot Management
            RxCommand::GetUserInfo => command::get_user_info(client).await,
            RxCommand::NewUserInfo {
                pilot_name,
                xlive_name,
                faction,
                pilot_data,
            } => command::new_user_info(client, pilot_name, xlive_name, faction, pilot_data).await,
            RxCommand::SetUserInfo { pilot_data } => {
                command::set_user_info(client, pilot_data).await
            }
            RxCommand::DelUserInfo => command::del_user_info(client).await,
            RxCommand::SetUserFaction { faction } => {
                command::set_user_faction(client, faction).await
            }

            // Garage Management
            RxCommand::VtOwned => command::vt_owned(client).await,
            RxCommand::VtAdd { vt_list } => command::vt_add(client, vt_list).await,
            RxCommand::VtDel { vt_list } => command::vt_del(client, vt_list).await,
            RxCommand::VtCapture {
                pilot_name,
                vt_list,
            } => command::vt_capture(client, pilot_name, vt_list).await,
            RxCommand::VtSortie { vt_list } => command::vt_sortie(client, vt_list).await,
            RxCommand::VtReturn { vt_list } => command::vt_return(client, vt_list).await,

            // Shop
            RxCommand::ShopList => command::shop_list(client).await,
            RxCommand::VtPotentialCost => command::vt_potential_cost(client).await,

            // Message Board
            RxCommand::MessageCount => command::message_count(client).await,
            RxCommand::MessagePost => command::message_post(client).await,

            // Leaderboard
            RxCommand::RankValues {
                round,
                turn,
                faction,
                category,
                start,
                count,
            } => command::rank_values(client, round, turn, faction, category, start, count).await,
            RxCommand::RankCountGlobal {
                round,
                turn,
                category,
            } => command::rank_count_global(client, round, turn, category).await,
            RxCommand::RankCountFaction {
                round,
                turn,
                category,
                faction,
            } => command::rank_count_faction(client, round, turn, category, faction).await,
            RxCommand::RankCountCategories { round, turn } => {
                command::rank_count_categories(client, round, turn).await
            }

            // Awards
            RxCommand::AwardAvailable => command::award_available(client).await,
            RxCommand::AwardPoints { is_turn } => command::award_points(client, is_turn).await,
            RxCommand::AwardRanks {
                round,
                turn,
                faction,
            } => command::award_ranks(client, round, turn, faction).await,
            RxCommand::AwardClaim {
                round,
                turn,
                vt_list,
                pilot_data,
            } => command::award_claim(client, round, turn, vt_list, pilot_data).await,

            // Trading
            RxCommand::TradeToken { pilot_name } => command::trade_token(client, pilot_name).await,
            RxCommand::TradeUserInfo {
                pilot_name,
                token,
                pilot_data,
            } => command::trade_user_info(client, pilot_name, token, pilot_data).await,
            RxCommand::TradeVT {
                token,
                pilot_name,
                rx_vt_list,
                tx_vt_list,
            } => command::trade_vt(client, token, pilot_name, rx_vt_list, tx_vt_list).await,

            _ => Err(ClientError::BadResponse),
        }?;

        client_log!(client, 6, "Sending command: {}", tx_command);
        client.send_command(tx_command).await?;
    }
}

async fn main(
    codec: Framed<TcpStream, command::CampaignCodec>,
    addr: SocketAddr,
    session_id: SessionID,
) {
    let mut client = Client {
        codec,
        session_id,

        xuid: Uuid::default(),
        xname: String::new(),
        faction: Faction::None,
        language: None,
        banned: false,
    };

    client_log!(client, 6, "Connected {}", addr);
    if let Err(err) = main_inner(&mut client).await {
        match err {
            ClientError::Disconnected => {} // Handled below
            ClientError::Timeout => {
                client_log!(client, 6, "Timeout");
                let _ = client.send_command(TxCommand::Retry).await;
            }

            // Errors
            ClientError::BadResponse => {
                client_log!(client, 5, "Bad Response");
                let _ = client.send_command(TxCommand::Reject).await;
            }
            ClientError::CodecError(codec_err) => {
                client_log!(client, 3, "Codec Error: {}", codec_err);
                let _ = client.send_command(TxCommand::Reject).await;
            }
            ClientError::SQLError(sql_err) => {
                client_log!(client, 3, "SQL Error: {}", sql_err);
                let _ = client.send_command(TxCommand::Reject).await;
            }
            ClientError::SystemDError(sysd_err) => {
                client_log!(client, 3, "SystemD Error: {}", sysd_err);
                let _ = client.send_command(TxCommand::Reject).await;
            }
            ClientError::Runtime(runtime_err) => {
                client_log!(client, 3, "Runtime Error: {}", runtime_err);
                let _ = client.send_command(TxCommand::Reject).await;
            }
        };
    }

    client_log!(client, 6, "Disconnect");
    let _ = client.close().await;
}

pub async fn listener(listener: TcpListener, cancel_token: CancellationToken) {
    let _ = systemd_log!(7, "Client Listener: Started");

    let tracker = TaskTracker::new();
    let mut session_id: SessionID = 0;
    loop {
        // Asynchronously wait for an incoming connection
        let (socket, addr) = tokio::select! {
            biased;

            _ = cancel_token.cancelled() => {
                break;
            }

            listener_res = listener.accept() => {
                match listener_res {
                    Err(err) => {
                        let _ = systemd_log!(2, "Client Listener (error): {}", err);
                        break;
                    }
                    Ok(v) => v,
                }
            }
        };

        // Spawn a new task to handle the connection concurrently
        let codec = Framed::new(socket, command::CampaignCodec);
        tracker.spawn(main(codec, addr, session_id));

        session_id = session_id.wrapping_add(1);
    }

    // Tracker must be closed before wait can be called
    tracker.close();
    tracker.wait().await;

    let _ = systemd_log!(7, "Client Listener: Stopped");
}
