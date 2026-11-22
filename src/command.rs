pub mod rx;
pub mod tx;

use crate::{
    client::{Client, ClientError, Faction, Language},
    get_config, get_pool,
};
use futures::future::join_all;
use sqlx::{self, Postgres, QueryBuilder};
use std::{
    array,
    collections::HashMap,
    ffi::CStr,
    fmt,
    sync::{Arc, LazyLock},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{Barrier, BarrierWaitResult, Mutex},
    time::timeout,
};
use tx::TxCommand;
use uuid::Uuid;

pub struct CampaignCodec;

#[derive(Error, Debug)]
pub enum CampaignCodecError {
    #[error("Invalid header")]
    InvalidHeader,

    #[error("Invalid faction id: {0}")]
    InvalidFaction(u8),

    #[error("Invalid language id: {0}")]
    InvalidLanguage(u8),

    #[error("Invalid category id: {0}")]
    InvalidCategory(u8),

    #[error("Pilot data length was wrong: {0}")]
    InvalidPilotDataLength(usize),

    #[error("War Tide does not sum to 100: HSD {0}, PRF {1}, RB {2}")]
    InvalidWarTide(u8, u8, u8),

    #[error("Ranks must either be 0, 1, or 2: HSD {0}, PRF {1}, RB {2}")]
    InvalidRanks(u8, u8, u8),

    #[error("MOTD is too long: {0}")]
    InvalidMOTD(usize),

    #[error("Name is invalid. length: {0}, is_ascii: {1}")]
    InvalidName(usize, bool),

    #[error("Invalid VT List. length: {0}")]
    InvalidListSize(usize),

    #[error("Not enough data in payload")]
    InsufficientData(#[from] bytes::TryGetError),

    #[error("UTF-8 error occured")]
    Utf8(#[from] std::str::Utf8Error),

    #[error("Disk I/O error occurred")]
    Io(#[from] std::io::Error),
}

const NAME_CSTR_LEN: usize = 16;

const PILOT_DATA_NAME_OFFSET: usize = 40;
pub const PILOT_DATA_LENGTH: u16 = 1816;
type PilotData = [u8; PILOT_DATA_LENGTH as usize];

pub fn pilot_data_decode_name(data: &PilotData) -> Result<String, CampaignCodecError> {
    let mut name_bytes = [0u8; NAME_CSTR_LEN + 1];
    name_bytes[0..NAME_CSTR_LEN]
        .copy_from_slice(&data[PILOT_DATA_NAME_OFFSET..PILOT_DATA_NAME_OFFSET + NAME_CSTR_LEN]);
    let name_str = CStr::from_bytes_until_nul(&name_bytes)
        .expect("Missing null byte")
        .to_str()?;
    return Ok(name_str.to_owned());
}

pub fn pilot_data_from_slice(data: &[u8]) -> Result<PilotData, CampaignCodecError> {
    data.try_into()
        .map_err(|_| CampaignCodecError::InvalidPilotDataLength(data.len()))
}

pub type VTID = u16;

pub struct VTEntry {
    id: VTID,
    serial: u32,
}

pub type VTEntryList = Vec<VTEntry>;

pub type VTIDList = Vec<VTID>;

pub struct ShopEntry {
    id: VTID,
    limit: u32,
    cost: u16,
}

pub type ShopEntryList = Vec<ShopEntry>;

pub enum RankCategory {
    None = 0,
    WeaponAccuracy = 1, // Awarded
    KillCount = 2,
    KillPoints = 3, // Awarded
    DeathCount = 4,
    MissionVictories = 5, // Awarded
    BaseCaptures = 6,     // Awarded
    CaptureCount = 7,
    Tippings = 8, // Number of times fallen over
}

impl fmt::Display for RankCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::None => "None",
            Self::WeaponAccuracy => "WeaponAccuracy",
            Self::KillCount => "KillCount",
            Self::KillPoints => "KillPoints",
            Self::DeathCount => "DeathCount",
            Self::MissionVictories => "MissionVictories",
            Self::BaseCaptures => "BaseCaptures",
            Self::CaptureCount => "CaptureCount",
            Self::Tippings => "Tippings",
        })
    }
}

impl RankCategory {
    fn query_sum_clause(&self) -> &'static str {
        match self {
            Self::None => "",

            Self::WeaponAccuracy => "(SUM(stat1) * 1000) as scoreNum, SUM(stat2) as scoreDiv",
            Self::KillPoints => "SUM(stat4) as scoreNum, 1 as scoreDiv",
            Self::MissionVictories => "(SUM(stat6) * 1000) as scoreNum, SUM(stat7) as scoreDiv",
            Self::BaseCaptures => "SUM(stat8) as scoreNum, 1 as scoreDiv",

            Self::KillCount => "SUM(stat3) as scoreNum, 1 as scoreDiv",
            Self::DeathCount => "SUM(stat5) as scoreNum, 1 as scoreDiv",
            Self::CaptureCount => "SUM(stat9) as scoreNum, 1 as scoreDiv",
            Self::Tippings => "SUM(stat10) as scoreNum, 1 as scoreDiv",
        }
    }

    fn query_where_clause(&self, is_turn: bool) -> &'static str {
        match self {
            Self::None => "",

            Self::WeaponAccuracy => {
                if is_turn {
                    "scoreDiv >= 50"
                } else {
                    "scoreDiv >= 200"
                }
            }
            Self::MissionVictories => {
                if is_turn {
                    "scoreDiv >= 5"
                } else {
                    "scoreDiv >= 20"
                }
            }

            Self::KillPoints
            | Self::BaseCaptures
            | Self::KillCount
            | Self::DeathCount
            | Self::CaptureCount
            | Self::Tippings => "scoreNum > 0",
        }
    }
}

pub struct RankEntry {
    rank: u32,
    pilot_name: String,
    xlive_name: String,
    score: u32,
}

pub type RankEntryList = Vec<RankEntry>;

pub struct AwardEntry {
    round: u16,
    turn: u16,
    id: VTID,
    faction: Faction,
}

pub type AwardEntryList = Vec<AwardEntry>;

pub struct AwardRankEntry {
    category: RankCategory,
    rank: u32,
}

pub type AwardRankEntryList = Vec<AwardRankEntry>;

const TURNS_PER_ROUND: u16 = 8;
const RB_START_TURN: u16 = 3;
const MAP_COUNT: usize = 24;

type CommandResult = Result<TxCommand, ClientError>;

// War Management
pub async fn turn_days(_client: &mut Client) -> CommandResult {
    //let pg = get_pool();

    // Recover sortied VTs from a disconnected match
    /*
    sqlx::query("UPDATE garage SET sortied = false WHERE xuid = $1")
        .bind(client.xuid)
        .execute(pg)
        .await?;
    */

    let cfg = get_config();

    Ok(TxCommand::TurnDays {
        days: cfg.turn_days,
    })
}

#[derive(sqlx::FromRow)]
struct RoundTurnTime {
    round: i16,
    turn: i16,
    remaining: i32,
}

pub async fn campaign_period(_client: &mut Client) -> CommandResult {
    let pg = get_pool();

    let round_turn_time: RoundTurnTime = sqlx::query_as("SELECT round, turn, EXTRACT(epoch FROM (period - now()))::int AS remaining FROM settings ORDER BY round DESC, turn DESC LIMIT 1").fetch_one(pg).await?;

    if round_turn_time.round < 0 {
        return Err(ClientError::Runtime("campaign_period.round < 0".to_owned()));
    }

    if round_turn_time.turn < 0 {
        return Err(ClientError::Runtime("campaign_period.turn < 0".to_owned()));
    }

    if round_turn_time.remaining < 0 {
        return Err(ClientError::Runtime(
            "campaign_period.remaining < 0".to_owned(),
        ));
    }

    let cfg = get_config();
    let turn_seconds = Duration::from_hours(cfg.turn_days as u64 * 24);

    // Add the remaining seconds in the current turn to the number of seconds left in all remaining turns
    let seconds = round_turn_time.remaining
        + (TURNS_PER_ROUND as i32 - round_turn_time.turn as i32) * turn_seconds.as_secs() as i32;

    Ok(TxCommand::CampaignPeriod {
        round: round_turn_time.round as u16,
        turn: round_turn_time.turn as u16,
        seconds,
    })
}

#[derive(sqlx::FromRow, Default)]
struct WarTide {
    hsd: i32,
    prf: i32,
    rb: i32,
}

fn war_tide_as_wins(war_tides: &[WarTide]) -> WarTide {
    let mut wins = WarTide {
        hsd: 0,
        prf: 0,
        rb: 0,
    };

    for tide in war_tides.iter() {
        if tide.hsd > tide.prf && tide.hsd > tide.rb {
            wins.hsd += 1;
        } else if tide.prf > tide.hsd && tide.prf > tide.rb {
            wins.prf += 1;
        } else if tide.rb > tide.prf && tide.rb > tide.hsd {
            wins.rb += 1;
        }
    }
    wins
}

pub async fn war_tide(_client: &mut Client, round: u16, turn: u16, map: u8) -> CommandResult {
    let pg = get_pool();

    let map_id = map as i16 + (turn as i16 - 1) * 3;

    let war_tide: WarTide =
        match sqlx::query_as("SELECT hsd, prf, rb FROM wartide WHERE round = $1 AND map = $2")
            .bind(round as i16)
            .bind(map_id)
            .fetch_one(pg)
            .await
        {
            Ok(wt) => wt,
            Err(sqlx::Error::RowNotFound) => WarTide::default(),
            err => err?,
        };

    // Give each faction some starting points so that early victories dont skew the pie-chart
    const STARTING_POINTS: f64 = 300.0;

    let hsd_points = war_tide.hsd as f64 + STARTING_POINTS;
    let prf_points = war_tide.prf as f64 + STARTING_POINTS;
    let rb_points = if turn >= RB_START_TURN {
        war_tide.rb as f64 + STARTING_POINTS
    } else {
        0.0
    };

    let total_points = hsd_points + prf_points + rb_points;

    let hsd = (100.0 * hsd_points / total_points) as u8;
    let prf = (100.0 * prf_points / total_points) as u8;
    let rb = (100.0 * rb_points / total_points) as u8;

    let error_percent = 100 - (hsd + prf + rb);

    Ok(TxCommand::WarTide {
        status: 0,
        hsd: hsd + error_percent,
        prf,
        rb,
    })
}

pub async fn war_influence(_client: &mut Client, round: u16, turn: u16) -> CommandResult {
    let pg = get_pool();

    let war_tides: Vec<WarTide> = if round > 0 {
        sqlx::query_as("SELECT hsd, prf, rb FROM wartide WHERE round = $1").bind(round as i16)
    } else {
        sqlx::query_as(
            "SELECT hsd, prf, rb FROM wartide WHERE round IN (SELECT MAX(round) FROM wartide)",
        )
    }
    .fetch_all(pg)
    .await?;

    let wins = war_tide_as_wins(&war_tides);

    let mut hsd_wins = wins.hsd as f64;
    let mut prf_wins = wins.prf as f64;
    let mut rb_wins = wins.rb as f64;

    let played_map_count = war_tides.iter().count();

    let mut unplayed = if played_map_count < MAP_COUNT {
        (MAP_COUNT - played_map_count) as f64
    } else {
        0.0
    };

    if turn >= RB_START_TURN {
        unplayed /= 3.0;
        hsd_wins += unplayed;
        prf_wins += unplayed;
        rb_wins += unplayed;
    } else {
        unplayed /= 2.0;
        hsd_wins += unplayed;
        prf_wins += unplayed;
    }

    let total_wins = hsd_wins + prf_wins + rb_wins;

    let hsd = (100.0 * hsd_wins / total_wins) as u8;
    let prf = (100.0 * prf_wins / total_wins) as u8;
    let rb = (100.0 * rb_wins / total_wins) as u8;

    let error_percent = 100 - (hsd + prf + rb);

    Ok(TxCommand::WarInfluence {
        status: 0,
        hsd: hsd + error_percent,
        prf,
        rb,
    })
}

pub async fn round_points(_client: &mut Client, round: u16) -> CommandResult {
    let pg = get_pool();

    let war_tides: Vec<WarTide> =
        sqlx::query_as("SELECT hsd, prf, rb FROM wartide WHERE round = $1")
            .bind(round as i16)
            .fetch_all(pg)
            .await?;

    let wins = war_tide_as_wins(&war_tides);

    // Ranks, 0 = 1st, 1 = 2nd, 2 = 3rd, all other values invalid
    let mut hsd = 2;
    let mut prf = 2;
    let mut rb = 2;

    if wins.hsd >= wins.prf {
        hsd -= 1;
    }
    if wins.hsd >= wins.rb {
        hsd -= 1;
    }

    if wins.prf >= wins.hsd {
        prf -= 1;
    }
    if wins.prf >= wins.rb {
        prf -= 1;
    }

    if wins.rb >= wins.hsd {
        rb -= 1;
    }
    if wins.rb >= wins.prf {
        rb -= 1;
    }

    let cfg = get_config();

    Ok(TxCommand::RoundPoints {
        status: 0,
        points: cfg.round_end_points,
        hsd,
        prf,
        rb,
    })
}

// Mission Management
pub async fn analyze(_client: &mut Client, _data: [u32; 10]) -> CommandResult {
    Ok(TxCommand::Analyze)
}

pub async fn add_user_point(
    client: &mut Client,
    round: u16,
    turn: u16,
    map: u8,
    points: [u32; 10],
) -> CommandResult {
    if matches!(client.faction, Faction::None) {
        return Err(ClientError::Runtime(
            "add_user_point.faction is invalid".to_owned(),
        ));
    }

    let pg = get_pool();

    let mut query = sqlx::query("
        INSERT INTO leaderboard (xuid, faction, round, turn, \"map\", stat1, stat2, stat3, stat4, stat5, stat6, stat7, stat8, stat9, stat10)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (xuid, faction, round, turn, \"map\") DO UPDATE SET
            stat1 = leaderboard.stat1 + excluded.stat1, stat2 = leaderboard.stat2 + excluded.stat2,
            stat3 = leaderboard.stat3 + excluded.stat3, stat4 = leaderboard.stat4 + excluded.stat4,
            stat5 = leaderboard.stat5 + excluded.stat5, stat6 = leaderboard.stat6 + excluded.stat6,
            stat7 = leaderboard.stat7 + excluded.stat7, stat8 = leaderboard.stat8 + excluded.stat8,
            stat9 = leaderboard.stat9 + excluded.stat9, stat10 = leaderboard.stat10 + excluded.stat10
        ").bind(client.xuid).bind(client.faction).bind(round as i16).bind(turn as i16).bind(map as i16);

    for p in points {
        query = query.bind(p as i64)
    }

    query.execute(pg).await?;

    Ok(TxCommand::AddUserPoint { status: 0 })
}

pub async fn add_mission_point(
    _client: &mut Client,
    round: u16,
    turn: u16,
    map: u8,
    points: u32,
    faction: Faction,
) -> CommandResult {
    let pg = get_pool();

    let map_id = map as i16 + (turn as i16 - 1) * 3;

    let query = match faction {
        Faction::Hsd => sqlx::query(
            "INSERT INTO wartide (round, \"map\", hsd) VALUES ($1, $2, $3) ON CONFLICT (round, \"map\") DO UPDATE SET hsd = wartide.hsd + excluded.hsd",
        ),
        Faction::Prf => sqlx::query(
            "INSERT INTO wartide (round, \"map\", prf) VALUES ($1, $2, $3) ON CONFLICT (round, \"map\") DO UPDATE SET prf = wartide.prf + excluded.prf",
        ),
        Faction::Rb => sqlx::query(
            "INSERT INTO wartide (round, \"map\", rb) VALUES ($1, $2, $3) ON CONFLICT (round, \"map\") DO UPDATE SET rb = wartide.rb + excluded.rb",
        ),
        _ => {
            return Err(ClientError::Runtime(
                "add_user_point.faction is invalid".to_owned(),
            ));
        }
    };

    query
        .bind(round as i16)
        .bind(map_id)
        .bind(points as i64)
        .execute(pg)
        .await?;

    Ok(TxCommand::AddMissionPoint { status: 0 })
}

pub async fn free_message(client: &mut Client, language: Language) -> CommandResult {
    let pg = get_pool();

    sqlx::query("UPDATE accounts SET language = $2 WHERE xuid = $1")
        .bind(client.xuid)
        .bind(language)
        .execute(pg)
        .await?;

    client.language = Some(language);

    let cfg = get_config();

    let message = match language {
        Language::Ja => cfg.motd_ja.as_str(),
        Language::En => cfg.motd_en.as_str(),
    };

    Ok(TxCommand::FreeMessage {
        status: 0,
        message: message.to_owned(),
    })
}

pub async fn fixed_message(client: &mut Client) -> CommandResult {
    let pg = get_pool();

    let round_turn_time: RoundTurnTime = sqlx::query_as("SELECT round, turn, EXTRACT(epoch FROM (period - now()))::int AS remaining FROM settings ORDER BY round DESC, turn DESC LIMIT 1").fetch_one(pg).await?;

    if round_turn_time.round < 0 {
        return Err(ClientError::Runtime("fixed_message.round < 0".to_owned()));
    }

    if round_turn_time.turn < 0 {
        return Err(ClientError::Runtime("fixed_message.turn < 0".to_owned()));
    }

    let remaining = Duration::from_secs(
        u64::try_from(round_turn_time.remaining)
            .map_err(|_| ClientError::Runtime("fixed_message.remaining < 0".to_owned()))?,
    );

    #[derive(sqlx::FromRow)]
    struct AwardCount {
        count: i64,
    }

    let award_count: AwardCount = sqlx::query_as("SELECT COUNT(*) FROM awards WHERE xuid = $1")
        .bind(client.xuid)
        .fetch_one(pg)
        .await?;

    let cfg = get_config();

    let mut messages = Vec::new();

    if matches!(client.faction, Faction::None) {
        if round_turn_time.turn == 1 {
            messages.push(0); // A new round has begun, pick a faction
        } else {
            messages.push(8); // You can now transfer to another faction
        }
    }

    if round_turn_time.turn > 1 && remaining > Duration::from_hours((cfg.turn_days as u64 - 2) * 24)
    {
        messages.push(1); // A new turn has begun
    }

    if remaining < Duration::from_hours(2 * 24) {
        if round_turn_time.turn == 8 {
            if remaining < Duration::from_hours(24) {
                messages.push(4); // Round is over
            }
            messages.push(3); // Time remaining in round
        } else {
            messages.push(2); // Time remaining in turn
        }
    }

    if round_turn_time.turn == 3 {
        messages.push(7); // RB appears
    } else if round_turn_time.turn == 5 {
        messages.push(9); // Jar appears
    }

    if award_count.count > 0 {
        messages.push(10); // Awards can be claimed
    }

    Ok(TxCommand::FixedMessage {
        status: 0,
        messages,
    })
}

// Pilot Management
pub async fn get_user_info(client: &mut Client) -> CommandResult {
    let pg = get_pool();

    #[derive(sqlx::FromRow)]
    struct PilotDataRow {
        pilot: Option<Vec<u8>>,
    }

    let pilot_data_row: PilotDataRow = sqlx::query_as("SELECT pilot FROM accounts WHERE xuid = $1")
        .bind(client.xuid)
        .fetch_one(pg)
        .await?;

    if let Some(pilot_bytes) = pilot_data_row.pilot {
        let pilot_data = pilot_data_from_slice(&pilot_bytes)?;
        let pilot_name = pilot_data_decode_name(&pilot_data)?;

        Ok(TxCommand::GetUserInfo {
            status: 0,
            pilot_name,
            faction: client.faction,
            pilot_data,
        })
    } else {
        Ok(TxCommand::GetUserInfo {
            status: 2,
            pilot_name: String::new(),
            faction: Faction::None,
            pilot_data: [0; _],
        })
    }
}

pub async fn new_user_info(
    client: &mut Client,
    _pilot_name: String,
    xlive_name: String,
    faction: Faction,
    pilot_data: PilotData,
) -> CommandResult {
    let pg = get_pool();

    sqlx::query("UPDATE accounts SET xname = $2, faction = $3, pilot = $4 WHERE xuid = $1")
        .bind(client.xuid)
        .bind(xlive_name)
        .bind(faction)
        .bind(&pilot_data)
        .execute(pg)
        .await?;

    client.faction = faction;

    Ok(TxCommand::NewUserInfo { status: 0 })
}

pub async fn set_user_info(client: &mut Client, pilot_data: PilotData) -> CommandResult {
    let pg = get_pool();

    sqlx::query("UPDATE accounts SET pilot = $2 WHERE xuid = $1")
        .bind(client.xuid)
        .bind(&pilot_data)
        .execute(pg)
        .await?;

    Ok(TxCommand::SetUserInfo { status: 0 })
}

pub async fn del_user_info(client: &mut Client) -> CommandResult {
    let pg = get_pool();

    let mut transaction = pg.begin().await?;

    sqlx::query("INSERT INTO graveyard (xuid, pilot, tod) SELECT xuid, pilot, NOW() FROM accounts WHERE xuid = $1")
        .bind(client.xuid)
        .execute(&mut *transaction)
        .await?;

    sqlx::query("UPDATE accounts SET faction = 'none', pilot = NULL WHERE xuid = $1")
        .bind(client.xuid)
        .execute(&mut *transaction)
        .await?;

    transaction.commit().await?;

    client.faction = Faction::None;

    Ok(TxCommand::DelUserInfo { status: 0 })
}

pub async fn set_user_faction(client: &mut Client, faction: Faction) -> CommandResult {
    let pg = get_pool();

    sqlx::query("UPDATE accounts SET faction = $2 WHERE xuid = $1")
        .bind(client.xuid)
        .bind(faction)
        .execute(pg)
        .await?;

    client.faction = faction;

    // Return status is always 1 to kick the user out of Campaign so that their garage reloads correctly
    Ok(TxCommand::SetUserFaction { status: 1 })
}

#[derive(sqlx::FromRow)]
struct GarageTypeSerial {
    #[sqlx(rename = "type")]
    id: i16,
    serial: i64,
}

// Garage Management
pub async fn vt_owned(client: &mut Client) -> CommandResult {
    let pg = get_pool();

    let garage_list: Vec<GarageTypeSerial> = sqlx::query_as(
        "SELECT type, serial FROM garage WHERE sortied = false AND faction = $2 AND xuid = $1",
    )
    .bind(client.xuid)
    .bind(client.faction)
    .fetch_all(pg)
    .await?;

    let vt_list: VTEntryList = garage_list
        .iter()
        .map(|vt| VTEntry {
            id: vt.id as u16,
            serial: vt.serial as u32,
        })
        .collect();

    Ok(TxCommand::VtOwned { status: 0, vt_list })
}

pub async fn vt_add(client: &mut Client, vt_list_in: VTIDList) -> CommandResult {
    let mut vt_add_counts: HashMap<VTID, u16> = HashMap::with_capacity(35);

    // Count the number of each VT type to add
    for &id in vt_list_in.iter() {
        *vt_add_counts.entry(id).or_insert(0) += 1;
    }

    let mut vt_list = Vec::new();

    let pg = get_pool();

    #[derive(sqlx::FromRow)]
    struct ShopSerialStock {
        #[sqlx(rename = "type")]
        id: i16,
        serial: i64,
        vtstock: Option<i16>,
    }

    let mut transaction = pg.begin().await?;

    // Fetch all relevant VT entries from the shop and lock them to prevent duplicate VTs from being created
    let mut shop_query_builder: QueryBuilder<Postgres> =
        QueryBuilder::new("SELECT \"type\", \"serial\", vtstock FROM shop WHERE \"type\" IN (");

    let mut shop_query_separated = shop_query_builder.separated(", ");
    for &id in vt_list_in.iter() {
        shop_query_separated.push_bind(id as i16);
    }
    shop_query_separated.push_unseparated(") FOR UPDATE");

    let shop_query = shop_query_builder.build_query_as::<ShopSerialStock>();
    let shop_serial_stocks = shop_query.fetch_all(&mut *transaction).await?;

    for serial_stocks in shop_serial_stocks.iter() {
        let count = vt_add_counts[&(serial_stocks.id as u16)];

        // Insufficient stock to complete this transaction
        if let Some(vtstock) = serial_stocks.vtstock
            && vtstock < count as i16
        {
            return Err(ClientError::Runtime(
                "vt_add insufficient vt stock".to_owned(),
            ));
        }

        // Update the serial number and the remaining VT stock in the shop
        sqlx::query("UPDATE shop SET \"serial\" = \"serial\" + $2, vtstock = GREATEST(vtstock - $3, 0) WHERE type = $1").bind(serial_stocks.id).bind(count as i64).bind(count as i16).execute(&mut *transaction).await?;

        // Insert the new VTs into the garage
        for i in 0..count {
            let serial = serial_stocks.serial + i as i64;
            sqlx::query(
                "INSERT INTO garage (xuid, \"type\", \"serial\", faction) VALUES ($1, $2, $3, $4)",
            )
            .bind(client.xuid)
            .bind(serial_stocks.id)
            .bind(serial)
            .bind(client.faction)
            .execute(&mut *transaction)
            .await?;

            vt_list.push(VTEntry {
                id: serial_stocks.id as u16,
                serial: serial as u32,
            });
        }
    }

    transaction.commit().await?;

    Ok(TxCommand::VtAdd { status: 0, vt_list })
}

pub async fn vt_del(client: &mut Client, vt_list_in: VTEntryList) -> CommandResult {
    let pg = get_pool();

    // Execute all queries in parallel. We want all of them to execute even if one fails
    let mut query_futures = Vec::with_capacity(vt_list_in.len());
    for vt in vt_list_in {
        query_futures.push(
        sqlx::query(
            "DELETE FROM garage WHERE xuid = $1 AND faction = $2 AND \"type\" = $3 AND \"serial\" = $4",
        ).bind(client.xuid).bind(client.faction).bind(vt.id as i16).bind(vt.serial as i64).execute(pg));
    }

    for query_result in join_all(query_futures).await {
        query_result?;
    }

    Ok(TxCommand::VtDel {
        status: 0,
        vt_list: Vec::new(),
    })
}

pub async fn vt_capture(
    client: &mut Client,
    pilot_name: String,
    vt_list_in: VTEntryList,
) -> CommandResult {
    let pg = get_pool();

    // Execute all queries in parallel. We want all of them to execute even if one fails
    let mut query_futures = Vec::with_capacity(vt_list_in.len());
    for vt in vt_list_in.iter() {
        query_futures.push(
            sqlx::query(
                "
                INSERT INTO garage (\"type\", \"serial\", xuid, faction)
                VALUES ($3, $4, $1, $2)
                ON CONFLICT (\"type\", \"serial\")
                DO UPDATE SET sortied = false, faction = excluded.faction, xuid = excluded.xuid
                ",
            )
            .bind(client.xuid)
            .bind(client.faction)
            .bind(vt.id as i16)
            .bind(vt.serial as i64)
            .execute(pg),
        );
    }

    for query_result in join_all(query_futures).await {
        query_result?;
    }

    Ok(TxCommand::VtCapture {
        status: 0,
        pilot_name,
        vt_list: vt_list_in,
    })
}

pub async fn vt_sortie(client: &mut Client, vt_list_in: VTEntryList) -> CommandResult {
    let pg = get_pool();

    let mut transaction = pg.begin().await?;

    for vt in vt_list_in {
        sqlx::query(
            "UPDATE garage SET sortied = true WHERE xuid = $1 AND faction = $2 AND \"type\" = $3 AND serial = $4"
        )
        .bind(client.xuid)
        .bind(client.faction)
        .bind(vt.id as i16)
        .bind(vt.serial as i64)
        .execute(&mut *transaction).await?;
    }

    transaction.commit().await?;

    Ok(TxCommand::VtSortie { status: 0 })
}

pub async fn vt_return(client: &mut Client, vt_list_in: VTEntryList) -> CommandResult {
    let pg = get_pool();

    let mut transaction = pg.begin().await?;

    for vt in vt_list_in.iter() {
        sqlx::query(
            "UPDATE garage SET sortied = false WHERE xuid = $1 AND faction = $2 AND \"type\" = $3 AND serial = $4"
        )
        .bind(client.xuid)
        .bind(client.faction)
        .bind(vt.id as i16)
        .bind(vt.serial as i64)
        .execute(&mut *transaction).await?;
    }

    transaction.commit().await?;

    Ok(TxCommand::VtReturn {
        status: 0,
        vt_list: vt_list_in,
    })
}

// Shop
pub async fn shop_list(client: &mut Client) -> CommandResult {
    let pg = get_pool();

    #[derive(sqlx::FromRow)]
    struct ShopCostStock {
        #[sqlx(rename = "type")]
        id: i16,
        cost: i32,
        vtstock: Option<i16>,
    }

    let shop_cost_stocks: Vec<ShopCostStock> = sqlx::query_as("SELECT \"type\", \"cost\", vtstock FROM shop WHERE faction = $1 AND COALESCE(vtstock, 1) > 0 AND turn <= (SELECT turn FROM settings ORDER BY round DESC, turn DESC LIMIT 1)").bind(client.faction).fetch_all(pg).await?;

    let mut shop_list: ShopEntryList = Vec::with_capacity(shop_cost_stocks.len());

    for cost_stock in shop_cost_stocks {
        shop_list.push(ShopEntry {
            id: cost_stock.id as u16,
            cost: cost_stock.cost as u16,
            limit: cost_stock.vtstock.map(|v| v as u32).unwrap_or(u32::MAX),
        });
    }

    Ok(TxCommand::ShopList {
        status: 0,
        shop_list,
    })
}

pub async fn vt_potential_cost(_client: &mut Client) -> CommandResult {
    Ok(TxCommand::VtPotentialCost)
}

// Message Board
pub async fn message_count(_client: &mut Client) -> CommandResult {
    // Message board is disabled, return an error code
    Ok(TxCommand::MessageCount {
        status: 1,
        start: 0,
        count: 0,
    })
}
pub async fn message_post(_client: &mut Client) -> CommandResult {
    // Message board is disabled, return an error code
    Ok(TxCommand::MessagePost { status: 1, id: 0 })
}

// Leaderboard
pub async fn rank_values(
    _client: &mut Client,
    round: u16,
    turn: u16,
    faction: Faction,
    category: RankCategory,
    start: u32,
    count: u16,
) -> CommandResult {
    let pg = get_pool();

    let is_turn = turn > 0;

    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT xname, pilot, (scoreNum / scoreDiv)::smallint as score FROM (SELECT xuid, ",
    );

    query_builder.push(category.query_sum_clause());

    query_builder.push(" FROM leaderboard WHERE round = ");
    query_builder.push_bind(round as i16);

    if is_turn {
        query_builder.push(" AND turn = ");
        query_builder.push_bind(turn as i16);
    }

    if !matches!(faction, Faction::None) {
        query_builder.push(" AND faction = ");
        query_builder.push_bind(faction);
    }

    query_builder
        .push(" GROUP BY xuid) as lb INNER JOIN accounts ON lb.xuid = accounts.xuid WHERE ");
    query_builder.push(category.query_where_clause(is_turn));

    query_builder.push(" ORDER BY score DESC LIMIT ");
    query_builder.push_bind(count as i32);

    query_builder.push(" OFFSET ");
    query_builder.push_bind(start as i32 - 1);

    #[derive(sqlx::FromRow)]
    struct RankScore {
        xname: String,
        pilot: Vec<u8>,
        score: i16,
    }

    let query = query_builder.build_query_as::<RankScore>();
    let rank_scores = query.fetch_all(pg).await?;

    let mut rank_list = Vec::new();
    for (i, rank_score) in rank_scores.iter().enumerate() {
        let rank = start + i as u32;

        let pilot_data = pilot_data_from_slice(&rank_score.pilot)?;
        let pilot_name = pilot_data_decode_name(&pilot_data)?;

        rank_list.push(RankEntry {
            rank,
            pilot_name,
            xlive_name: rank_score.xname.clone(),
            score: rank_score.score as u32,
        });
    }

    Ok(TxCommand::RankValues {
        status: 0,
        rank_list,
    })
}

async fn rank_count_internal(
    count_opt: Option<&mut u32>,
    place_opt: Option<(&mut u32, Uuid)>,
    round: u16,
    turn: u16,
    category: RankCategory,
    faction: Faction,
) -> Result<(), ClientError> {
    let pg = get_pool();
    let is_turn = turn > 0;

    if let Some(count) = count_opt {
        let mut query_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("SELECT COUNT(*) FROM (SELECT ");

        query_builder.push(category.query_sum_clause());

        query_builder.push(" FROM leaderboard WHERE round = ");
        query_builder.push_bind(round as i16);

        if is_turn {
            query_builder.push(" AND turn = ");
            query_builder.push_bind(turn as i16);
        }

        if !matches!(faction, Faction::None) {
            query_builder.push(" AND faction = ");
            query_builder.push_bind(faction);
        }

        query_builder.push(" GROUP BY xuid) as lb WHERE ");
        query_builder.push(category.query_where_clause(is_turn));

        #[derive(sqlx::FromRow)]
        struct ScoreCount {
            count: i64,
        }

        let query = query_builder.build_query_as::<ScoreCount>();
        let score_count = query.fetch_one(pg).await?;

        *count = score_count.count as u32;
    }

    if let Some((place, xuid)) = place_opt {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "SELECT \"rank\" FROM (SELECT xuid, ROW_NUMBER() OVER(ORDER BY (scoreNum / scoreDiv) DESC) \"rank\" FROM (SELECT xuid, ",
        );

        query_builder.push(category.query_sum_clause());

        query_builder.push(" FROM leaderboard WHERE round = ");
        query_builder.push_bind(round as i16);

        if is_turn {
            query_builder.push(" AND turn = ");
            query_builder.push_bind(turn as i16);
        }

        if !matches!(faction, Faction::None) {
            query_builder.push(" AND faction = ");
            query_builder.push_bind(faction);
        }

        query_builder.push(" GROUP BY xuid) as lb WHERE ");
        query_builder.push(category.query_where_clause(is_turn));

        query_builder.push(" ) as lb_rank WHERE xuid = ");
        query_builder.push_bind(xuid);

        #[derive(sqlx::FromRow)]
        struct ScoreRank {
            rank: i64,
        }

        let query = query_builder.build_query_as::<ScoreRank>();
        let score_rank = match query.fetch_one(pg).await {
            Ok(sc) => sc,
            Err(sqlx::Error::RowNotFound) => ScoreRank { rank: 0 },
            err => err?,
        };

        *place = score_rank.rank as u32;
    }

    Ok(())
}

pub async fn rank_count_global(
    client: &mut Client,
    round: u16,
    turn: u16,
    category: RankCategory,
) -> CommandResult {
    let mut count = 0;
    let mut place = 0;
    rank_count_internal(
        Some(&mut count),
        Some((&mut place, client.xuid)),
        round,
        turn,
        category,
        Faction::None,
    )
    .await?;

    Ok(TxCommand::RankCountGlobal {
        status: 0,
        place,
        count,
    })
}

pub async fn rank_count_faction(
    client: &mut Client,
    round: u16,
    turn: u16,
    category: RankCategory,
    faction: Faction,
) -> CommandResult {
    let mut count = 0;
    let mut place = 0;
    rank_count_internal(
        Some(&mut count),
        Some((&mut place, client.xuid)),
        round,
        turn,
        category,
        faction,
    )
    .await?;

    Ok(TxCommand::RankCountFaction {
        status: 0,
        place,
        count,
    })
}

pub async fn rank_count_categories(_client: &mut Client, round: u16, turn: u16) -> CommandResult {
    let mut count = 0;
    rank_count_internal(
        Some(&mut count),
        None,
        round,
        turn,
        RankCategory::WeaponAccuracy,
        Faction::None,
    )
    .await?;

    Ok(TxCommand::RankCountCategories {
        status: 0,
        categories: array::from_fn(|_| count),
    })
}

// Awards
pub async fn award_available(client: &mut Client) -> CommandResult {
    let pg = get_pool();
    let cfg = get_config();

    #[derive(sqlx::FromRow)]
    struct AwardRow {
        round: i16,
        turn: i16,
        rank1: Option<i32>,
        rank2: Option<i32>,
        rank3: Option<i32>,
        rank4: Option<i32>,
    }

    let award_rows: Vec<AwardRow> = sqlx::query_as("SELECT round, turn, rank1, rank2, rank3, rank4 FROM awards WHERE xuid = $1 ORDER BY round ASC, turn ASC LIMIT 128").bind(client.xuid).fetch_all(pg).await?;

    const AWARD_NO_VT: VTID = 35;

    let mut award_list = Vec::new();
    for award in award_rows {
        if let Some(rank_val) = award
            .rank1
            .min(award.rank2.min(award.rank3.min(award.rank4)))
        {
            if rank_val < 0 {
                continue;
            }

            let vt_ids: &[VTID] = if award.turn >= 6 {
                // End of Turn Award
                match client.faction {
                    Faction::None => &[],
                    Faction::Hsd => &cfg.hsd_turn_awards,
                    Faction::Prf => &cfg.prf_turn_awards,
                    Faction::Rb => &cfg.rb_turn_awards,
                    Faction::Jar => &cfg.jar_turn_awards,
                }
            } else if award.turn == 0 {
                // End of Round Award
                match client.faction {
                    Faction::None => &[],
                    Faction::Hsd => &cfg.hsd_round_awards,
                    Faction::Prf => &cfg.prf_round_awards,
                    Faction::Rb => &cfg.rb_round_awards,
                    Faction::Jar => &cfg.jar_round_awards,
                }
            } else {
                &[]
            };

            award_list.push(AwardEntry {
                round: award.round as u16,
                turn: award.turn as u16,
                id: *vt_ids.get(rank_val as usize - 1).unwrap_or(&AWARD_NO_VT),
                faction: client.faction,
            })
        }
    }

    Ok(TxCommand::AwardAvailable {
        status: 0,
        award_list,
    })
}

pub async fn award_points(_client: &mut Client, is_turn: bool) -> CommandResult {
    let cfg = get_config();

    let points = if is_turn {
        cfg.turn_points.clone()
    } else {
        cfg.round_points.clone()
    };

    Ok(TxCommand::AwardPoints { status: 0, points })
}

pub async fn award_ranks(
    client: &mut Client,
    round: u16,
    turn: u16,
    _faction: Faction,
) -> CommandResult {
    let pg = get_pool();

    #[derive(sqlx::FromRow)]
    struct AwardRow {
        rank1: Option<i32>,
        rank2: Option<i32>,
        rank3: Option<i32>,
        rank4: Option<i32>,
    }

    let award_row: AwardRow = sqlx::query_as("SELECT rank1, rank2, rank3, rank4 FROM awards WHERE round = $2 AND turn = $3 AND xuid = $1").bind(client.xuid).bind(round as i16).bind(turn as i16).fetch_one(pg).await?;

    let mut award_list = Vec::new();

    if let Some(rank) = award_row.rank1 {
        award_list.push(AwardRankEntry {
            category: RankCategory::WeaponAccuracy,
            rank: rank as u32,
        })
    }

    if let Some(rank) = award_row.rank2 {
        award_list.push(AwardRankEntry {
            category: RankCategory::KillCount,
            rank: rank as u32,
        })
    }

    if let Some(rank) = award_row.rank3 {
        award_list.push(AwardRankEntry {
            category: RankCategory::MissionVictories,
            rank: rank as u32,
        })
    }

    if let Some(rank) = award_row.rank4 {
        award_list.push(AwardRankEntry {
            category: RankCategory::BaseCaptures,
            rank: rank as u32,
        })
    }

    Ok(TxCommand::AwardRanks {
        status: 0,
        award_list,
    })
}

pub async fn award_claim(
    client: &mut Client,
    round: u16,
    turn: u16,
    vt_list_in: VTIDList,
    pilot_data: PilotData,
) -> CommandResult {
    let pg = get_pool();

    let mut transaction = pg.begin().await?;

    // Claim the award
    sqlx::query("DELETE FROM awards WHERE xuid = $1 AND round = $2 AND turn = $3")
        .bind(client.xuid)
        .bind(round as i16)
        .bind(turn as i16)
        .execute(&mut *transaction)
        .await?;

    // Update pilot
    sqlx::query("UPDATE accounts SET pilot = $2 WHERE xuid = $1")
        .bind(client.xuid)
        .bind(pilot_data)
        .execute(&mut *transaction)
        .await?;

    // Only update VTs if requested
    let mut vt_list = Vec::new();
    if !vt_list_in.is_empty() {
        let mut vt_add_counts: HashMap<VTID, u16> = HashMap::with_capacity(35);

        // Count the number of each VT type to add
        for &id in vt_list_in.iter() {
            *vt_add_counts.entry(id).or_insert(0) += 1;
        }

        #[derive(sqlx::FromRow)]
        struct ShopSerial {
            #[sqlx(rename = "type")]
            id: i16,
            serial: i64,
        }

        // Fetch all relevant VT entries from the shop and lock them to prevent duplicate VTs from being created
        let mut shop_query_builder: QueryBuilder<Postgres> =
            QueryBuilder::new("SELECT \"type\", \"serial\" FROM shop WHERE \"type\" IN (");

        let mut shop_query_separated = shop_query_builder.separated(", ");
        for &id in vt_list_in.iter() {
            shop_query_separated.push_bind(id as i16);
        }
        shop_query_separated.push_unseparated(") FOR UPDATE");

        let shop_query = shop_query_builder.build_query_as::<ShopSerial>();
        let shop_serial_stocks = shop_query.fetch_all(&mut *transaction).await?;

        for serial_stocks in shop_serial_stocks.iter() {
            let count = vt_add_counts[&(serial_stocks.id as u16)];

            // Update the serial number in the shop
            sqlx::query("UPDATE shop SET \"serial\" = \"serial\" + $2 WHERE type = $1")
                .bind(serial_stocks.id)
                .bind(count as i64)
                .execute(&mut *transaction)
                .await?;

            // Insert the new VTs into the garage
            for i in 0..count {
                let serial = serial_stocks.serial + i as i64;
                sqlx::query(
                "INSERT INTO garage (xuid, \"type\", \"serial\", faction) VALUES ($1, $2, $3, $4)",
            )
            .bind(client.xuid)
            .bind(serial_stocks.id)
            .bind(serial)
            .bind(client.faction)
            .execute(&mut *transaction)
            .await?;

                vt_list.push(VTEntry {
                    id: serial_stocks.id as u16,
                    serial: serial as u32,
                });
            }
        }
    }

    transaction.commit().await?;

    Ok(TxCommand::AwardClaim { status: 0, vt_list })
}

// Trading
type TradeToken = u32;
static TRADE_TOKEN_CURRENT: Mutex<TradeToken> = Mutex::const_new(0);

#[derive(Clone, Copy, PartialEq, Eq)]
enum TradeType {
    UserInfo,
    VtList,
}

struct Trade {
    trade_type: TradeType,
    barrier: Arc<Barrier>,
}

static TRADE_BARRIERS: LazyLock<Mutex<HashMap<TradeToken, Trade>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn trade_token(_client: &mut Client, _pilot_name: String) -> CommandResult {
    // Get the next trade token
    let mut counter = TRADE_TOKEN_CURRENT.lock().await;
    let token = *counter;
    *counter = token.wrapping_add(1);

    Ok(TxCommand::TradeToken { status: 0, token })
}

async fn trade_wait_for_partner(
    trade_type: TradeType,
    token: TradeToken,
) -> Result<BarrierWaitResult, ClientError> {
    let barrier = {
        let mut trades = TRADE_BARRIERS.lock().await;

        // If a trade with this token already exists, then it is returned. Otherwise a new trade is created
        let trade = trades.entry(token).or_insert(Trade {
            trade_type,
            barrier: Arc::new(Barrier::new(2)),
        });

        // If the trade_type for this trade doesn't match then bail out. The trade partner will simply timeout
        if trade.trade_type != trade_type {
            return Err(ClientError::Runtime("Trade type missmatch".to_owned()));
        }

        trade.barrier.clone() // Drop/Unlock the TRADE_BARRIERS mutex but retain a reference to the barrier itself
    };

    // WARNING: Ensure that the TRADE_BARRIERS mutex is released before barrier.wait() is invoked. This prevents a deadlock
    let duration = Duration::from_secs(5);
    let barrier_result = timeout(duration, barrier.wait()).await;

    {
        // Cleanup the trade. If the token is already removed then this is a no-op
        let mut trades = TRADE_BARRIERS.lock().await;
        trades.remove(&token);
    }

    barrier_result.map_err(|_| ClientError::Timeout)
}

pub async fn trade_user_info(
    client: &mut Client,
    _pilot_name: String,
    token: u32,
    pilot_data: PilotData,
) -> CommandResult {
    trade_wait_for_partner(TradeType::UserInfo, token).await?;

    let pg = get_pool();

    sqlx::query("UPDATE accounts SET pilot = $2 WHERE xuid = $1")
        .bind(client.xuid)
        .bind(pilot_data)
        .execute(pg)
        .await?;

    Ok(TxCommand::TradeUserInfo {
        status: 0,
        delay: 0,
    })
}

pub async fn trade_vt(
    client: &mut Client,
    token: u32,
    _pilot_name: String,
    rx_vt_list: VTEntryList,
    _tx_vt_list: VTEntryList,
) -> CommandResult {
    trade_wait_for_partner(TradeType::VtList, token).await?;

    let pg = get_pool();

    for vt in rx_vt_list.iter() {
        sqlx::query(
            "UPDATE garage SET sortied = false, faction = $2, xuid = $1 WHERE type = $3 AND serial = $4",
        ).bind(client.xuid).bind(client.faction).bind(vt.id as i16).bind(vt.serial as i64).execute(pg).await?;
    }

    Ok(TxCommand::TradeVT {
        status: 0,
        vt_list: rx_vt_list,
    })
}
