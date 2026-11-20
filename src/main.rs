macro_rules! systemd_log {
    ($level:expr, $fmt:expr) => {
        systemd::journal::log($level, file!(), line!(), module_path!(), &format_args!($fmt))
    };
    ($level:expr, $fmt:expr, $($args:tt)*) => {
        systemd::journal::log($level, file!(), line!(), module_path!(), &format_args!($fmt, $($args)*))
    };
}

pub mod client;
pub mod command;
pub mod service;

use crate::command::VTID;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{
    net::{IpAddr, SocketAddr},
    path::Path,
    sync::OnceLock,
};
use systemd::daemon::{STATE_READY, STATE_STATUS, STATE_STOPPING, notify};
use tokio::{
    join,
    net::TcpListener,
    signal::unix::{SignalKind, signal},
};
use tokio_util::sync::CancellationToken;
use toml::Table;

#[derive(Debug)]
pub struct CampaignConfig {
    turn_days: u16,

    motd_en: String,
    motd_ja: String,

    round_end_points: u32,

    turn_points: Vec<u32>,
    round_points: Vec<u32>,

    hsd_turn_awards: Vec<VTID>,
    prf_turn_awards: Vec<VTID>,
    rb_turn_awards: Vec<VTID>,
    jar_turn_awards: Vec<VTID>,

    hsd_round_awards: Vec<VTID>,
    prf_round_awards: Vec<VTID>,
    rb_round_awards: Vec<VTID>,
    jar_round_awards: Vec<VTID>,

    shop_drop_count: u32,
}

fn toml_to_vtids(campaign_config: &Table, name: &str) -> Vec<VTID> {
    campaign_config[name]
        .as_array()
        .expect(format!("TOML is missing campaign.{}", name).as_str())
        .iter()
        .map(|v| {
            let id = u16::try_from(
                v.as_integer()
                    .expect(format!("campaign.{} has non-integer value", name).as_str()),
            )
            .expect("campaign.hsd_turn_awards has an invalid VT id");
            assert!(id != 4 && id < 31, "Invalid VT ID {}", id);
            id
        })
        .collect()
}

fn toml_to_points(campaign_config: &Table, name: &str) -> Vec<u32> {
    campaign_config[name]
        .as_array()
        .expect(format!("TOML is missing campaign.{}", name).as_str())
        .iter()
        .map(|v| {
            u32::try_from(
                v.as_integer()
                    .expect(format!("campaign.{} has non-integer value", name).as_str()),
            )
            .expect("campaign.hsd_turn_awards has an invalid VT id")
        })
        .collect()
}

impl CampaignConfig {
    fn new(campaign_config: &Table) -> Self {
        Self {
            turn_days: campaign_config
                .get("turn_days")
                .and_then(|v| v.as_integer())
                .and_then(|v| u16::try_from(v).ok())
                .unwrap_or(7),

            motd_en: campaign_config
                .get("motd_en")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned(),
            motd_ja: campaign_config
                .get("motd_ja")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned(),

            round_end_points: campaign_config["round_end_points"]
                .as_integer()
                .and_then(|v| u32::try_from(v).ok())
                .expect("TOML is missing campaign.round_end_points"),

            turn_points: toml_to_points(campaign_config, "turn_points"),
            round_points: toml_to_points(campaign_config, "round_points"),

            hsd_turn_awards: toml_to_vtids(campaign_config, "hsd_turn_awards"),
            prf_turn_awards: toml_to_vtids(campaign_config, "prf_turn_awards"),
            rb_turn_awards: toml_to_vtids(campaign_config, "rb_turn_awards"),
            jar_turn_awards: toml_to_vtids(campaign_config, "jar_turn_awards"),

            hsd_round_awards: toml_to_vtids(campaign_config, "hsd_round_awards"),
            prf_round_awards: toml_to_vtids(campaign_config, "prf_round_awards"),
            rb_round_awards: toml_to_vtids(campaign_config, "rb_round_awards"),
            jar_round_awards: toml_to_vtids(campaign_config, "jar_round_awards"),

            shop_drop_count: campaign_config["shop_drop_count"]
                .as_integer()
                .and_then(|v| u32::try_from(v).ok())
                .expect("TOML is missing campaign.shop_drop_count"),
        }
    }
}

static CONFIG: OnceLock<CampaignConfig> = OnceLock::new();
pub fn get_config() -> &'static CampaignConfig {
    CONFIG.get().unwrap()
}

static PG_POOL: OnceLock<PgPool> = OnceLock::new();
pub fn get_pool() -> &'static PgPool {
    PG_POOL.get().unwrap()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() <= 1 {
        let _ = systemd_log!(1, "usage: {} <config.toml>", args[0]);
        return Ok(());
    }

    let root_config: Table = {
        let _ = systemd_log!(7, "Loading config from: {}", args[1]);
        let config_str = std::fs::read_to_string(&args[1])?;
        config_str.parse()?
    };

    let campaign_config = root_config["campaign"]
        .as_table()
        .expect("TOML is missing campaign section")
        .to_owned();

    CONFIG.set(CampaignConfig::new(&campaign_config)).unwrap();

    let database_config = root_config["database"]
        .as_table()
        .expect("TOML is missing database section");

    PG_POOL
        .set(database_connect(database_config).await?)
        .unwrap();

    let network_config = root_config["network"]
        .as_table()
        .expect("TOML is missing network section");

    let listener = network_listen(network_config).await?;

    let cancel_token = CancellationToken::new();

    // Launch the service task
    let service_handle = tokio::spawn(service::main(cancel_token.clone()));

    // Launch the listener task to accept new clients
    let client_listener_handle = tokio::spawn(client::listener(listener, cancel_token.clone()));

    if !notify([(STATE_READY, "1"), (STATE_STATUS, "Running")].iter())? {
        eprintln!("Failed to contact SystemD service for Ready");
    }

    let mut sigterm = signal(SignalKind::terminate())?;
    sigterm.recv().await;

    if !notify([(STATE_STOPPING, "1"), (STATE_STATUS, "Stopping")].iter())? {
        eprintln!("Failed to contact SystemD service for Stopping");
    }

    cancel_token.cancel();

    let _ = join!(service_handle, client_listener_handle);
    return Ok(());
}

async fn database_connect(database_config: &Table) -> Result<PgPool, sqlx::Error> {
    let connect = PgConnectOptions::new()
        .username(
            database_config["username"]
                .as_str()
                .expect("TOML is missing database.username"),
        )
        .socket(Path::new(
            &database_config
                .get("socket")
                .and_then(|socket| socket.as_str())
                .unwrap_or("/var/run/postgresql"),
        ))
        .application_name(
            database_config["app_name"]
                .as_str()
                .expect("TOML is missing database.app_name"),
        )
        .database(
            database_config["database"]
                .as_str()
                .expect("TOML is missing database.database"),
        );

    return PgPoolOptions::new().connect_with(connect).await;
}

async fn network_listen(network_config: &Table) -> Result<TcpListener, Box<dyn std::error::Error>> {
    let bind_ip = {
        let bind_ip_str = network_config
            .get("bind_ip")
            .and_then(|v| v.as_str())
            .unwrap_or("::");
        bind_ip_str.parse::<IpAddr>()?
    };
    let bind_port = {
        let bind_port_int = network_config["bind_port"]
            .as_integer()
            .expect("TOML is missing network.bind_port");
        u16::try_from(bind_port_int)?
    };

    Ok(TcpListener::bind(SocketAddr::new(bind_ip, bind_port)).await?)
}
