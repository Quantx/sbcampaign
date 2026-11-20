use crate::{client::*, command::*};
use bytes::{Buf, BytesMut};
use num_enum::TryFromPrimitive;
use std::{ffi::CStr, fmt, io::Read};
use tokio_util::codec::Decoder;

#[derive(TryFromPrimitive)]
#[repr(u16)]
enum RxID {
    // Common
    Pong = 0x2010,
    XUID = 0x2020,

    // War Management
    TurnDays = 0x3030,
    CampaignPeriod = 0x1030,
    WarTide = 0x1031,
    WarInfluence = 0x1032,
    RoundPoints = 0x1028,

    // Mission Management
    Analyze = 0x1090,
    AddUserPoint = 0x5026,
    AddMissionPoint = 0x5027,

    // Message of the Day
    FreeMessage = 0x101A,
    FixedMessage = 0x101B,

    // Pilot Management
    GetUserInfo = 0x1021,
    NewUserInfo = 0x5022,
    SetUserInfo = 0x5024,
    DelUserInfo = 0x1023,
    SetUserFaction = 0x5025,

    // Garage Management
    VtOwned = 0x1040,
    VtAdd = 0x1042,
    VtDel = 0x1044,
    VtCapture = 0x1046,
    VtSortie = 0x1047,
    VtReturn = 0x3047,

    // Shop
    ShopList = 0x1041,
    VtPotentialCost = 0x1049,

    // Message Board
    MessageCount = 0x1050,
    MessagePost = 0x1052,

    // Leaderboard
    RankValues = 0x1060,
    RankCountGlobal = 0x1061,
    RankCountFaction = 0x1062,
    RankCountCategories = 0x1063,

    // Awards
    AwardAvailable = 0x7041,
    AwardPoints = 0x104C,
    AwardRanks = 0x3062,
    AwardClaim = 0x1048,

    // Trading
    TradeToken = 0x104A,
    TradeUserInfo = 0x104B,
    TradeVT = 0x1045,
}

pub enum RxCommand {
    // Common
    Pong,
    XUID(u64),

    // War Management
    TurnDays,
    CampaignPeriod,
    WarTide {
        round: u16,
        turn: u16,
        map: u8,
    },
    WarInfluence {
        round: u16,
        turn: u16,
    },
    RoundPoints {
        round: u16,
    },

    // Mission Management
    Analyze {
        data: [u32; 10],
    },
    AddUserPoint {
        round: u16,
        turn: u16,
        map: u8,
        points: [u32; 10],
    },
    AddMissionPoint {
        round: u16,
        turn: u16,
        map: u8,
        points: u32,
        faction: Faction,
    },

    // Message of the Day
    FreeMessage {
        language: Language,
    },
    FixedMessage,

    // Pilot Management
    GetUserInfo,
    NewUserInfo {
        pilot_name: String,
        xlive_name: String,
        faction: Faction,
        pilot_data: PilotData,
    },
    SetUserInfo {
        pilot_data: PilotData,
    },
    DelUserInfo,
    SetUserFaction {
        faction: Faction,
    },

    // Garage Management
    VtOwned,
    VtAdd {
        vt_list: VTIDList,
    },
    VtDel {
        vt_list: VTEntryList,
    },
    VtCapture {
        pilot_name: String,
        vt_list: VTEntryList,
    },
    VtSortie {
        vt_list: VTEntryList,
    },
    VtReturn {
        vt_list: VTEntryList,
    },

    // Shop
    ShopList,
    VtPotentialCost,

    // Message Board
    MessageCount,
    MessagePost,

    // Leaderboard
    RankValues {
        round: u16,
        turn: u16,
        faction: Faction,
        category: RankCategory,
        start: u32,
        count: u16,
    },
    RankCountGlobal {
        round: u16,
        turn: u16,
        category: RankCategory,
    },
    RankCountFaction {
        round: u16,
        turn: u16,
        category: RankCategory,
        faction: Faction,
    },
    RankCountCategories {
        round: u16,
        turn: u16,
    },

    // Awards
    AwardAvailable,
    AwardPoints {
        is_turn: bool,
    },
    AwardRanks {
        round: u16,
        turn: u16,
        faction: Faction,
    },
    AwardClaim {
        round: u16,
        turn: u16,
        vt_list: VTIDList,
        pilot_data: PilotData,
    },

    // Trading
    TradeToken {
        pilot_name: String,
    },
    TradeUserInfo {
        pilot_name: String,
        token: TradeToken,
        pilot_data: PilotData,
    },
    TradeVT {
        token: TradeToken,
        pilot_name: String,
        rx_vt_list: VTEntryList,
        tx_vt_list: VTEntryList,
    },
}

impl fmt::Display for RxCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Common
            RxCommand::Pong => f.write_str("Pong"),
            RxCommand::XUID(xuid) => write!(f, "XUID - ID {:016X}", xuid),

            // War Management
            RxCommand::TurnDays => f.write_str("TurnDays"),
            RxCommand::CampaignPeriod => f.write_str("CampaignPeriod"),
            RxCommand::WarTide { round, turn, map } => {
                write!(f, "WarTide - Round {}, Turn {}, Map {}", round, turn, map)
            }
            RxCommand::WarInfluence { round, turn } => {
                write!(f, "WarInfluence - Round {}, Turn {}", round, turn)
            }
            RxCommand::RoundPoints { round } => {
                write!(f, "RoundPoints - Round {}", round)
            }

            // Mission Management
            RxCommand::Analyze { data } => {
                f.write_str("Analyze - Data [")?;
                for d in data {
                    write!(f, "{}, ", d)?;
                }
                f.write_str("]")
            }
            RxCommand::AddUserPoint {
                round,
                turn,
                map,
                points,
            } => {
                write!(
                    f,
                    "AddUserPoint - Round {}, Turn {}, Map {}, Points [",
                    round, turn, map
                )?;
                for p in points {
                    write!(f, "{}, ", p)?;
                }
                f.write_str("]")
            }
            RxCommand::AddMissionPoint {
                round,
                turn,
                map,
                points,
                faction,
            } => {
                write!(
                    f,
                    "AddMissionPoint - Round {}, Turn {}, Map {}, Points {}, Faction {}",
                    round, turn, map, points, faction
                )
            }

            // Message of the Day
            RxCommand::FreeMessage { language } => {
                write!(f, "FreeMessage - Language {}", language)
            }
            RxCommand::FixedMessage => f.write_str("FixedMessage"),

            // Pilot Management
            RxCommand::GetUserInfo => f.write_str("GetUserInfo"),
            RxCommand::NewUserInfo {
                pilot_name,
                xlive_name,
                faction,
                ..
            } => {
                write!(
                    f,
                    "NewUserInfo - PilotName \"{}\", XLiveName \"{}\", Faction {}, <PilotData>",
                    pilot_name, xlive_name, faction
                )
            }
            RxCommand::SetUserInfo { .. } => f.write_str("SetUserInfo - <PilotData>"),
            RxCommand::DelUserInfo => f.write_str("DelUserInfo"),
            RxCommand::SetUserFaction { faction } => {
                write!(f, "SetUserFaction - Faction {}", faction)
            }

            // Garage Management
            RxCommand::VtOwned => f.write_str("VtOwned"),
            RxCommand::VtAdd { vt_list } => {
                write!(f, "VtAdd - VtListLen {}", vt_list.len())
            }
            RxCommand::VtDel { vt_list } => {
                write!(f, "VtDel - VtListLen {}", vt_list.len())
            }
            RxCommand::VtCapture {
                pilot_name,
                vt_list,
            } => {
                write!(
                    f,
                    "VtCapture - PilotName \"{}\", VtListLen {}",
                    pilot_name,
                    vt_list.len()
                )
            }
            RxCommand::VtSortie { vt_list } => {
                write!(f, "VtSortie - VtListLen {}", vt_list.len())
            }
            RxCommand::VtReturn { vt_list } => {
                write!(f, "VtReturn - VtListLen {}", vt_list.len())
            }

            // Shop
            RxCommand::ShopList => f.write_str("ShopList"),
            RxCommand::VtPotentialCost => f.write_str("VtPotentialCost"),

            // Message Board
            RxCommand::MessageCount => f.write_str("MessageCount"),
            RxCommand::MessagePost => f.write_str("MessagePost"),

            // Leaderboard
            RxCommand::RankValues {
                round,
                turn,
                faction,
                category,
                start,
                count,
            } => {
                write!(
                    f,
                    "RankValues - Round {}, Turn {}, Faction {}, Category {}, Start {}, Count {}",
                    round, turn, faction, category, start, count
                )
            }
            RxCommand::RankCountGlobal {
                round,
                turn,
                category,
            } => {
                write!(
                    f,
                    "RankCountGlobal - Round {}, Turn {}, Category {}",
                    round, turn, category
                )
            }
            RxCommand::RankCountFaction {
                round,
                turn,
                category,
                faction,
            } => {
                write!(
                    f,
                    "RankCountFaction - Round {}, Turn {}, Category {}, Faction {}",
                    round, turn, category, faction
                )
            }
            RxCommand::RankCountCategories { round, turn } => {
                write!(f, "RankCountCategories - Round {}, Turn {}", round, turn)
            }

            // Awards
            RxCommand::AwardAvailable => f.write_str("AwardAvailable"),
            RxCommand::AwardPoints { is_turn } => {
                write!(f, "AwardPoints - IsTurn {}", is_turn)
            }
            RxCommand::AwardRanks {
                round,
                turn,
                faction,
            } => {
                write!(
                    f,
                    "AwardRanks - Round {}, Turn {}, Faction {}",
                    round, turn, faction
                )
            }
            RxCommand::AwardClaim {
                round,
                turn,
                vt_list,
                ..
            } => {
                write!(
                    f,
                    "AwardClaim - Round {}, Turn {}, VtListLen {}, <PilotData>",
                    round,
                    turn,
                    vt_list.len()
                )
            }

            // Trading
            RxCommand::TradeToken { pilot_name } => {
                write!(f, "TradeToken - PilotName \"{}\"", pilot_name)
            }
            RxCommand::TradeUserInfo {
                pilot_name, token, ..
            } => {
                write!(
                    f,
                    "TradeToken - PilotName \"{}\", Token {:08X}, <PilotData>",
                    pilot_name, token
                )
            }
            RxCommand::TradeVT {
                token,
                pilot_name,
                rx_vt_list,
                tx_vt_list,
            } => {
                write!(
                    f,
                    "TradeToken - Token {}, PilotName \"{}\", RxVtListLen {}, TxVtListLen {}",
                    token,
                    pilot_name,
                    rx_vt_list.len(),
                    tx_vt_list.len()
                )
            }
        }
    }
}

fn decode_faction(id: u8) -> Result<Faction, CampaignCodecError> {
    match id {
        0 => Ok(Faction::None),
        1 => Ok(Faction::Hsd),
        2 => Ok(Faction::Prf),
        3 => Ok(Faction::Rb),
        4 => Ok(Faction::Jar),
        _ => Err(CampaignCodecError::InvalidFaction(id)),
    }
}

fn decode_language(id: u8) -> Result<Language, CampaignCodecError> {
    match id {
        0 => Ok(Language::Ja),
        1 => Ok(Language::En),
        _ => Err(CampaignCodecError::InvalidLanguage(id)),
    }
}

fn decode_category(id: u8) -> Result<RankCategory, CampaignCodecError> {
    match id {
        0 => Ok(RankCategory::None),
        1 => Ok(RankCategory::WeaponAccuracy),
        2 => Ok(RankCategory::KillCount),
        3 => Ok(RankCategory::KillPoints),
        4 => Ok(RankCategory::DeathCount),
        5 => Ok(RankCategory::MissionVictories),
        6 => Ok(RankCategory::BaseCaptures),
        7 => Ok(RankCategory::CaptureCount),
        8 => Ok(RankCategory::Tippings),
        _ => Err(CampaignCodecError::InvalidCategory(id)),
    }
}

fn decode_pilot_data<R: Buf + Read>(data: &mut R) -> Result<PilotData, CampaignCodecError> {
    let length = data.try_get_u16_le()?;
    if length != PILOT_DATA_LENGTH {
        return Err(CampaignCodecError::InvalidPilotDataLength(length as usize));
    }

    let mut pilot_data: PilotData = [0; _];
    data.read_exact(&mut pilot_data)?;

    return Ok(pilot_data);
}

fn decode_vt_entry_list<R: Buf + Read>(data: &mut R) -> Result<VTEntryList, CampaignCodecError> {
    let count = data.try_get_u16_le()? as usize;
    let mut list: VTEntryList = Vec::with_capacity(count);
    for _ in 0..count {
        list.push(VTEntry {
            id: data.try_get_u16_le()?,
            serial: data.try_get_u32_le()?,
        });
    }

    return Ok(list);
}

fn decode_vt_id_list<R: Buf + Read>(data: &mut R) -> Result<VTIDList, CampaignCodecError> {
    let count = data.try_get_u16_le()? as usize;
    let mut list: VTIDList = Vec::with_capacity(count);
    for _ in 0..count {
        list.push(data.try_get_u16_le()?);
    }

    return Ok(list);
}

fn decode_name<R: Buf + Read>(data: &mut R) -> Result<String, CampaignCodecError> {
    let mut name_bytes = [0u8; NAME_CSTR_LEN + 1];
    data.read_exact(&mut name_bytes[0..NAME_CSTR_LEN])?;
    let name_str = CStr::from_bytes_until_nul(&name_bytes)
        .expect("Missing null byte")
        .to_str()?;
    return Ok(name_str.to_owned());
}

impl Decoder for CampaignCodec {
    type Item = RxCommand;
    type Error = CampaignCodecError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            // Not enough data to read header
            return Ok(None);
        }

        let length = {
            let length_bytes: [u8; 2] = src[0..2].try_into().unwrap();
            u16::from_le_bytes(length_bytes)
        };

        let command = {
            let command_bytes: [u8; 2] = src[2..4].try_into().unwrap();
            RxID::try_from_primitive(u16::from_le_bytes(command_bytes))
                .map_err(|_| CampaignCodecError::InvalidHeader)?
        };

        let total_length = 4 + length as usize;

        if src.len() < length as usize {
            // Reserve all needed space
            src.reserve(total_length - src.len());
            return Ok(None);
        }

        let data_vec = src[4..total_length].to_vec();
        src.advance(total_length);

        let mut data = data_vec.as_slice();
        let command = match command {
            // Common
            RxID::Pong => RxCommand::Pong,
            RxID::XUID => RxCommand::XUID(data.try_get_u64_le()?),

            // War Management
            RxID::TurnDays => RxCommand::TurnDays,
            RxID::CampaignPeriod => RxCommand::CampaignPeriod,
            RxID::WarTide => RxCommand::WarTide {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
                map: data.try_get_u8()?,
            },
            RxID::WarInfluence => RxCommand::WarInfluence {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
            },
            RxID::RoundPoints => RxCommand::RoundPoints {
                round: data.try_get_u16_le()?,
            },

            // Mission Management
            RxID::Analyze => {
                let mut analyze_data = [0u32; 10];
                for i in 0..analyze_data.len() {
                    analyze_data[i] = data.try_get_u32_le()?;
                }
                RxCommand::Analyze { data: analyze_data }
            }
            RxID::AddUserPoint => {
                let round = data.try_get_u16_le()?;
                let turn = data.try_get_u16_le()?;
                let map = data.try_get_u8()?;
                let mut points = [0u32; 10];
                for i in 0..points.len() {
                    points[i] = data.try_get_u32_le()?;
                }
                RxCommand::AddUserPoint {
                    round,
                    turn,
                    map,
                    points,
                }
            }
            RxID::AddMissionPoint => RxCommand::AddMissionPoint {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
                map: data.try_get_u8()?,
                points: data.try_get_u32_le()?,
                faction: decode_faction(data.try_get_u8()?)?,
            },

            // Message of the Day
            RxID::FreeMessage => RxCommand::FreeMessage {
                language: decode_language(data.try_get_u8()?)?,
            },
            RxID::FixedMessage => RxCommand::FixedMessage,

            // Pilot Management
            RxID::GetUserInfo => RxCommand::GetUserInfo,
            RxID::NewUserInfo => RxCommand::NewUserInfo {
                pilot_name: decode_name(&mut data)?,
                xlive_name: decode_name(&mut data)?,
                faction: decode_faction(data.try_get_u8()?)?,
                pilot_data: decode_pilot_data(&mut data)?,
            },
            RxID::SetUserInfo => RxCommand::SetUserInfo {
                pilot_data: decode_pilot_data(&mut data)?,
            },
            RxID::DelUserInfo => RxCommand::DelUserInfo,
            RxID::SetUserFaction => RxCommand::SetUserFaction {
                faction: decode_faction(data.try_get_u8()?)?,
            },

            // Garage Management
            RxID::VtOwned => RxCommand::VtOwned,
            RxID::VtAdd => RxCommand::VtAdd {
                vt_list: decode_vt_id_list(&mut data)?,
            },
            RxID::VtDel => RxCommand::VtDel {
                vt_list: decode_vt_entry_list(&mut data)?,
            },
            RxID::VtCapture => RxCommand::VtCapture {
                pilot_name: decode_name(&mut data)?,
                vt_list: decode_vt_entry_list(&mut data)?,
            },
            RxID::VtSortie => RxCommand::VtSortie {
                vt_list: decode_vt_entry_list(&mut data)?,
            },
            RxID::VtReturn => RxCommand::VtReturn {
                vt_list: decode_vt_entry_list(&mut data)?,
            },

            // Shop
            RxID::ShopList => RxCommand::ShopList,
            RxID::VtPotentialCost => RxCommand::VtPotentialCost,

            // Message Board
            RxID::MessageCount => RxCommand::MessageCount,
            RxID::MessagePost => RxCommand::MessagePost,

            // Leaderboard
            RxID::RankValues => RxCommand::RankValues {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
                faction: decode_faction(data.try_get_u8()?)?,
                category: decode_category(data.try_get_u8()?)?,
                start: data.try_get_u32_le()?,
                count: data.try_get_u16_le()?,
            },
            RxID::RankCountGlobal => RxCommand::RankCountGlobal {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
                category: decode_category(data.try_get_u8()?)?,
            },
            RxID::RankCountFaction => RxCommand::RankCountFaction {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
                category: decode_category(data.try_get_u8()?)?,
                faction: decode_faction(data.try_get_u8()?)?,
            },
            RxID::RankCountCategories => RxCommand::RankCountCategories {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
            },

            // Awards
            RxID::AwardAvailable => RxCommand::AwardAvailable,
            RxID::AwardPoints => RxCommand::AwardPoints {
                is_turn: data.try_get_u8()? != 0,
            },
            RxID::AwardRanks => RxCommand::AwardRanks {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
                faction: decode_faction(data.try_get_u8()?)?,
            },
            RxID::AwardClaim => RxCommand::AwardClaim {
                round: data.try_get_u16_le()?,
                turn: data.try_get_u16_le()?,
                vt_list: decode_vt_id_list(&mut data)?,
                pilot_data: decode_pilot_data(&mut data)?,
            },

            // Trading
            RxID::TradeToken => RxCommand::TradeToken {
                pilot_name: decode_name(&mut data)?,
            },
            RxID::TradeUserInfo => RxCommand::TradeUserInfo {
                pilot_name: decode_name(&mut data)?,
                token: data.try_get_u32_le()?,
                pilot_data: decode_pilot_data(&mut data)?,
            },
            RxID::TradeVT => RxCommand::TradeVT {
                token: data.try_get_u32_le()?,
                pilot_name: decode_name(&mut data)?,
                rx_vt_list: decode_vt_entry_list(&mut data)?,
                tx_vt_list: decode_vt_entry_list(&mut data)?,
            },
        };

        return Ok(Some(command));
    }
}
