use crate::{client::*, command::*};
use bytes::{BufMut, BytesMut};
use std::io::Write;
use tokio_util::codec::Encoder;

#[repr(u16)]
enum TxID {
    // Common
    Ping = 0x1010,
    XUID = 0x1020,

    // Connection
    Accept = 0x3013,
    Reject = 0x3014,
    Retry = 0x3015,

    // War Management
    TurnDays = 0x4030,
    CampaignPeriod = 0x2030,
    WarTide = 0x2031,
    WarInfluence = 0x2032,
    RoundPoints = 0x2028,

    // Mission Management
    Analyze = 0x2090,
    AddUserPoint = 0x6026,
    AddMissionPoint = 0x6027,

    // Message of the Day
    FreeMessage = 0x201A,
    FixedMessage = 0x201B,

    // Pilot Management
    GetUserInfo = 0x2021,
    NewUserInfo = 0x6022,
    SetUserInfo = 0x6024,
    DelUserInfo = 0x2023,
    SetUserFaction = 0x6025,

    // Garage Management
    VtOwned = 0x2040,
    VtAdd = 0x2042,
    VtDel = 0x2044,
    VtCapture = 0x2046,
    VtSortie = 0x2047,
    VtReturn = 0x4047,

    // Shop
    ShopList = 0x2041,
    VtPotentialCost = 0x2049,

    // Message Board
    MessageCount = 0x2050,
    MessagePost = 0x2052,

    // Leaderboard
    RankValues = 0x2060,
    RankCountGlobal = 0x2061,
    RankCountFaction = 0x2062,
    RankCountCategories = 0x2063,

    // Awards
    AwardAvailable = 0x8041,
    AwardPoints = 0x204C,
    AwardRanks = 0x4062,
    AwardClaim = 0x2048,

    // Trading
    TradeToken = 0x204A,
    TradeUserInfo = 0x204B,
    TradeVT = 0x2045,
}

pub enum TxCommand {
    // Common
    Ping,
    XUID,

    // Connection
    Accept,
    Reject,
    Retry,

    // War Management
    TurnDays {
        days: u16,
    },
    CampaignPeriod {
        round: u16,
        turn: u16,
        seconds: i32,
    },
    WarTide {
        status: u8,
        hsd: u8,
        prf: u8,
        rb: u8,
    },
    WarInfluence {
        status: u8,
        hsd: u8,
        prf: u8,
        rb: u8,
    },
    RoundPoints {
        status: u8,
        points: u32,
        hsd: u8,
        prf: u8,
        rb: u8,
    },

    // Mission Management
    Analyze,
    AddUserPoint {
        status: u8,
    },
    AddMissionPoint {
        status: u8,
    },

    // Message of the Day
    FreeMessage {
        status: u8,
        message: String,
    },
    FixedMessage {
        status: u8,
        messages: Vec<u8>,
    },

    // Pilot Management
    GetUserInfo {
        status: u8, // 0 = Registered, 1 = Unregistered
        pilot_name: String,
        faction: Faction,
        pilot_data: PilotData,
    },
    NewUserInfo {
        status: u8,
    },
    SetUserInfo {
        status: u8,
    },
    DelUserInfo {
        status: u8,
    },
    SetUserFaction {
        status: u8,
    },

    // Garage Management
    VtOwned {
        status: u8,
        vt_list: VTEntryList,
    },
    VtAdd {
        status: u8,
        vt_list: VTEntryList,
    },
    VtDel {
        status: u8,
        vt_list: VTEntryList,
    },
    VtCapture {
        status: u8,
        pilot_name: String,
        vt_list: VTEntryList,
    },
    VtSortie {
        status: u8,
    },
    VtReturn {
        status: u8,
        vt_list: VTEntryList,
    },

    // Shop
    ShopList {
        status: u8,
        shop_list: ShopEntryList,
    },
    VtPotentialCost,

    // Message Board
    MessageCount {
        status: u8,
        start: u32,
        count: u16,
    },
    MessagePost {
        status: u8,
        id: u32,
    },

    // Leaderboard
    RankValues {
        status: u8,
        rank_list: RankEntryList,
    },
    RankCountGlobal {
        status: u8,
        place: u32,
        count: u32,
    },
    RankCountFaction {
        status: u8,
        place: u32,
        count: u32,
    },
    RankCountCategories {
        status: u8,
        categories: [u32; 8],
    },

    // Awards
    AwardAvailable {
        status: u8,
        award_list: AwardEntryList,
    },
    AwardPoints {
        status: u8,
        points: Vec<u32>,
    },
    AwardRanks {
        status: u8,
        award_list: AwardRankEntryList,
    },
    AwardClaim {
        status: u8,
        vt_list: VTEntryList,
    },

    // Trading
    TradeToken {
        status: u8,
        token: TradeToken,
    },
    TradeUserInfo {
        status: u8,
        delay: u16,
    },
    TradeVT {
        status: u8,
        vt_list: VTEntryList,
    },
}

impl fmt::Display for TxCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Common
            TxCommand::Ping => f.write_str("Ping"),
            TxCommand::XUID => f.write_str("XUID"),

            // Connection
            TxCommand::Accept => f.write_str("Accept"),
            TxCommand::Reject => f.write_str("Reject"),
            TxCommand::Retry => f.write_str("Retry"),

            // War Management
            TxCommand::TurnDays { days } => write!(f, "TurnDays - Days {}", days),
            TxCommand::CampaignPeriod {
                round,
                turn,
                seconds,
            } => write!(
                f,
                "CampaignPeriod - Round {}, Turn {}, Seconds {}",
                round, turn, seconds
            ),
            TxCommand::WarTide {
                status,
                hsd,
                prf,
                rb,
            } => write!(
                f,
                "WarTide ({}) - HSD {}, PRF {}, RB {}, Total {}",
                status,
                hsd,
                prf,
                rb,
                *hsd as u16 + *prf as u16 + *rb as u16
            ),
            TxCommand::WarInfluence {
                status,
                hsd,
                prf,
                rb,
            } => write!(
                f,
                "WarInfluence ({}) - HSD {}, PRF {}, RB {}, Total {}",
                status,
                hsd,
                prf,
                rb,
                *hsd as u16 + *prf as u16 + *rb as u16
            ),
            TxCommand::RoundPoints {
                status,
                points,
                hsd,
                prf,
                rb,
            } => write!(
                f,
                "RoundPoints ({}) - Points {}, HSD {}, PRF {}, RB {}",
                status, points, hsd, prf, rb
            ),

            // Mission Management
            TxCommand::Analyze => f.write_str("Analyze"),
            TxCommand::AddUserPoint { status } => write!(f, "AddUserPoint ({})", status),
            TxCommand::AddMissionPoint { status } => write!(f, "AddMissionPoint ({})", status),

            // Message of the Day
            TxCommand::FreeMessage { status, message } => {
                write!(f, "FreeMessage ({}) - MessageLen {}", status, message.len())
            }
            TxCommand::FixedMessage { status, messages } => write!(
                f,
                "FixedMessage ({}) - MessageCount {}",
                status,
                messages.len()
            ),

            // Pilot Management
            TxCommand::GetUserInfo {
                status,
                pilot_name,
                faction,
                ..
            } => write!(
                f,
                "GetUserInfo ({}) - PilotName \"{}\", Faction {}, <PilotData>",
                status, pilot_name, faction
            ),
            TxCommand::NewUserInfo { status } => write!(f, "NewUserInfo ({})", status),
            TxCommand::SetUserInfo { status } => write!(f, "SetUserInfo ({})", status),
            TxCommand::DelUserInfo { status } => write!(f, "DelUserInfo ({})", status),
            TxCommand::SetUserFaction { status } => write!(f, "SetUserFaction ({})", status),

            // Garage Management
            TxCommand::VtOwned { status, vt_list } => {
                write!(f, "VtOwned ({}) - VtListLen {}", status, vt_list.len())
            }
            TxCommand::VtAdd { status, vt_list } => {
                write!(f, "VtAdd ({}) - VtListLen {}", status, vt_list.len())
            }
            TxCommand::VtDel { status, vt_list } => {
                write!(f, "VtDel ({}) - VtListLen {}", status, vt_list.len())
            }
            TxCommand::VtCapture {
                status,
                pilot_name,
                vt_list,
            } => write!(
                f,
                "VtCapture ({}) - PilotName \"{}\", VtListLen {}",
                status,
                pilot_name,
                vt_list.len()
            ),
            TxCommand::VtSortie { status } => write!(f, "VtSortie ({})", status),
            TxCommand::VtReturn { status, vt_list } => {
                write!(f, "VtReturn ({}) - VtListLen {}", status, vt_list.len())
            }

            // Shop
            TxCommand::ShopList { status, shop_list } => {
                write!(f, "ShopList ({}) - ShopListLen {}", status, shop_list.len())
            }
            TxCommand::VtPotentialCost => f.write_str("VtPotentialCost"),

            // Message Board
            TxCommand::MessageCount {
                status,
                start,
                count,
            } => write!(
                f,
                "MessageCount ({}) - Start {}, Count {}",
                status, start, count
            ),
            TxCommand::MessagePost { status, id } => {
                write!(f, "MessagePost ({}) - ID {}", status, id)
            }

            // Leaderboard
            TxCommand::RankValues { status, rank_list } => write!(
                f,
                "RankValues ({}) - RankListLen {}",
                status,
                rank_list.len()
            ),
            TxCommand::RankCountGlobal {
                status,
                place,
                count,
            } => write!(
                f,
                "RankCountGlobal ({}) - Place {}, Count {}",
                status, place, count
            ),
            TxCommand::RankCountFaction {
                status,
                place,
                count,
            } => write!(
                f,
                "RankCountFaction ({}) - Place {}, Count {}",
                status, place, count
            ),
            TxCommand::RankCountCategories { status, categories } => write!(
                f,
                "RankCountCategories ({}) - Categories {} (x10 should all be the same)",
                status, categories[0]
            ),

            // Awards
            TxCommand::AwardAvailable { status, award_list } => write!(
                f,
                "AwardAvailable ({}) - AwardListLen {}",
                status,
                award_list.len()
            ),
            TxCommand::AwardPoints { status, points } => {
                write!(f, "AwardPoints ({}) - PointsLen {}", status, points.len())
            }
            TxCommand::AwardRanks { status, award_list } => write!(
                f,
                "AwardRanks ({}) - AwardListLen {}",
                status,
                award_list.len()
            ),
            TxCommand::AwardClaim { status, vt_list } => {
                write!(f, "AwardRanks ({}) - VtListLen {}", status, vt_list.len())
            }

            // Trading
            TxCommand::TradeToken { status, token } => {
                write!(f, "TradeToken ({}) - Token {:08X}", status, token)
            }
            TxCommand::TradeUserInfo { status, delay } => {
                write!(f, "TradeUserInfo ({}) - Delay {}", status, delay)
            }
            TxCommand::TradeVT { status, vt_list } => {
                write!(f, "TradeVT ({}) - VtListLen {}", status, vt_list.len())
            }
        }
    }
}

fn encode_name<W: Write>(data: &mut W, name: &str) -> Result<(), CampaignCodecError> {
    if name.len() > NAME_CSTR_LEN || !name.is_ascii() {
        return Err(CampaignCodecError::InvalidName(name.len(), name.is_ascii()));
    }
    let mut name_bytes: Vec<u8> = name.as_bytes().to_owned();
    name_bytes.resize(NAME_CSTR_LEN, 0);
    data.write_all(&name_bytes)?;
    return Ok(());
}

fn encode_vt_entry_list<W: BufMut + Write>(
    data: &mut W,
    vt_list: &VTEntryList,
) -> Result<(), CampaignCodecError> {
    data.put_u16_le(
        u16::try_from(vt_list.len())
            .map_err(|_| CampaignCodecError::InvalidListSize(vt_list.len()))?,
    );

    for entry in vt_list {
        data.put_u16_le(entry.id);
        data.put_u32_le(entry.serial);
    }

    return Ok(());
}

fn encode_rank_entry_list<W: BufMut + Write>(
    data: &mut W,
    rank_list: &RankEntryList,
) -> Result<(), CampaignCodecError> {
    data.put_u16_le(
        u16::try_from(rank_list.len())
            .map_err(|_| CampaignCodecError::InvalidListSize(rank_list.len()))?,
    );

    for entry in rank_list {
        data.put_u32_le(entry.rank);
        encode_name(data, &entry.pilot_name)?;
        encode_name(data, &entry.xlive_name)?;
        data.put_u32_le(0); // Padding
        data.put_u32_le(entry.score);
    }

    return Ok(());
}

impl Encoder<TxCommand> for CampaignCodec {
    type Error = CampaignCodecError;

    fn encode(&mut self, item: TxCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut data: Vec<u8> = Vec::new();
        let command = match item {
            // Common
            TxCommand::Ping => TxID::Ping,
            TxCommand::XUID => TxID::XUID,

            // Connection
            TxCommand::Accept => TxID::Accept,
            TxCommand::Reject => TxID::Reject,
            TxCommand::Retry => TxID::Retry,

            // War Management
            TxCommand::TurnDays { days } => {
                data.put_u16_le(days);
                TxID::TurnDays
            }
            TxCommand::CampaignPeriod {
                round,
                turn,
                seconds,
            } => {
                data.put_u16_le(round);
                data.put_u16_le(turn);
                data.put_i32_le(seconds);
                TxID::CampaignPeriod
            }
            TxCommand::WarTide {
                status,
                hsd,
                prf,
                rb,
            } => {
                if hsd as i32 + prf as i32 + rb as i32 != 100 {
                    return Err(CampaignCodecError::InvalidWarTide(hsd, prf, rb));
                }

                data.put_u8(status);
                data.write_all(&[0u8; 5])?; // Padding
                data.put_i32_le(hsd as i32);
                data.put_i32_le(prf as i32);
                data.put_i32_le(rb as i32);
                TxID::WarTide
            }
            TxCommand::WarInfluence {
                status,
                hsd,
                prf,
                rb,
            } => {
                if hsd as i32 + prf as i32 + rb as i32 != 100 {
                    return Err(CampaignCodecError::InvalidWarTide(hsd, prf, rb));
                }

                data.put_u8(status);
                data.write_all(&[0u8; 4])?; // Padding
                data.put_i32_le(hsd as i32);
                data.put_i32_le(prf as i32);
                data.put_i32_le(rb as i32);
                TxID::WarInfluence
            }
            TxCommand::RoundPoints {
                status,
                points,
                hsd,
                prf,
                rb,
            } => {
                // Ranks must be either 0, 1, or 2 any other value will result in a buffer overflow in the game client
                if hsd >= 3 || prf >= 3 || rb >= 3 {
                    return Err(CampaignCodecError::InvalidRanks(hsd, prf, rb));
                }

                data.put_u8(status);
                data.put_u32_le(points);

                // The game client subtracts 1 from each of these values, so add 1 here
                data.put_u8(hsd + 1);
                data.put_u8(prf + 1);
                data.put_u8(rb + 1);
                TxID::RoundPoints
            }

            // Mission Management
            TxCommand::Analyze => TxID::Analyze,
            TxCommand::AddUserPoint { status } => {
                data.put_u8(status);
                TxID::AddUserPoint
            }
            TxCommand::AddMissionPoint { status } => {
                data.put_u8(status);
                TxID::AddMissionPoint
            }

            // Message of the Day
            TxCommand::FreeMessage { status, message } => {
                data.put_u8(status);
                data.put_u8(0); // Padding

                let message_utf16: Vec<u16> = message.encode_utf16().collect();
                let message_len_bytes = message_utf16.len() * 2;

                data.put_u16_le(
                    u16::try_from(message_len_bytes)
                        .map_err(|_| CampaignCodecError::InvalidMOTD(message_len_bytes))?,
                );
                message_utf16.iter().for_each(|&rune| data.put_u16_le(rune));

                TxID::FreeMessage
            }
            TxCommand::FixedMessage { status, messages } => {
                data.put_u8(status);

                data.put_u16_le(
                    u16::try_from(messages.len())
                        .map_err(|_| CampaignCodecError::InvalidMOTD(messages.len()))?,
                );
                data.write_all(&messages)?;

                TxID::FixedMessage
            }

            // Pilot Management
            TxCommand::GetUserInfo {
                status,
                pilot_name,
                faction,
                pilot_data,
            } => {
                data.put_u8(status);

                // Match client side behavior
                if status == 0 {
                    encode_name(&mut data, &pilot_name)?;

                    data.put_u8(faction as u8);

                    data.put_u16_le(PILOT_DATA_LENGTH);
                    data.write_all(&pilot_data)?;
                }

                TxID::GetUserInfo
            }
            TxCommand::NewUserInfo { status } => {
                data.put_u8(status);
                TxID::NewUserInfo
            }
            TxCommand::SetUserInfo { status } => {
                data.put_u8(status);
                TxID::SetUserInfo
            }
            TxCommand::DelUserInfo { status } => {
                data.put_u8(status);
                TxID::DelUserInfo
            }
            TxCommand::SetUserFaction { status } => {
                data.put_u8(status);
                TxID::SetUserFaction
            }

            // Garage Management
            TxCommand::VtOwned { status, vt_list } => {
                data.put_u8(status);
                encode_vt_entry_list(&mut data, &vt_list)?;
                TxID::VtOwned
            }
            TxCommand::VtAdd { status, vt_list } => {
                data.put_u8(status);
                encode_vt_entry_list(&mut data, &vt_list)?;
                TxID::VtAdd
            }
            TxCommand::VtDel { status, vt_list } => {
                data.put_u8(status);
                encode_vt_entry_list(&mut data, &vt_list)?;
                TxID::VtDel
            }
            TxCommand::VtCapture {
                status,
                pilot_name,
                vt_list,
            } => {
                data.put_u8(status);
                encode_name(&mut data, &pilot_name)?;
                encode_vt_entry_list(&mut data, &vt_list)?;
                TxID::VtCapture
            }
            TxCommand::VtSortie { status } => {
                data.put_u8(status);
                TxID::VtSortie
            }
            TxCommand::VtReturn { status, vt_list } => {
                data.put_u8(status);
                encode_vt_entry_list(&mut data, &vt_list)?;
                TxID::VtReturn
            }

            // Shop
            TxCommand::ShopList { status, shop_list } => {
                data.put_u8(status);

                data.put_u16_le(
                    u16::try_from(shop_list.len())
                        .map_err(|_| CampaignCodecError::InvalidListSize(shop_list.len()))?,
                );

                for entry in shop_list {
                    data.put_u16_le(entry.id);
                    data.put_u32_le(entry.limit);
                    data.put_u16_le(entry.cost);
                    data.put_u8(0); // Padding
                }
                TxID::ShopList
            }
            TxCommand::VtPotentialCost => {
                data.put_u8(0); // Status
                data.put_u16_le(0); // No data to send
                /*
                // Placeholder data
                data.put_u16_le(31);
                for i in 0..31u16 {
                    if i == 4 {
                        continue;
                    }
                    data.put_u16_le(i);
                    data.put_u16_le(1);
                }
                */
                TxID::VtPotentialCost
            }

            // Message Board
            TxCommand::MessageCount {
                status,
                start,
                count,
            } => {
                data.put_u8(status);
                data.put_u16_le(0); // Padding
                data.put_u32_le(start);
                data.put_u16_le(count);
                TxID::MessageCount
            }
            TxCommand::MessagePost { status, id } => {
                data.put_u8(status);
                data.put_u16_le(0); // Padding
                data.put_u32_le(id);
                TxID::MessagePost
            }

            // Leaderboard
            TxCommand::RankValues { status, rank_list } => {
                data.put_u8(status);
                let padding = [0u8; 10];
                data.write_all(&padding)?;
                encode_rank_entry_list(&mut data, &rank_list)?;
                TxID::RankValues
            }
            TxCommand::RankCountGlobal {
                status,
                place,
                count,
            } => {
                data.put_u8(status);

                let padding = [0u8; 5];
                data.write_all(&padding)?;

                data.put_u32_le(place);
                data.put_u32_le(count);

                data.put_u32_le(0);
                TxID::RankCountGlobal
            }
            TxCommand::RankCountFaction {
                status,
                place,
                count,
            } => {
                data.put_u8(status);

                let padding = [0u8; 5];
                data.write_all(&padding)?;

                data.put_u32_le(place);
                data.put_u32_le(count);

                data.put_u32_le(0);
                TxID::RankCountFaction
            }
            TxCommand::RankCountCategories { status, categories } => {
                data.put_u8(status);
                data.put_u32_le(0); // Padding

                for c in categories {
                    data.put_u32_le(c);
                }
                TxID::RankCountCategories
            }

            // Awards
            TxCommand::AwardAvailable { status, award_list } => {
                data.put_u8(status);

                data.put_u16_le(
                    u16::try_from(award_list.len())
                        .map_err(|_| CampaignCodecError::InvalidListSize(award_list.len()))?,
                );
                for entry in award_list {
                    data.put_u16_le(entry.round);
                    data.put_u16_le(entry.turn);
                    data.put_u32_le(0); // Padding
                    data.put_u16_le(entry.id);
                    data.put_u32_le(0); // Padding
                    data.put_u8(entry.faction as u8);
                }

                TxID::AwardAvailable
            }
            TxCommand::AwardPoints { status, points } => {
                data.put_u8(status);
                data.put_u8(0); // Padding

                data.put_u16_le(
                    u16::try_from(points.len())
                        .map_err(|_| CampaignCodecError::InvalidListSize(points.len()))?,
                );
                points.iter().for_each(|&p| data.put_u32_le(p));

                TxID::AwardPoints
            }
            TxCommand::AwardRanks { status, award_list } => {
                data.put_u8(status);
                data.put_u32_le(0); // Padding

                data.put_u16_le(
                    u16::try_from(award_list.len())
                        .map_err(|_| CampaignCodecError::InvalidListSize(award_list.len()))?,
                );
                for entry in award_list {
                    data.put_u8(entry.category as u8);
                    data.put_u32_le(entry.rank);
                    data.put_u32_le(0); // Padding
                    data.put_u32_le(0); // Padding
                }

                TxID::AwardRanks
            }
            TxCommand::AwardClaim { status, vt_list } => {
                data.put_u8(status);
                data.put_u32_le(0); // Padding
                encode_vt_entry_list(&mut data, &vt_list)?;
                TxID::AwardClaim
            }

            // Trading
            TxCommand::TradeToken { status, token } => {
                data.put_u8(status);
                data.put_u32_le(token);
                TxID::TradeToken
            }
            TxCommand::TradeUserInfo { status, delay } => {
                data.put_u8(status);
                data.put_u16(delay);
                TxID::TradeUserInfo
            }
            TxCommand::TradeVT { status, vt_list } => {
                data.put_u8(status);
                encode_vt_entry_list(&mut data, &vt_list)?;
                TxID::TradeVT
            }
        };

        // Validate length before allocating more space
        let length = u16::try_from(data.len()).map_err(|_| CampaignCodecError::InvalidHeader)?;

        dst.reserve(4 + data.len());

        dst.put_u16_le(length);
        dst.put_u16_le(command as u16);

        dst.extend_from_slice(&data);

        Ok(())
    }
}
