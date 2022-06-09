use crate::{Incarnation, PeerId};
use std::cmp::Ordering;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum DeserializationError {
    #[error("at least {0} more bytes necessary")]
    TooSmall(usize),
    #[error("at least {0} more bytes necessary for Alive(ipv6 sockaddr)")]
    V6TooSmall(usize),
    #[error("unknown rumor tag {0}")]
    InvalidRumor(u8),
    #[error("unknown ip version {0}")]
    InvalidIp(u8),
}

/// Node states
#[derive(PartialEq, Debug, Clone, Copy, Eq)]
pub enum RumorKind {
    /// Alive messages also deliver details for new peers
    Alive(SocketAddr),
    Suspect,
    Failed,
    // How to handle custom user commands?
    // User(u8, [u8; 512]),
}

impl RumorKind {
    pub fn serialize_to(&self, buf: &mut Vec<u8>) {
        match self {
            RumorKind::Suspect => {
                buf.extend_from_slice(&1u8.to_le_bytes());
            }
            RumorKind::Failed => {
                buf.extend_from_slice(&2u8.to_le_bytes());
            }
            RumorKind::Alive(SocketAddr::V4(sa4)) => {
                buf.extend_from_slice(&4u8.to_le_bytes());
                buf.extend_from_slice(&sa4.ip().octets());
                buf.extend_from_slice(&sa4.port().to_le_bytes());
            }
            RumorKind::Alive(SocketAddr::V6(sa6)) => {
                buf.extend_from_slice(&6u8.to_le_bytes());
                buf.extend_from_slice(&sa6.ip().octets());
                buf.extend_from_slice(&sa6.port().to_le_bytes());
                buf.extend_from_slice(&sa6.flowinfo().to_le_bytes());
                buf.extend_from_slice(&sa6.scope_id().to_le_bytes());
            }
        }
    }

    /// # Safety
    /// It's expected that you've already ensured the slice isn't empty.
    pub fn deserialize(bytes: &[u8]) -> Result<(RumorKind, &[u8]), DeserializationError> {
        // FIXME: return `rest` here
        match bytes[0] {
            1 => Ok((RumorKind::Suspect, &bytes[1..])),
            2 => Ok((RumorKind::Failed, &bytes[1..])),
            4 => {
                // Alive v4
                if bytes.len() < 7 {
                    // tag + v4 + u16 sockaddr
                    return Err(DeserializationError::TooSmall(8 - bytes.len()));
                }
                let mut octets: [u8; 4] = Default::default();
                let (addr_bytes, rest) = bytes[1..].split_at(4);
                octets.clone_from_slice(addr_bytes);
                let ip = Ipv4Addr::from(octets);
                let (port_bytes, rest) = rest.split_at(2);
                let port = u16::from_le_bytes(port_bytes.try_into().unwrap());
                Ok((
                    RumorKind::Alive(SocketAddr::V4(SocketAddrV4::new(ip, port))),
                    rest,
                ))
            }
            6 => {
                // Alive v6
                if bytes.len() < 27 {
                    return Err(DeserializationError::V6TooSmall(27 - bytes.len()));
                }

                let mut octets: [u8; 16] = Default::default();
                let (addr_bytesab, rest) = bytes[1..].split_at(16);
                octets.clone_from_slice(addr_bytesab);
                let ip = Ipv6Addr::from(octets);

                let (pb, rest) = rest.split_at(2);
                let port = u16::from_le_bytes(pb.try_into().unwrap());

                let (fb, rest) = rest.split_at(4);
                let fi = u32::from_le_bytes(fb.try_into().unwrap());

                let (sb, rest) = rest.split_at(4);
                let si = u32::from_le_bytes(sb.try_into().unwrap());
                Ok((
                    RumorKind::Alive(SocketAddr::V6(SocketAddrV6::new(ip, port, fi, si))),
                    rest,
                ))
            }
            tag => Err(DeserializationError::InvalidRumor(tag)),
        }
    }

    pub fn tag(&self) -> u8 {
        match self {
            RumorKind::Suspect => 1,
            RumorKind::Failed => 2,
            RumorKind::Alive(SocketAddr::V4(_)) => 4,
            RumorKind::Alive(SocketAddr::V6(_)) => 6,
        }
    }
}

impl PartialOrd for RumorKind {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use RumorKind::*;
        if self == other {
            return Some(Ordering::Equal);
        }
        match (self, other) {
            (Failed, _) => Some(Ordering::Greater),
            (_, Failed) => Some(Ordering::Less),
            _ => None,
        }
    }
}

/// Rumors disseminated on top of normal gossip
#[derive(PartialEq, Debug, Copy, Clone, Eq)]
pub struct Rumor {
    /// ID of the peer this rumor is about
    pub peer_id: PeerId,
    pub incarnation: Incarnation,
    pub kind: RumorKind,
}

pub const SMALLEST_RUMOR: usize = mem::size_of::<PeerId>() + mem::size_of::<Incarnation>() + 1;

impl Rumor {
    /// Deserialize a rumor from a buffer, returning the Rumor itself and the unprocessed
    /// slice of the buffer.
    pub fn deserialize(bytes: &[u8]) -> Result<(Rumor, &[u8]), DeserializationError> {
        if bytes.len() < SMALLEST_RUMOR {
            return Err(DeserializationError::TooSmall(SMALLEST_RUMOR));
        }
        let (pb, rest) = bytes[0..].split_at(mem::size_of::<PeerId>());
        let peer_id = PeerId::deserialize(pb.try_into().unwrap());

        let (ib, rest) = rest.split_at(mem::size_of::<Incarnation>());
        let incarnation = Incarnation::deserialize(ib.try_into().unwrap());

        let (kind, rest) = RumorKind::deserialize(rest)?;
        Ok((
            Rumor {
                peer_id,
                incarnation,
                kind,
            },
            rest,
        ))
    }

    /// rumors are serialized as:
    /// peer_id, incarnation, rumor_kind_tag, rumor_kind_value
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(SMALLEST_RUMOR);
        self.peer_id.serialize_to(&mut buf);
        self.incarnation.serialize_to(&mut buf);
        self.kind.serialize_to(&mut buf);

        buf
    }
}

impl PartialOrd for Rumor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.peer_id != other.peer_id {
            return None;
        }
        match self.incarnation.cmp(&other.incarnation) {
            Ordering::Equal => self.kind.partial_cmp(&other.kind),
            ord => Some(ord),
        }
    }
}

#[cfg(test)]
mod rumor_tests {
    use super::*;
    use pretty_hex::*;
    type TestResult<T = (), E = Box<dyn std::error::Error>> = Result<T, E>;

    fn sockaddr() -> SocketAddr {
        "127.0.0.1:8080".parse().unwrap()
    }

    #[test]
    fn test_only_cmp_same_peer() {
        let alive = Rumor {
            peer_id: 1.into(),
            kind: RumorKind::Alive(sockaddr()),
            incarnation: 1.into(),
        };
        let alive2 = Rumor {
            peer_id: 2.into(),
            kind: RumorKind::Alive(sockaddr()),
            incarnation: 33.into(),
        };
        assert_eq!(alive.partial_cmp(&alive2), None);
    }

    #[test]
    fn test_rumor_precedence_favors_incarnation_num() {
        let alive1 = Rumor {
            peer_id: 1.into(),
            kind: RumorKind::Alive(sockaddr()),
            incarnation: 1.into(),
        };
        let sus2 = Rumor {
            peer_id: 1.into(),
            kind: RumorKind::Suspect,
            incarnation: 2.into(),
        };
        assert_eq!(alive1.partial_cmp(&sus2), Some(Ordering::Less));
        let alive3 = Rumor {
            peer_id: 1.into(),
            kind: RumorKind::Alive(sockaddr()),
            incarnation: 3.into(),
        };
        assert_eq!(alive3.partial_cmp(&sus2), Some(Ordering::Greater));
        let failed2 = Rumor {
            peer_id: 1.into(),
            kind: RumorKind::Failed,
            incarnation: 2.into(),
        };
        assert_eq!(failed2.partial_cmp(&alive3), Some(Ordering::Less));
        assert_eq!(failed2.partial_cmp(&sus2), Some(Ordering::Greater));
    }

    #[test]
    fn test_serialize_deserialize() -> TestResult {
        let rumors = [
            Rumor {
                peer_id: 0.into(),
                kind: RumorKind::Alive(sockaddr()),
                incarnation: 1.into(),
            },
            Rumor {
                peer_id: 1.into(),
                kind: RumorKind::Alive(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1),
                    8080,
                    13,
                    89,
                ))),
                incarnation: 1.into(),
            },
            Rumor {
                peer_id: 99.into(),
                kind: RumorKind::Suspect,
                incarnation: 12.into(),
            },
            Rumor {
                peer_id: 2.into(),
                kind: RumorKind::Suspect,
                incarnation: 3.into(),
            },
        ];
        for rumor in rumors {
            let (r, _) = Rumor::deserialize(&rumor.serialize())?;
            assert_eq!(rumor, r);
        }
        Ok(())
    }

    #[test]
    fn deserialize() -> TestResult {
        let mut buf = [0u8; 15];
        // [0, 4) are 0 for peer_id 0
        // [4, 8) are incarnation 1
        buf[4] = 1;
        // u8 rumorkind tag. 4 for Alive IPv4
        buf[8] = 4;
        // 4 bytes for the octets
        buf[9] = 127;
        buf[10] = 0;
        buf[11] = 0;
        buf[12] = 1;
        // 2 bytes for the port
        buf[13..15].copy_from_slice(&(8080u16).to_le_bytes());
        match Rumor::deserialize(&buf) {
            Ok((deser, _)) => {
                assert_eq!(
                    Rumor {
                        peer_id: 0.into(),
                        incarnation: 1.into(),
                        kind: RumorKind::Alive(sockaddr()),
                    },
                    deser,
                    "Incorrectly parsed\n{:?}",
                    pretty_hex(&buf)
                );
                Ok(())
            }
            Err(e) => {
                eprintln!("Failed to parse rumor from\n{:?}", pretty_hex(&buf));
                Err(e.into())
            }
        }
    }

    #[test]
    fn deserialize_many() -> TestResult {
        let mut buf = [0u8; 26];
        // two rumors
        buf[0] = 2;
        // peer 0
        buf[2] = 0;
        buf[6] = 1;
        buf[10] = 4;
        buf[11] = 127;
        buf[12] = 0;
        buf[13] = 0;
        buf[14] = 1;
        // 2 bytes for the port
        buf[15..17].copy_from_slice(&(8080u16).to_le_bytes());
        // second rumor
        buf[17] = 1;
        buf[21] = 3;
        buf[25] = 1; // tag 1 is suspect

        let rest = Rumor::deserialize(&buf[2..])
            .map(|(deser, rest)| {
                assert_eq!(
                    Rumor {
                        peer_id: 0.into(),
                        incarnation: 1.into(),
                        kind: RumorKind::Alive(sockaddr()),
                    },
                    deser,
                    "first rumor is incorrect"
                );
                rest
            })
            .map_err(|e| {
                eprintln!("Failed to parse rumor from\n{:?}", pretty_hex(&buf));
                e
            })?;

        match Rumor::deserialize(&rest) {
            Ok((deser, _)) => {
                assert_eq!(
                    Rumor {
                        peer_id: 1.into(),
                        incarnation: 3.into(),
                        kind: RumorKind::Suspect,
                    },
                    deser,
                    "second rumor is incorrect"
                );
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}
