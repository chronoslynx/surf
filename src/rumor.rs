use std::cmp::Ordering;
use std::mem;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

#[derive(Debug, thiserror::Error)]
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
    pub fn serialize(&self, buf: &mut Vec<u8>) {
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
    pub fn from_slice(bytes: &[u8]) -> Result<RumorKind, DeserializationError> {
        match bytes[0] {
            1 => Ok(RumorKind::Suspect),
            2 => Ok(RumorKind::Failed),
            4 => {
                // Alive v4
                if bytes.len() < 7 {
                    // tag + v4 + u16 sockaddr
                    return Err(DeserializationError::TooSmall(8 - bytes.len()));
                }
                let octets: [u8; 4] = Default::default();
                let (addr_bytes, rest) = bytes[1..].split_at(4);
                octets.clone_from_slice(addr_bytes);
                let ip = Ipv4Addr::from(octets);
                let (port_bytes, _) = rest.split_at(2);
                let port = u16::from_le_bytes(port_bytes.try_into().unwrap());
                Ok(RumorKind::Alive(SocketAddr::V4(SocketAddrV4::new(
                    ip, port,
                ))))
            }
            6 => {
                // Alive v6
                if bytes.len() < 27 {
                    return Err(DeserializationError::V6TooSmall(27 - bytes.len()));
                }

                let octets: [u8; 16] = Default::default();
                let (addr_bytesab, rest) = bytes[1..].split_at(16);
                octets.clone_from_slice(addr_bytesab);
                let ip = Ipv6Addr::from(octets);

                let (pb, rest) = rest.split_at(2);
                let port = u16::from_le_bytes(pb.try_into().unwrap());

                let (fb, rest) = rest.split_at(4);
                let fi = u32::from_le_bytes(fb.try_into().unwrap());

                let (sb, rest) = rest.split_at(4);
                let si = u32::from_le_bytes(sb.try_into().unwrap());
                Ok(RumorKind::Alive(SocketAddr::V6(SocketAddrV6::new(
                    ip, port, fi, si,
                ))))
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
    pub peer_id: usize,
    pub incarnation: usize,
    pub kind: RumorKind,
}

const SMALLEST_RUMOR: usize = mem::size_of::<usize>() * 2 + 1;

impl Rumor {
    fn from_bytes(bytes: &[u8]) -> Result<Rumor, DeserializationError> {
        if bytes.len() < SMALLEST_RUMOR {
            return Err(DeserializationError::TooSmall(SMALLEST_RUMOR));
        }
        todo!()
    }

    /// rumors are serialized as:
    /// peer_id, incarnation, rumor_kind_tag, rumor_kind_value
    fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(SMALLEST_RUMOR);
        buf.extend_from_slice(&self.peer_id.to_le_bytes());
        buf.extend_from_slice(&self.incarnation.to_le_bytes());

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

    fn sockaddr() -> SocketAddr {
        "127.0.0.1:8080".parse().unwrap()
    }

    #[test]
    fn test_only_cmp_same_peer() {
        let alive = Rumor {
            peer_id: 1,
            kind: RumorKind::Alive(sockaddr()),
            incarnation: 1,
        };
        let alive2 = Rumor {
            peer_id: 2,
            kind: RumorKind::Alive(sockaddr()),
            incarnation: 33,
        };
        assert_eq!(alive.partial_cmp(&alive2), None);
    }

    #[test]
    fn test_rumor_precedence_favors_incarnation_num() {
        let alive1 = Rumor {
            peer_id: 1,
            kind: RumorKind::Alive(sockaddr()),
            incarnation: 1,
        };
        let sus2 = Rumor {
            peer_id: 1,
            kind: RumorKind::Suspect,
            incarnation: 2,
        };
        assert_eq!(alive1.partial_cmp(&sus2), Some(Ordering::Less));
        let alive3 = Rumor {
            peer_id: 1,
            kind: RumorKind::Alive(sockaddr()),
            incarnation: 3,
        };
        assert_eq!(alive3.partial_cmp(&sus2), Some(Ordering::Greater));
        let failed2 = Rumor {
            peer_id: 1,
            kind: RumorKind::Failed,
            incarnation: 2,
        };
        assert_eq!(failed2.partial_cmp(&alive3), Some(Ordering::Less));
        assert_eq!(failed2.partial_cmp(&sus2), Some(Ordering::Greater));
    }
}
