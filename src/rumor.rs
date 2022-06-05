use std::cmp::Ordering;
use std::net::SocketAddr;

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
    pub kind: RumorKind,
    pub incarnation: usize,
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
