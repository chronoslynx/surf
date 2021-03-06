use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use crate::rumor::*;
use crate::PeerId;

#[derive(PartialEq, Eq, Debug)]
pub struct Broadcast {
    pub id: usize,
    pub peer_id: PeerId,
    pub sends: usize,
    pub message: Vec<u8>,
}

impl PartialOrd for Broadcast {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.sends.cmp(&other.sends) {
            Ordering::Equal => {}
            ord => return Some(ord),
        }
        match self.message.len().cmp(&other.message.len()) {
            Ordering::Equal => {}
            ord => return Some(ord),
        }
        Some(Reverse(self.id).cmp(&Reverse(other.id)))
    }
}

impl Ord for Broadcast {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // We reverse this here because we want a min heap
        other.partial_cmp(self).unwrap()
    }
}

pub struct BroadcastStore {
    queue: BinaryHeap<Broadcast>,
    // Current messages we're broadcasting. Used to dedupe
    // on replay
    // Rumors are small so I don't care that we're storing them twice
    broadcasting: HashMap<PeerId, (usize, Rumor)>,
    next_broadcast: usize,
}

impl BroadcastStore {
    pub fn new() -> Self {
        BroadcastStore {
            queue: BinaryHeap::new(),
            broadcasting: HashMap::new(),
            next_broadcast: 0,
        }
    }

    pub fn replay(&mut self, mut broadcast: Broadcast) {
        broadcast.sends += 1;
        self.queue.push(broadcast)
    }

    pub fn push(&mut self, rumor: Rumor) {
        if let Some((rumor_id, cur_rumor)) = self.broadcasting.get_mut(&rumor.peer_id) {
            assert_eq!(cur_rumor.peer_id, rumor.peer_id);
            if let Some(Ordering::Greater) = rumor.partial_cmp(cur_rumor) {
                *rumor_id = self.next_broadcast;
                *cur_rumor = rumor;
            } else {
                // Old news
                return;
            }
        } else {
            self.broadcasting
                .insert(rumor.peer_id, (self.next_broadcast, rumor));
        }
        self.queue.push(Broadcast {
            peer_id: rumor.peer_id,
            message: rumor.serialize(),
            sends: 0,
            id: self.next_broadcast,
        });
        self.next_broadcast = self.next_broadcast.wrapping_add(1);
    }

    pub fn push_broadcast(&mut self, broadcast: Broadcast) {
        self.queue.push(broadcast);
    }

    pub fn pop(&mut self) -> Option<Broadcast> {
        while let Some(bc) = self.queue.pop() {
            let (latest_id, _) = self.broadcasting.get(&bc.peer_id).unwrap();
            if bc.id >= *latest_id {
                return Some(bc);
            } else {
                return None;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disregards_lesser_news() {
        // We only keep the latest news for a given peer
        let mut bs = BroadcastStore::new();
        bs.push(Rumor {
            peer_id: 1.into(),
            incarnation: 1.into(),
            kind: RumorKind::Suspect,
        });
        let alive = Rumor {
            peer_id: 1.into(),
            incarnation: 2.into(),
            kind: RumorKind::Alive("127.0.0.1:8080".parse().unwrap()),
        };
        bs.push(alive);
        assert_eq!(
            bs.pop(),
            Some(Broadcast {
                peer_id: 1.into(),
                message: alive.serialize(),
                sends: 0,
                id: 1,
            })
        );
        // The suspect rumor is ignored as new news arrived
        assert_eq!(bs.pop(), None);
    }

    #[test]
    fn test_broadcast_ordering() {
        // Fewest sends, then largest size, then newest message
        todo!()
    }
}
