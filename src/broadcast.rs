use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};

use crate::rumor::*;

#[derive(PartialEq, Eq)]
pub struct Broadcast {
    pub id: usize,
    pub peer_id: usize,
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
    broadcasting: HashMap<usize, (usize, Rumor)>,
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
            // FIXME: serialize rumor
            message: Vec::new(),
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
    fn test_dont_enqueue_duplicates() {
        todo!()
    }

    #[test]
    fn test_broadcast_old_news() {
        todo!()
    }

    #[test]
    fn test_broadcast_ordering() {
        // Fewest sends, then largest size, then newest message
        todo!()
    }
}
