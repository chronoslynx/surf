use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::rumor::*;

#[derive(PartialEq, Eq)]
pub struct Broadcast {
    // TODO store serialized rumor only
    pub rumor: Rumor,
    pub serialized: Vec<u8>,
    pub id: usize,
    pub sends: usize,
}

impl PartialOrd for Broadcast {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.sends < other.sends {
            Some(Ordering::Less)
        } else if self.serialized.len() > other.serialized.len() {
            Some(Ordering::Less)
        } else {
            Some(self.id.cmp(&other.id))
        }
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
    broadcasting: HashMap<usize, Rumor>,
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
        if let Some(cur_rumor) = self.broadcasting.get_mut(&rumor.peer_id) {
            assert_eq!(cur_rumor.peer_id, rumor.peer_id);
            if let Some(Ordering::Greater) = rumor.partial_cmp(cur_rumor) {
                *cur_rumor = rumor;
            } else {
                // Old news
                return;
            }
        } else {
            self.broadcasting.insert(rumor.peer_id, rumor);
        }
        self.queue.push(Broadcast {
            rumor,
            // TODO: serialize rumor
            serialized: Vec::new(),
            sends: 0,
            id: self.next_broadcast,
        });
        self.next_broadcast = self.next_broadcast.wrapping_add(1);
    }

    pub fn pop(&mut self) -> Option<Broadcast> {
        while let Some(bc) = self.queue.pop() {
            let latest = self.broadcasting.get(&bc.rumor.peer_id).unwrap();
            match bc.rumor.partial_cmp(latest) {
                Some(Ordering::Greater) => panic!(
                    "Bug! broadcasting rumor {:?} newer than latest tracked {:?}",
                    bc.rumor, latest
                ),
                Some(Ordering::Equal) => {
                    return Some(bc);
                }
                Some(Ordering::Less) => {
                    continue;
                }
                None => panic!(
                    "Bug! should have ordering between {:?} and {:?}",
                    bc.rumor, latest
                ),
            }
        }
        None
    }
}
