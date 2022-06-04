#[macro_use]
extern crate log;

use core::fmt;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    fmt::Display,
    mem::take,
    net::SocketAddr,
};

/// Node states
#[derive(PartialEq, Debug, Clone, Eq)]
pub enum RumorKind {
    /// Alive messages also deliver details for new peers
    Alive(SocketAddr),
    Suspect,
    Failed,
    Depart,
    // How to handle custom user commands?
    // User(u8, [u8; 512]),
}

/// Rumors disseminated on top of normal gossip
#[derive(PartialEq, Debug, Clone, Eq)]
struct Rumor {
    /// ID of the node this rumor is about
    peer_id: usize,
    kind: RumorKind,
    incarnation: usize,
}

enum AntiEntropy {
    Push(Vec<Peer>),
    Pull(Vec<Peer>),
}

struct BroadcastStore {
    broadcasts: BinaryHeap<Broadcast>,
    next_broadcast: usize,
}

impl BroadcastStore {
    fn new() -> Self {
        BroadcastStore {
            broadcasts: BinaryHeap::new(),
            next_broadcast: 0,
        }
    }

    fn replay_broadcast(&mut self, mut broadcast: Broadcast) {
        broadcast.sends += 1;
        self.broadcasts.push(broadcast)
    }

    fn push(&mut self, rumor: Rumor) {
        self.broadcasts.push(Broadcast {
            msg: rumor,
            serialized: Vec::new(),
            sends: 0,
            id: self.next_broadcast,
        });
        self.next_broadcast = self.next_broadcast.wrapping_add(1);
    }

    fn pop(&mut self) -> Option<Broadcast> {
        self.broadcasts.pop()
    }
}

/// Failure Detector messages. These piggy-back higher level data
#[derive(Debug)]
pub enum MsgKind {
    // Ping(sender_id, sender_ip, sender_port)
    Ping(usize, SocketAddr),
    Ack(usize, usize),
    PingReq {
        target_id: usize,
        target: SocketAddr,
        sender_id: usize,
        sender: SocketAddr,
    },
}

const PIGGYBACKED_MSGS: usize = 10;

#[derive(Debug)]
pub struct Message {
    pub recipient: usize,
    pub kind: MsgKind,
    // TODO: how can I support arbitrary messages?
    gossip: Vec<Rumor>,
}

#[derive(PartialEq, Eq)]
struct Broadcast {
    // TODO we probably don't need this...
    msg: Rumor,
    serialized: Vec<u8>,
    id: usize,
    sends: usize,
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

#[derive(Debug, PartialEq)]
enum PingState {
    Normal,
    Forwarded,
    FromElsewhere,
}

#[derive(Debug)]
struct Ping {
    addr: SocketAddr,
    requester: usize,
    state: PingState,
    sent_at: usize,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PeerState {
    Alive,
    Suspect,
    Failed,
    Departed,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Peer {
    id: usize,
    addr: SocketAddr,
    state: PeerState,
    incarnation: usize,
}

impl Peer {
    fn new(id: usize, addr: SocketAddr, incarnation: usize) -> Peer {
        Peer {
            id,
            addr,
            state: PeerState::Alive,
            incarnation,
        }
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Peer({}, {}, {:?}, {})",
            self.id, self.addr, self.state, self.incarnation
        )
    }
}

pub struct Server {
    pub id: usize,
    addr: SocketAddr,
    tick: usize,
    incarnation: usize,
    pingreq_subgroup_sz: usize,
    ping_interval: usize,
    failure_detection_interval: usize,
    suspicion_period: usize,
    broadcasts: BroadcastStore,
    pings: HashMap<usize, Ping>,
    // Index into memberlist
    last_pinged: usize,
    memberlist: Vec<usize>,
    /// Node id -> (State, timestamp the state was updated)
    membership: HashMap<usize, Peer>,
    outbox: Vec<Message>,
}

impl Display for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node({}, {})", self.id, self.incarnation)
    }
}

impl Server {
    pub fn new(
        id: usize,
        addr: SocketAddr,
        ping_interval: usize,
        pingreq_subgroup_sz: usize,
        gossip_interval: usize,
        suspicion_period: usize,
    ) -> Self {
        Server {
            id,
            addr,
            pingreq_subgroup_sz,
            ping_interval,
            failure_detection_interval: gossip_interval,
            suspicion_period,
            tick: 0,
            incarnation: 1,
            broadcasts: BroadcastStore::new(),
            pings: HashMap::new(),
            last_pinged: 0,
            memberlist: Vec::new(),
            membership: HashMap::new(),
            outbox: Vec::new(),
        }
    }

    fn ack(&mut self, node: usize, recipient: usize) {
        let m = Message {
            recipient,
            kind: MsgKind::Ack(node, self.incarnation),
            gossip: self.gossip(),
        };
        self.outbox.push(m);
    }

    fn ping(&mut self, node: usize, addr: SocketAddr, recipient: usize) {
        let m = Message {
            recipient: node,
            kind: MsgKind::Ping(self.id, self.addr),
            // TODO: if node is `suspect` then spread that gossip!
            gossip: self.gossip(),
        };
        self.outbox.push(m);
        let state = if recipient != self.id {
            PingState::FromElsewhere
        } else {
            PingState::Normal
        };
        debug!(
            "{}@{} pinging {} for {}",
            self.id, self.tick, node, recipient
        );
        self.pings.insert(
            node,
            Ping {
                addr,
                requester: recipient,
                state,
                sent_at: self.tick,
            },
        );
    }

    pub fn live_members(&self) -> Vec<usize> {
        self.membership
            .iter()
            .filter(|(_, peer)| peer.state == PeerState::Alive)
            .map(|(k, _)| *k)
            .collect()
    }

    fn add(&mut self, id: usize, addr: SocketAddr, incarnation: usize) {
        if self.membership.contains_key(&id) {
            debug!("{:03} already knows of {:03}", self.id, id);
            return;
        }
        let peer = Peer::new(id, addr, incarnation);
        info!("{:03}@{} discovered {:03}", self.id, self.tick, peer);
        let mut rng = thread_rng();
        let n: usize = rng.gen_range(0..=self.memberlist.len());
        self.memberlist.insert(n, peer.id);
        self.membership.insert(peer.id, peer);
    }

    /// Add a peer to our network. The peer is not reflected in the member list
    /// until it has responded to a ping.
    pub fn add_peer(&mut self, id: usize, addr: SocketAddr) {
        if self.membership.contains_key(&id) {
            return;
        }
        self.ping(id, addr, self.id);
    }

    fn process_gossip(&mut self, rumor: &Rumor) {
        match &rumor.kind {
            RumorKind::Depart => {}
            RumorKind::Alive(addr) => {
                if rumor.peer_id == self.id {
                    self.incarnation += 1;
                    self.broadcasts.push(rumor.clone());
                } else if let Some(peer) = self.membership.get_mut(&rumor.peer_id) {
                    if rumor.incarnation > peer.incarnation {
                        if peer.state == PeerState::Failed {
                            // rejoin!
                        } else if peer.state != PeerState::Alive {
                            info!("{:03}@{} marking {:03} as Alive", self.id, self.tick, peer);
                        }
                        peer.state = PeerState::Alive;
                        peer.incarnation = rumor.incarnation;
                        self.broadcasts.push(rumor.clone());
                    }
                } else {
                    self.add(rumor.peer_id, *addr, rumor.incarnation);
                    self.broadcasts.push(rumor.clone());
                }
            }
            RumorKind::Suspect => {
                if rumor.peer_id == self.id {
                    // Reports of my death have been greatly exagerrated.
                    self.broadcasts.push(Rumor {
                        peer_id: self.id,
                        incarnation: self.incarnation,
                        kind: RumorKind::Alive(self.addr),
                    });
                } else if let Some(peer) = self.membership.get_mut(&rumor.peer_id) {
                    if rumor.incarnation > peer.incarnation {
                        if peer.state != PeerState::Suspect {
                            info!(
                                "{:03}@{} marking {:03} as Suspect",
                                self.id, self.tick, peer
                            );
                        }
                        peer.state = PeerState::Suspect;
                        peer.incarnation = rumor.incarnation;
                        self.broadcasts.push(rumor.clone());
                    }
                }
            }
            RumorKind::Failed => {
                if let Some(peer) = self.membership.remove(&rumor.peer_id) {
                    warn!("{}@{} marking {} as Failed", self.id, self.tick, peer);
                    let mut idx = usize::MAX;
                    for (i, n) in self.memberlist.iter().enumerate() {
                        if *n == rumor.peer_id {
                            idx = i;
                            break;
                        }
                    }
                    assert!(idx != usize::MAX);
                    self.memberlist.swap_remove(idx);
                    self.broadcasts.push(rumor.clone());
                }
            }
        }
    }

    // FIXME: only provide rumors up to a certain byte boundary
    fn gossip(&mut self) -> Vec<Rumor> {
        let mut msgs = Vec::new();
        let n = (self.membership.len() + 2) as f32;
        let max_sends = 3 * n.log10().ceil() as usize;
        // From the paper
        self.suspicion_period = self.failure_detection_interval * max_sends;
        // FIXME peek and check size first
        while msgs.len() < PIGGYBACKED_MSGS {
            if let Some(update) = self.broadcasts.pop() {
                let dm = update.msg.clone();
                if update.sends < max_sends - 1 {
                    self.broadcasts.replay_broadcast(update);
                }
                msgs.push(dm);
            } else {
                break;
            }
        }
        msgs
    }

    pub fn process(&mut self, msg: Message) {
        self.incarnation += 1;
        assert_eq!(
            msg.recipient, self.id,
            "Simulator bug; sent {:?} to the wrong node",
            msg
        );
        match msg.kind {
            MsgKind::Ping(sender_id, sender_addr) => {
                self.add(sender_id, sender_addr, 0);
                self.ack(self.id, sender_id);
            }
            MsgKind::PingReq {
                target_id,
                target,
                sender_id,
                sender,
            } => {
                assert!(target_id != self.id);
                self.add(sender_id, sender, 0);
                self.ping(target_id, target, sender_id);
            }
            MsgKind::Ack(peer_id, incarnation) => {
                if !self.pings.contains_key(&peer_id) {
                    debug!("{}@{} unexpected ack from {}", self.id, self.tick, peer_id);
                    return;
                }
                let Ping {
                    addr,
                    requester,
                    state: _state,
                    sent_at: _sent_at,
                } = self.pings.remove(&peer_id).unwrap();
                if requester != self.id {
                    self.ack(peer_id, requester);
                }

                if let Some(peer) = self.membership.get_mut(&peer_id) {
                    if peer.state != PeerState::Failed && incarnation > peer.incarnation {
                        peer.state = PeerState::Alive;
                        peer.incarnation = incarnation;
                        self.broadcasts.push(Rumor {
                            peer_id,
                            incarnation,
                            kind: RumorKind::Alive(peer.addr),
                        });
                    }
                } else {
                    self.add(peer_id, addr, incarnation);
                    self.broadcasts.push(Rumor {
                        peer_id,
                        incarnation,
                        kind: RumorKind::Alive(addr),
                    });
                }
            }
        };

        for rumor in msg.gossip.iter() {
            self.process_gossip(rumor);
        }
    }

    pub fn tick(&mut self) -> Vec<Message> {
        self.tick += 1;
        if self.last_pinged >= self.memberlist.len() {
            let mut rng = thread_rng();
            self.memberlist.shuffle(&mut rng);
            self.last_pinged = 0;
        }

        let mut to_rm = Vec::new();
        let mut pings = take(&mut self.pings);
        for (node, ping) in pings.iter_mut() {
            if self.tick > (ping.sent_at + self.suspicion_period) {
                assert!(ping.state == PingState::Forwarded);
                let peer = self.membership.get(node).unwrap();
                self.broadcasts.push(Rumor {
                    peer_id: *node,
                    incarnation: peer.incarnation,
                    kind: RumorKind::Failed,
                });
                to_rm.push(*node);
            } else if self.tick > (ping.sent_at + self.failure_detection_interval) {
                // At this point we throw out pings for non-member peers.
                if ping.state == PingState::FromElsewhere || !self.membership.contains_key(node) {
                    to_rm.push(*node);
                    continue;
                }
                let peer = self.membership.get(node).unwrap();
                debug!("{} suspects that {} has failed", self.id, node);
                self.broadcasts.push(Rumor {
                    peer_id: *node,
                    incarnation: peer.incarnation,
                    kind: RumorKind::Suspect,
                });
            } else if ping.state != PingState::Forwarded
                && self.tick > (ping.sent_at + self.ping_interval)
            {
                if ping.state != PingState::Normal {
                    debug!(
                        "{}@{} expire ping from {} to {}",
                        self.id, self.tick, ping.requester, node
                    );
                    to_rm.push(*node);
                    continue;
                }
                // late, send ping_req to k nodes
                let mut chosen = HashSet::new();
                let mut rng = thread_rng();
                let subgroup_sz = self.pingreq_subgroup_sz.min(self.memberlist.len());
                let incarnation = self
                    .membership
                    .get(node)
                    .map(|p| p.incarnation)
                    .unwrap_or(0);
                if self.memberlist.len() <= 1 {
                    debug!(
                        "{}@{} suspects that {} has failed",
                        self.id, self.tick, node
                    );
                    to_rm.push(*node);
                    self.broadcasts.push(Rumor {
                        peer_id: *node,
                        incarnation,
                        kind: RumorKind::Suspect,
                    });
                    continue;
                }
                while chosen.len() < subgroup_sz {
                    let recipient = *self.memberlist.choose(&mut rng).unwrap();
                    if recipient != *node && !chosen.contains(&recipient) {
                        chosen.insert(recipient);
                        let m = Message {
                            recipient,
                            kind: MsgKind::PingReq {
                                target_id: *node,
                                target: ping.addr,
                                sender_id: self.id,
                                sender: self.addr,
                            },
                            gossip: self.gossip(),
                        };
                        self.outbox.push(m);
                    }
                }
                ping.state = PingState::Forwarded;
            }
        }
        self.pings = pings;
        for node in to_rm {
            debug!("{}@{} expire ping to {}", self.id, self.tick, node);
            self.pings.remove(&node);
        }
        if !self.membership.is_empty() {
            assert_eq!(
                self.memberlist.len(),
                self.membership.len(),
                "membership {:?}\nmemberlist {:?}",
                self.membership,
                self.memberlist
            );
            let ping_rcpt = self.memberlist[self.last_pinged];
            let ping_peer = self.membership.get(&ping_rcpt).unwrap().clone();
            self.ping(ping_rcpt, ping_peer.addr, self.id);
            self.last_pinged += 1;
        }
        take(&mut self.outbox)
    }
}

#[cfg(test)]
mod test {}
