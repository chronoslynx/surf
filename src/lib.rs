#[macro_use]
extern crate log;

mod broadcast;
mod rumor;

pub use broadcast::*;
use rumor::*;

use core::fmt;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    mem::take,
    net::SocketAddr,
    time::{Duration, Instant},
};

const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, PartialEq)]
enum PingState {
    Normal,
    Forwarded,
    FromElsewhere,
}

#[derive(Debug)]
struct PendingPing {
    addr: SocketAddr,
    seq_no: usize,
    requester: usize,
    state: PingState,
    sent_at: Instant,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PeerState {
    Alive,
    Suspect,
    Failed,
}

impl From<RumorKind> for PeerState {
    fn from(rk: RumorKind) -> Self {
        match rk {
            RumorKind::Alive(_) => PeerState::Alive,
            RumorKind::Suspect => PeerState::Suspect,
            RumorKind::Failed => PeerState::Failed,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Peer {
    id: usize,
    addr: SocketAddr,
    state: PeerState,
    incarnation: usize,
}

impl Peer {
    fn new(id: usize, addr: SocketAddr, incarnation: usize, state: PeerState) -> Peer {
        Peer {
            id,
            addr,
            state,
            incarnation,
        }
    }

    fn rumor_kind(&self) -> RumorKind {
        match self.state {
            PeerState::Alive => RumorKind::Alive(self.addr),
            PeerState::Failed => RumorKind::Failed,
            PeerState::Suspect => RumorKind::Suspect,
        }
    }

    /// Create a rumor about this peer's current state
    fn rumor(&self) -> Rumor {
        Rumor {
            peer_id: self.id,
            incarnation: self.incarnation,
            kind: self.rumor_kind(),
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

const PIGGYBACKED_MSGS: usize = 10;

/// Failure Detector messages. These piggy-back higher level data
#[derive(Debug)]
pub enum MsgKind {
    Ping,
    Ack(usize, usize),
    PingReq {
        target_id: usize,
        target: SocketAddr,
    },
    Push(Vec<Peer>),
    Pull(Vec<Peer>),
}

#[derive(Debug)]
pub struct Message {
    pub protocol_version: u16,
    pub recipient: usize,
    pub sender_id: usize,
    pub sender: SocketAddr,
    pub seq_no: usize,
    pub kind: MsgKind,
    // FIXME separate this from the failure detector messages
    // Gossip shuold be pulled by whatever does the networking work...
    // so it can manage packing
    pub gossip: Vec<Rumor>,
}

pub struct Server {
    pub id: usize,
    addr: SocketAddr,
    seq_no: usize,
    incarnation: usize,
    pingreq_subgroup_sz: usize,
    ping_interval: Duration,
    protocol_period: Duration,
    suspicion_period: Duration,
    broadcasts: BroadcastStore,
    pings: HashMap<usize, PendingPing>,
    // Index into memberlist
    last_pinged: usize,
    memberlist: Vec<usize>,
    /// Node id -> (State, timestamp the state was updated)
    membership: HashMap<usize, Peer>,
    // FIXME we need something better than this. Maybe a callback? another delegate, I mean
    outbox: Vec<Message>,
}

impl Display for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Server({}, {})", self.id, self.incarnation)
    }
}

impl Server {
    pub fn new(
        id: usize,
        addr: SocketAddr,
        ping_interval: Duration,
        pingreq_subgroup_sz: usize,
        protocol_period: Duration,
        suspicion_period: Duration,
    ) -> Self {
        Server {
            id,
            addr,
            pingreq_subgroup_sz,
            ping_interval,
            protocol_period,
            suspicion_period,
            seq_no: 1,
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
            protocol_version: PROTOCOL_VERSION,
            recipient,
            sender_id: self.id,
            sender: self.addr,
            seq_no: self.seq_no,
            kind: MsgKind::Ack(node, self.incarnation),
            gossip: self.gossip(),
        };
        self.seq_no = self.seq_no.wrapping_add(1);
        self.outbox.push(m);
    }

    fn ping(&mut self, node: usize, addr: SocketAddr, recipient: usize) {
        assert_ne!(node, self.id, "Attempted to ping ourselves");
        let m = Message {
            protocol_version: PROTOCOL_VERSION,
            recipient: node,
            sender_id: self.id,
            sender: self.addr,
            seq_no: self.seq_no,
            kind: MsgKind::Ping,
            // TODO: if node is `suspect` then spread that gossip!
            gossip: self.gossip(),
        };
        self.seq_no = self.seq_no.wrapping_add(1);
        let state = if recipient != self.id {
            PingState::FromElsewhere
        } else {
            PingState::Normal
        };
        debug!(
            "{:03} pinging {:03} on behalf of {:03}",
            self.id, node, recipient
        );
        self.pings.insert(
            node,
            PendingPing {
                addr,
                seq_no: m.seq_no,
                requester: recipient,
                state,
                sent_at: Instant::now(),
            },
        );
        self.outbox.push(m);
    }

    pub fn live_members(&self) -> Vec<Peer> {
        let peer_self = Peer::new(self.id, self.addr, self.incarnation, PeerState::Alive);
        let mut peers = Vec::with_capacity(1 + self.membership.len());
        peers.push(peer_self);
        for peer in self.membership.values() {
            peers.push(peer.clone());
        }
        peers
    }

    /// Apply new information to the specified peer state machine.
    fn upsert_peer(&mut self, peer_id: usize, incarnation: usize, rumor_kind: RumorKind) {
        assert_ne!(peer_id, self.id, "We should handle ourselves elsewhere");
        if let Some(peer) = self.membership.get_mut(&peer_id) {
            if incarnation < peer.incarnation {
                return;
            }
            peer.incarnation = incarnation;
            let state = rumor_kind.into();
            if peer.state == state {
                self.broadcasts.push(peer.rumor());
                return;
            }
            info!(
                "{:03} update peer {:03}: {:?} -> {:?}",
                self.id, peer.id, peer.state, state
            );
            if peer.state == PeerState::Failed {
                // we actually have to probe them now
                let mut rng = thread_rng();
                let n: usize = rng.gen_range(0..=self.memberlist.len());
                self.memberlist.insert(n, peer.id);
            } else if state == PeerState::Failed {
                // dont bother probing failed peers
                let mut idx = usize::MAX;
                for (i, n) in self.memberlist.iter().enumerate() {
                    if *n == peer_id {
                        idx = i;
                        break;
                    }
                }
                assert!(idx != usize::MAX);
                self.memberlist.swap_remove(idx);
            }
            peer.state = state;
            self.broadcasts.push(peer.rumor());
        } else if let RumorKind::Alive(addr) = rumor_kind {
            let peer = Peer::new(peer_id, addr, incarnation, rumor_kind.into());
            info!("{:03} discovered {:03}", self.id, peer);
            let mut rng = thread_rng();
            let n: usize = rng.gen_range(0..=self.memberlist.len());
            self.memberlist.insert(n, peer.id);
            self.membership.insert(peer.id, peer);
            self.broadcasts.push(peer.rumor());
        }
    }

    /// Join a cluster the specified peer belongs to
    /// FIXME tie messages to addresses!
    pub fn join(&mut self, peer_id: usize, addr: SocketAddr) {
        if self.membership.contains_key(&peer_id) {
            return;
        }

        let m = Message {
            protocol_version: PROTOCOL_VERSION,
            recipient: peer_id,
            sender_id: self.id,
            sender: self.addr,
            seq_no: 0,
            kind: MsgKind::Pull(Vec::new()),
            gossip: Vec::new(),
        };
        self.outbox.push(m);
    }

    fn process_gossip(&mut self, rumor: &Rumor) {
        if rumor.peer_id == self.id {
            if rumor.incarnation < self.incarnation {
                return;
            }
            match &rumor.kind {
                RumorKind::Alive(_) => {
                    self.incarnation += 1;
                }
                RumorKind::Suspect | RumorKind::Failed => {
                    // Reports of my death have been greatly exaggerated.
                    self.incarnation = self.incarnation.wrapping_add(1);
                    self.broadcasts.push(Rumor {
                        peer_id: self.id,
                        incarnation: self.incarnation,
                        kind: RumorKind::Alive(self.addr),
                    });
                }
            }
        } else {
            self.upsert_peer(rumor.peer_id, rumor.incarnation, rumor.kind);
        }
    }

    // FIXME: only provide rumors up to a certain byte boundary
    fn gossip(&mut self) -> Vec<Rumor> {
        let mut msgs = Vec::new();
        let n = (self.membership.len() + 2) as f32;
        let max_sends = 3 * n.log10().ceil() as u32;
        // From the paper
        self.suspicion_period = self.protocol_period * max_sends;
        // FIXME peek and check size first
        while msgs.len() < PIGGYBACKED_MSGS {
            if let Some(update) = self.broadcasts.pop() {
                let dm = update.rumor;
                if update.sends < (max_sends as usize - 1) {
                    self.broadcasts.replay(update);
                }
                msgs.push(dm);
            } else {
                break;
            }
        }
        msgs
    }

    // TODO: return a response
    pub fn process(&mut self, msg: Message) {
        self.incarnation += 1;
        assert_eq!(
            msg.recipient, self.id,
            "Simulator bug; sent {:?} to the wrong node",
            msg
        );
        self.upsert_peer(msg.sender_id, 0, RumorKind::Alive(msg.sender));
        match msg.kind {
            MsgKind::Push(peers) => {
                // Merge with our state
                for peer in peers {
                    if peer.id != self.id {
                        self.upsert_peer(peer.id, peer.incarnation, peer.rumor_kind())
                    }
                }
            }
            MsgKind::Pull(peers) => {
                // Respond with our state in a Push
                let our_peers = self.live_members();
                let m = Message {
                    protocol_version: PROTOCOL_VERSION,
                    recipient: msg.sender_id,
                    sender_id: self.id,
                    sender: self.addr,
                    seq_no: 0,
                    kind: MsgKind::Push(our_peers),
                    gossip: self.gossip(),
                };
                self.outbox.push(m);
                // TODO what if they think we're suspect?
                for peer in peers {
                    if peer.id != self.id {
                        self.upsert_peer(peer.id, peer.incarnation, peer.rumor_kind())
                    }
                }
            }
            MsgKind::Ping => {
                self.ack(self.id, msg.sender_id);
            }
            MsgKind::PingReq { target_id, target } => {
                self.ping(target_id, target, msg.sender_id);
            }
            MsgKind::Ack(peer_id, incarnation) => {
                if !self.pings.contains_key(&peer_id) {
                    debug!("{:03} unexpected ack from {}", self.id, peer_id);
                    return;
                }
                let PendingPing {
                    addr,
                    requester,
                    seq_no,
                    state: _state,
                    sent_at: _sent_at,
                } = self.pings.remove(&peer_id).unwrap();
                if seq_no != msg.seq_no {
                    return;
                } else if requester != self.id {
                    self.ack(peer_id, requester);
                }

                self.upsert_peer(peer_id, incarnation, RumorKind::Alive(addr));
            }
        };

        for rumor in msg.gossip.iter() {
            self.process_gossip(rumor);
        }
    }

    /// Called once per protocol period
    pub fn tick(&mut self) -> Vec<Message> {
        if self.last_pinged >= self.memberlist.len() {
            let mut rng = thread_rng();
            self.memberlist.shuffle(&mut rng);
            self.last_pinged = 0;
        }

        let mut to_rm = Vec::new();
        let mut pings = take(&mut self.pings);
        let now = Instant::now();
        for (node, ping) in pings.iter_mut() {
            if now > (ping.sent_at + self.suspicion_period) {
                assert!(ping.state == PingState::Forwarded);
                let peer = self.membership.get(node).unwrap();
                self.broadcasts.push(Rumor {
                    peer_id: *node,
                    incarnation: peer.incarnation,
                    kind: RumorKind::Failed,
                });
                to_rm.push(*node);
            } else if now > (ping.sent_at + self.protocol_period) {
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
                && now > (ping.sent_at + self.ping_interval)
            {
                if ping.state != PingState::Normal {
                    debug!(
                        "{:03} expire ping from {:03} to {:03}",
                        self.id, ping.requester, node
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
                    debug!("{:03} suspects that {:03} has failed", self.id, node);
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
                            protocol_version: PROTOCOL_VERSION,
                            recipient,
                            sender_id: self.id,
                            sender: self.addr,
                            seq_no: ping.seq_no,
                            kind: MsgKind::PingReq {
                                target_id: *node,
                                target: ping.addr,
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
            debug!("{:03} expire ping to {}", self.id, node);
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
mod tests {
    use super::*;

    #[test]
    fn test_periodically_ping() {
        todo!()
    }

    #[test]
    fn test_periodically_pushpull() {
        todo!()
    }

    #[test]
    fn test_pushpull() {
        todo!()
    }

    #[test]
    fn test_pings_are_acked() {
        todo!()
    }

    #[test]
    fn test_pingreqs_beget_pings() {
        todo!()
    }

    #[test]
    fn test_pingreq_acks_are_forwarded() {
        todo!()
    }

    #[test]
    fn test_late_acks_are_suspect() {
        todo!()
    }

    #[test]
    fn test_timely_acks_clear_suspicion() {
        todo!()
    }

    #[test]
    fn test_recognize_failed_peer() {
        todo!()
    }

    #[test]
    fn test_ignore_old_news() {
        todo!()
    }
}
