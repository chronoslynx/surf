#[macro_use]
extern crate log;

use core::fmt;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
    mem::take,
    net::{self, IpAddr},
};

/// Node states
#[derive(PartialEq, Debug, Clone)]
pub enum RumorKind {
    /// Alive messages also deliver details for new peers
    Alive(IpAddr, u16),
    Suspect,
    Failed,
    Depart,
    // Send our state and request that the other do the same
    Pull(Vec<Peer>),
    Push(Vec<Peer>),
    // How to handle custom user commands?
    // User(u8, [u8; 512]),
}

/// Dissemination Messages
#[derive(PartialEq, Debug, Clone)]
struct Rumor {
    /// ID of the node this rumor is about
    peer_id: usize,
    kind: RumorKind,
    incarnation: usize,
    // payload?
}

/// Failure Detector messages. These piggy-back higher level data
#[derive(Debug)]
pub enum MsgKind {
    // Ping(sender_id, sender_ip, sender_port)
    Ping(usize, IpAddr, u16),
    Ack(usize, usize),
    PingReq {
        target_id: usize,
        target_ip: IpAddr,
        target_port: u16,
        sender_id: usize,
        sender_ip: IpAddr,
        sender_port: u16,
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

struct Update {
    msg: Rumor,
    sends: usize,
}

#[derive(Debug, PartialEq)]
enum PingState {
    Normal,
    Forwarded,
    FromElsewhere,
}

#[derive(Debug)]
struct Ping {
    ip: IpAddr,
    port: u16,
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
    ip: net::IpAddr,
    port: u16,
    state: PeerState,
    incarnation: usize,
}

impl Peer {
    fn new(id: usize, ip: IpAddr, port: u16, incarnation: usize) -> Peer {
        Peer {
            id,
            ip,
            port,
            state: PeerState::Alive,
            incarnation,
        }
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Peer({}, {}:{}, {:?}, {})",
            self.id, self.ip, self.port, self.state, self.incarnation
        )
    }
}

pub struct Server {
    pub id: usize,
    ip: net::IpAddr,
    port: u16,
    tick: usize,
    incarnation: usize,
    pingreq_subgroup_sz: usize,
    ping_interval: usize,
    gossip_interval: usize,
    suspicion_period: usize,
    updates: VecDeque<Update>,
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
        addr: net::IpAddr,
        port: u16,
        ping_interval: usize,
        pingreq_subgroup_sz: usize,
        gossip_interval: usize,
        suspicion_period: usize,
    ) -> Self {
        Server {
            id,
            ip: addr,
            port,
            incarnation: 0,
            tick: 0,
            pingreq_subgroup_sz,
            last_pinged: 0,
            ping_interval,
            gossip_interval,
            suspicion_period,
            memberlist: Vec::new(),
            updates: VecDeque::new(),
            membership: HashMap::new(),
            pings: HashMap::new(),
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

    fn ping(&mut self, node: usize, ip: IpAddr, port: u16, recipient: usize) {
        let m = Message {
            recipient: node,
            kind: MsgKind::Ping(self.id, self.ip, self.port),
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
                ip,
                port,
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

    fn add(&mut self, id: usize, ip: IpAddr, port: u16, incarnation: usize) {
        if self.membership.contains_key(&id) {
            debug!("{:03} already knows of {:03}", self.id, id);
            return;
        }
        let peer = Peer::new(id, ip, port, incarnation);
        info!("{:03}@{} discovered {:03}", self.id, self.tick, peer);
        let mut rng = thread_rng();
        let n: usize = rng.gen_range(0..=self.memberlist.len());
        self.memberlist.insert(n, peer.id);
        self.membership.insert(peer.id, peer);
    }

    /// Add a peer to our network. The peer is not reflected in the member list
    /// until it has responded to a ping.
    pub fn add_peer(&mut self, id: usize, ip: IpAddr, port: u16) {
        if self.membership.contains_key(&id) {
            return;
        }
        self.ping(id, ip, port, self.id);
    }

    fn process_gossip(&mut self, rumor: &Rumor) {
        match &rumor.kind {
            RumorKind::Depart => {}
            RumorKind::Pull(_memberlist) => {}
            RumorKind::Push(_memberlist) => {}
            RumorKind::Alive(ip, port) => {
                if rumor.peer_id == self.id {
                    self.incarnation += 1;
                    self.updates.push_front(Update {
                        msg: rumor.clone(),
                        sends: 0,
                    });
                } else if let Some(peer) = self.membership.get_mut(&rumor.peer_id) {
                    if rumor.incarnation > peer.incarnation {
                        if peer.state == PeerState::Failed {
                            // rejoin!
                        } else if peer.state != PeerState::Alive {
                            info!("{:03}@{} marking {:03} as Alive", self.id, self.tick, peer);
                        }
                        peer.state = PeerState::Alive;
                        peer.incarnation = rumor.incarnation;
                        self.updates.push_front(Update {
                            msg: rumor.clone(),
                            sends: 0,
                        });
                    }
                } else {
                    self.add(rumor.peer_id, *ip, *port, rumor.incarnation);
                    self.updates.push_front(Update {
                        msg: rumor.clone(),
                        sends: 0,
                    });
                }
            }
            RumorKind::Suspect => {
                if rumor.peer_id == self.id {
                    // Reports of my death have been greatly exagerrated.
                    self.updates.push_front(Update {
                        msg: Rumor {
                            peer_id: self.id,
                            incarnation: self.incarnation,
                            kind: RumorKind::Alive(self.ip, self.port),
                        },
                        sends: 0,
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
                        self.updates.push_front(Update {
                            msg: rumor.clone(),
                            sends: 0,
                        });
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
                    self.updates.push_front(Update {
                        msg: rumor.clone(),
                        sends: 0,
                    });
                }
            }
        }
    }

    fn gossip(&mut self) -> Vec<Rumor> {
        let mut msgs = Vec::new();
        let n = (self.membership.len() + 2) as f32;
        let max_sends = 3 * n.log10().ceil() as usize;
        // From the paper
        self.suspicion_period = self.gossip_interval * max_sends;
        while msgs.len() < PIGGYBACKED_MSGS {
            if let Some(mut update) = self.updates.pop_front() {
                let dm = update.msg.clone();
                update.sends += 1;
                if update.sends < max_sends {
                    self.updates.push_back(update);
                }
                msgs.push(dm);
            } else {
                break;
            }
        }
        msgs
    }

    pub fn process(&mut self, sender: usize, msg: Message) {
        self.incarnation += 1;
        assert_eq!(
            msg.recipient, self.id,
            "Simulator bug; sent {:?} to the wrong node",
            msg
        );
        match msg.kind {
            MsgKind::Ping(sender_id, sender_ip, sender_port) => {
                self.add(sender_id, sender_ip, sender_port, 0);
                self.ack(self.id, sender_id);
            }
            MsgKind::PingReq {
                target_id,
                target_ip,
                target_port,
                sender_id,
                sender_ip,
                sender_port,
            } => {
                assert!(target_id != self.id);
                self.add(sender_id, sender_ip, sender_port, 0);
                self.ping(target_id, target_ip, target_port, sender_id);
            }
            MsgKind::Ack(peer_id, incarnation) => {
                if !self.pings.contains_key(&peer_id) {
                    debug!("{}@{} unexpected ack from {}", self.id, self.tick, peer_id);
                    return;
                }
                let Ping {
                    ip,
                    port,
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
                        self.updates.push_front(Update {
                            msg: Rumor {
                                peer_id,
                                incarnation,
                                kind: RumorKind::Alive(peer.ip, peer.port),
                            },
                            sends: 0,
                        });
                    }
                } else {
                    self.add(peer_id, ip, port, incarnation);
                    self.updates.push_front(Update {
                        msg: Rumor {
                            peer_id,
                            incarnation,
                            kind: RumorKind::Alive(ip, port),
                        },
                        sends: 0,
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
                self.updates.push_front(Update {
                    msg: Rumor {
                        peer_id: *node,
                        incarnation: peer.incarnation,
                        kind: RumorKind::Failed,
                    },
                    sends: 0,
                });
                to_rm.push(*node);
            } else if self.tick > (ping.sent_at + self.gossip_interval) {
                // At this point we throw out pings for non-member peers.
                if ping.state == PingState::FromElsewhere || !self.membership.contains_key(node) {
                    to_rm.push(*node);
                    continue;
                }
                let peer = self.membership.get(node).unwrap();
                debug!("{} suspects that {} has failed", self.id, node);
                self.updates.push_front(Update {
                    msg: Rumor {
                        peer_id: *node,
                        incarnation: peer.incarnation,
                        kind: RumorKind::Suspect,
                    },
                    sends: 0,
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
                    self.updates.push_front(Update {
                        msg: Rumor {
                            peer_id: *node,
                            incarnation,
                            kind: RumorKind::Suspect,
                        },
                        sends: 0,
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
                                target_ip: ping.ip,
                                target_port: ping.port,
                                sender_id: self.id,
                                sender_ip: self.ip,
                                sender_port: self.port,
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
            self.ping(ping_rcpt, ping_peer.ip, ping_peer.port, self.id);
            self.last_pinged += 1;
        }
        take(&mut self.outbox)
    }
}

#[cfg(test)]
mod test {}
