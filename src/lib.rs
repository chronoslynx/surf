#[macro_use]
extern crate log;

use rand::prelude::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
};

/// Node states
#[derive(PartialEq, Debug, Copy, Clone)]
pub enum Command {
    Alive,
    Suspect,
    Failed,
    // How to handle custom user commands?
    User(u8),
}

/// Dissemination Messages
#[derive(PartialEq, Debug, Copy, Clone)]
struct Rumor {
    node: usize,
    ts: usize,
    command: Command,
    // payload?
}

/// Failure Detector messages. These piggy-back higher level data
#[derive(Debug)]
pub enum MsgKind {
    Ping,
    Ack(usize),
    PingReq(usize),
}

const PIGGYBACKED_MSGS: usize = 10;

#[derive(Debug)]
pub struct Message {
    pub recipient: usize,
    pub kind: MsgKind,
    // TODO: how can I support arbitrary messages?
    gossip: [Option<Rumor>; PIGGYBACKED_MSGS],
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
    requester: usize,
    state: PingState,
    sent_at: usize,
}

pub struct Node {
    pub id: usize,
    tick: usize,
    clock: usize,
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
    membership: HashMap<usize, (Command, usize)>,
    outbox: Vec<Message>,
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node({}, {})", self.id, self.clock)
    }
}

impl Node {
    pub fn new(
        id: usize,
        ping_interval: usize,
        pingreq_subgroup_sz: usize,
        gossip_interval: usize,
        suspicion_period: usize,
    ) -> Self {
        Node {
            id,
            clock: 0,
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
            kind: MsgKind::Ack(node),
            gossip: self.gossip(),
        };
        self.outbox.push(m);
    }

    fn ping(&mut self, node: usize, recipient: usize) {
        let m = Message {
            recipient: node,
            kind: MsgKind::Ping,
            gossip: self.gossip(),
        };
        self.outbox.push(m);
        let state = if recipient != self.id {
            PingState::FromElsewhere
        } else {
            PingState::Normal
        };
        self.pings.insert(
            node,
            Ping {
                requester: recipient,
                state,
                sent_at: self.tick,
            },
        );
    }

    /// Add a peer to our network. The peer is not reflected in the member list
    /// until it has responded to a ping.
    pub fn add_peer(&mut self, id: usize) {
        if self.membership.contains_key(&id) {
            return;
        }
        self.memberlist.push(id);
        self.membership.insert(id, (Command::Alive, self.clock));
        self.updates.push_front(Update {
            msg: Rumor {
                node: id,
                ts: self.clock,
                command: Command::Suspect,
            },
            sends: 0,
        });
        self.ping(id, self.id);
    }

    fn process_gossip(&mut self, rumor: &Rumor) {
        match rumor.command {
            Command::User(_tag) => {
                todo!()
            }
            Command::Alive => {
                if rumor.node == self.id {
                    self.clock += 1;
                    self.updates.push_front(Update {
                        msg: rumor.clone(),
                        sends: 0,
                    });
                } else if let Some((state, ts)) = self.membership.get(&rumor.node) {
                    assert!(*state != Command::Failed);
                    if rumor.ts > *ts {
                        if *state != Command::Alive {
                            info!("{} marking {} as Alive", self.id, rumor.node);
                        }
                        self.membership
                            .insert(rumor.node, (Command::Alive, rumor.ts));
                        self.updates.push_front(Update {
                            msg: rumor.clone(),
                            sends: 0,
                        });
                    }
                } else {
                    // new node!
                    info!("{} discovered {}", self.id, rumor.node);
                    self.membership
                        .insert(rumor.node, (Command::Alive, rumor.ts));
                    let mut rng = thread_rng();
                    let n: usize = rng.gen_range(0..=self.memberlist.len());
                    self.memberlist.insert(n, rumor.node);
                    self.updates.push_front(Update {
                        msg: rumor.clone(),
                        sends: 0,
                    });
                }
            }
            Command::Suspect => {
                if rumor.node == self.id {
                    // Reports of my death have been greatly exagerrated.
                    self.updates.push_front(Update {
                        msg: Rumor {
                            node: self.id,
                            ts: self.clock,
                            command: Command::Alive,
                        },
                        sends: 0,
                    });
                } else if let Some((state, ts)) = self.membership.get(&rumor.node) {
                    if rumor.ts > *ts {
                        if *state != Command::Suspect {
                            info!("{} marking {} as Suspect", self.id, rumor.node);
                        }
                        self.membership
                            .insert(rumor.node, (Command::Suspect, rumor.ts));
                        self.updates.push_front(Update {
                            msg: rumor.clone(),
                            sends: 0,
                        });
                    }
                }
            }
            Command::Failed => {
                warn!("{} marking {} as Failed", self.id, rumor.node);
                if let Some(_) = self.membership.remove(&rumor.node) {
                    let mut idx = usize::MAX;
                    for (i, node) in self.memberlist.iter().enumerate() {
                        if *node == rumor.node {
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

    fn gossip(&mut self) -> [Option<Rumor>; PIGGYBACKED_MSGS] {
        let mut msgs = 0;
        let mut resp = [None; PIGGYBACKED_MSGS];
        let N = (self.membership.len() + 2) as f32;
        let max_sends = 3 * N.log10().ceil() as usize;
        // From the paper
        self.suspicion_period = self.gossip_interval * max_sends;
        while msgs < PIGGYBACKED_MSGS {
            if let Some(mut update) = self.updates.pop_front() {
                let dm = update.msg.clone();
                update.sends += 1;
                if update.sends < max_sends {
                    self.updates.push_back(update);
                }
                resp[msgs] = Some(dm);
                msgs += 1;
            } else {
                break;
            }
        }
        return resp;
    }

    // FIXME report higher-level things here like "Failed"
    pub fn process(&mut self, sender: usize, msg: Message) {
        self.clock += 1;
        assert_eq!(
            msg.recipient, self.id,
            "Simulator bug; sent {:?} to the wrong node",
            msg
        );
        match msg.kind {
            MsgKind::Ping => self.ack(self.id, sender),
            MsgKind::PingReq(node) => {
                assert!(node != self.id);
                self.ping(node, sender);
            }
            MsgKind::Ack(node) => {
                if !self.pings.contains_key(&node) || !self.membership.contains_key(&node) {
                    debug!("{}: unexpected ack from {}", self.id, node);
                    return;
                }
                if let Some((state, ts)) = self.membership.get_mut(&node) {
                    if state != &Command::Failed && self.clock > *ts {
                        *state = Command::Alive;
                        *ts = self.clock;
                        self.updates.push_front(Update {
                            msg: Rumor {
                                node,
                                ts: self.clock,
                                command: Command::Alive,
                            },
                            sends: 0,
                        });
                    }
                } else {
                    // New node!
                    // TODO: relay up the chain
                    self.membership.insert(node, (Command::Alive, self.clock));
                    self.updates.push_front(Update {
                        msg: Rumor {
                            node,
                            ts: self.clock,
                            command: Command::Alive,
                        },
                        sends: 0,
                    });
                }
                let Ping {
                    requester,
                    state: _,
                    sent_at: _,
                } = self.pings.remove(&node).unwrap();
                if requester != self.id {
                    self.ack(node, requester);
                }
            }
        };

        for rumor in msg.gossip.iter() {
            if let Some(ref rumor) = rumor {
                self.process_gossip(rumor);
            } else {
                break;
            }
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
        for (node, ping) in self.pings.iter_mut() {
            if self.tick > (ping.sent_at + self.suspicion_period) {
                assert!(ping.state == PingState::Forwarded);
                self.updates.push_front(Update {
                    msg: Rumor {
                        node: *node,
                        ts: self.tick,
                        command: Command::Failed,
                    },
                    sends: 0,
                });
                to_rm.push(*node);
            } else if self.tick > (ping.sent_at + self.gossip_interval) {
                if ping.state == PingState::FromElsewhere {
                    to_rm.push(*node);
                    continue;
                }
                debug!("{} suspects that {} has failed", self.id, node);
                self.updates.push_front(Update {
                    msg: Rumor {
                        node: *node,
                        ts: self.tick,
                        command: Command::Suspect,
                    },
                    sends: 0,
                });
            } else if ping.state != PingState::Forwarded
                && self.tick > (ping.sent_at + self.ping_interval)
            {
                if ping.state != PingState::Normal {
                    debug!(
                        "{}: expire ping from {} to {}",
                        self.id, ping.requester, node
                    );
                    to_rm.push(*node);
                    continue;
                }
                // late, send ping_req to k nodes
                let mut chosen = HashSet::new();
                let mut rng = thread_rng();
                let subgroup_sz = self.pingreq_subgroup_sz.min(self.memberlist.len());
                if self.memberlist.len() <= 1 {
                    debug!("{} suspects that {} has failed", self.id, node);
                    to_rm.push(*node);
                    self.updates.push_front(Update {
                        msg: Rumor {
                            node: *node,
                            ts: self.tick,
                            command: Command::Suspect,
                        },
                        sends: 0,
                    });
                    continue;
                }
                while chosen.len() < subgroup_sz {
                    let recipient = self.memberlist.choose(&mut rng).unwrap();
                    if *recipient != *node && !chosen.contains(recipient) {
                        chosen.insert(*recipient);
                        let m = Message {
                            recipient: *recipient,
                            kind: MsgKind::PingReq(*node),
                            gossip: [None; PIGGYBACKED_MSGS],
                        };
                        self.outbox.push(m);
                    }
                }
                ping.state = PingState::Forwarded;
            }
        }
        for node in to_rm {
            debug!("{}: expire ping to {}", self.id, node);
            self.pings.remove(&node);
        }
        if !self.membership.is_empty() {
            let ping_rcpt = self.memberlist[self.last_pinged];
            self.ping(ping_rcpt, self.id);
            self.last_pinged += 1;
        }
        std::mem::replace(&mut self.outbox, Vec::new())
    }
}
