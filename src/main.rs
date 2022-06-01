#[macro_use]
extern crate log;

use clap::Parser;
use rand::prelude::*;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Display,
};

/// Node states
#[derive(PartialEq, Debug, Copy, Clone)]
enum State {
    Alive,
    Suspect,
    Failed,
}

/// Dissemination Messages
#[derive(PartialEq, Debug, Copy, Clone)]
struct Rumor {
    node: usize,
    ts: usize,
    state: State,
}

/// Failure Detector messages. These piggy-back higher level data
#[derive(Debug)]
enum MsgKind {
    Ping,
    Ack(usize),
    PingReq(usize),
}

const PIGGYBACKED_MSGS: usize = 10;

#[derive(Debug)]
struct Message {
    recipient: usize,
    kind: MsgKind,
    gossip: [Option<Rumor>; PIGGYBACKED_MSGS],
}

struct Update {
    msg: Rumor,
    sends: u8,
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

struct Node {
    id: usize,
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
    membership: HashMap<usize, (State, usize)>,
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node({}, {})", self.id, self.clock)
    }
}

impl Node {
    fn process_gossip(&mut self, rumor: &Rumor) {
        match rumor.state {
            State::Alive => {
                if rumor.node == self.id {
                    self.clock += 1;
                } else if let Some((state, ts)) = self.membership.get(&rumor.node) {
                    assert!(*state != State::Failed);
                    if rumor.ts > *ts {
                        info!("{} marking {} as Alive", self.id, rumor.node);
                        self.membership.insert(rumor.node, (State::Alive, rumor.ts));
                    }
                }
                self.updates.push_front(Update {
                    msg: rumor.clone(),
                    sends: 0,
                });
            }
            State::Suspect => {
                if rumor.node == self.id {
                    // Reports of my death have been greatly exagerrated.
                    self.updates.push_front(Update {
                        msg: Rumor {
                            node: self.id,
                            ts: self.clock,
                            state: State::Alive,
                        },
                        sends: 0,
                    });
                } else if let Some((_, ts)) = self.membership.get(&rumor.node) {
                    if rumor.ts > *ts {
                        info!("{} marking {} as Suspect", self.id, rumor.node);
                        self.membership
                            .insert(rumor.node, (State::Suspect, rumor.ts));
                    }
                    self.updates.push_front(Update {
                        msg: rumor.clone(),
                        sends: 0,
                    });
                }
            }
            State::Failed => {
                warn!("{} marking {} as Failed", self.id, rumor.node);
                if let Some(_) = self.membership.remove(&rumor.node) {
                    let mut idx = usize::MAX;
                    for (i, node) in self.memberlist.iter().enumerate() {
                        if *node == rumor.node {
                            idx = i;
                            break;
                        }
                    }
                    info!(
                        "found {} at idx {} in {:?}",
                        rumor.node, idx, self.memberlist
                    );
                    assert!(idx != usize::MAX);
                    self.memberlist.swap_remove(idx);
                }
                self.updates.push_front(Update {
                    msg: rumor.clone(),
                    sends: 0,
                });
            }
        }
    }

    fn gossip(&mut self) -> [Option<Rumor>; PIGGYBACKED_MSGS] {
        let mut msgs = 0;
        let mut resp = [None; PIGGYBACKED_MSGS];
        while msgs < PIGGYBACKED_MSGS {
            if let Some(mut update) = self.updates.pop_front() {
                let dm = update.msg.clone();
                update.sends += 1;
                if update.sends < 3 {
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
    pub fn process(&mut self, sender: usize, msg: Message) -> Option<Message> {
        self.clock += 1;
        assert_eq!(
            msg.recipient, self.id,
            "Simulator bug; sent {:?} to the wrong node",
            msg
        );
        for rumor in msg.gossip.iter() {
            if let Some(ref rumor) = rumor {
                self.process_gossip(rumor);
            } else {
                break;
            }
        }
        match msg.kind {
            MsgKind::Ping => Some(Message {
                recipient: sender,
                kind: MsgKind::Ack(self.id),
                gossip: self.gossip(),
            }),
            MsgKind::PingReq(node) => {
                assert!(node != self.id);
                self.pings.insert(
                    node,
                    Ping {
                        requester: sender,
                        state: PingState::FromElsewhere,
                        sent_at: self.tick,
                    },
                );
                Some(Message {
                    recipient: node,
                    kind: MsgKind::Ping,
                    gossip: self.gossip(),
                })
            }
            MsgKind::Ack(node) => {
                if !self.pings.contains_key(&node) || !self.membership.contains_key(&node) {
                    info!("{}: unknown ack {:?}", self.id, msg);
                    return None;
                }
                let (state, ts) = self.membership.get_mut(&node).unwrap();
                if state != &State::Failed && self.clock > *ts {
                    *state = State::Alive;
                    *ts = self.clock;
                    self.updates.push_front(Update {
                        msg: Rumor {
                            node,
                            ts: self.clock,
                            state: State::Alive,
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
                    Some(Message {
                        recipient: requester,
                        kind: MsgKind::Ack(node),
                        gossip: self.gossip(),
                    })
                } else {
                    None
                }
            }
        }
    }
    pub fn tick(&mut self) -> Vec<Message> {
        self.tick += 1;
        // TODO: pick a node to ping
        if self.last_pinged >= self.memberlist.len() {
            let mut rng = thread_rng();
            self.memberlist.shuffle(&mut rng);
            self.last_pinged = 0;
        }
        let mut msgs = Vec::new();

        let mut to_rm = Vec::new();
        for (node, ping) in self.pings.iter_mut() {
            if self.tick > (ping.sent_at + self.suspicion_period) {
                assert!(ping.state == PingState::Forwarded);
                self.updates.push_front(Update {
                    msg: Rumor {
                        node: *node,
                        ts: self.tick,
                        state: State::Failed,
                    },
                    sends: 0,
                });
                to_rm.push(*node);
            } else if self.tick > (ping.sent_at + self.gossip_interval) {
                if ping.state == PingState::FromElsewhere {
                    to_rm.push(*node);
                    continue;
                }
                warn!("{} suspects that {} has failed", self.id, node);
                self.updates.push_front(Update {
                    msg: Rumor {
                        node: *node,
                        ts: self.tick,
                        state: State::Suspect,
                    },
                    sends: 0,
                });
            } else if ping.state != PingState::Forwarded
                && self.tick > (ping.sent_at + self.ping_interval)
            {
                if ping.state != PingState::Normal {
                    info!(
                        "{}: expire ping from {} to {}",
                        self.id, ping.requester, node
                    );
                    to_rm.push(*node);
                    continue;
                }
                // late, send ping_req to k nodes
                let mut chosen = HashSet::new();
                let mut rng = thread_rng();
                let subgroup_sz = self.pingreq_subgroup_sz.min(self.memberlist.len() - 1);
                while chosen.len() < subgroup_sz {
                    let recipient = self.memberlist.choose(&mut rng).unwrap();
                    if *recipient != *node && !chosen.contains(recipient) {
                        chosen.insert(*recipient);
                        msgs.push(Message {
                            recipient: *recipient,
                            kind: MsgKind::PingReq(*node),
                            gossip: [None; PIGGYBACKED_MSGS],
                        });
                    }
                }
                ping.state = PingState::Forwarded;
            }
        }
        for node in to_rm {
            info!("{}: expire ping to {}", self.id, node);
            self.pings.remove(&node);
        }
        for msg in msgs.iter_mut() {
            msg.gossip = self.gossip();
        }

        let ping_rcpt = self.memberlist[self.last_pinged];
        msgs.push(Message {
            recipient: ping_rcpt,
            kind: MsgKind::Ping,
            gossip: self.gossip(),
        });
        self.pings.insert(
            ping_rcpt,
            Ping {
                requester: self.id,
                state: PingState::Normal,
                sent_at: self.tick,
            },
        );
        self.last_pinged += 1;

        // loop over membership and take care of suspicious and faulty nodes
        msgs
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Initial cluster size
    #[clap(short, long, default_value_t = 13)]
    n: usize,

    /// Failure detection subgroup size
    #[clap(short, long, default_value_t = 3)]
    k: usize,

    /// Probability that a message will be delayed by another tick
    #[clap(long, default_value_t = 0.05)]
    p_delay: f32,

    /// Probability of message loss
    #[clap(long, default_value_t = 0.01)]
    p_loss: f32,

    /// Per-tick probability of node failure
    #[clap(long, default_value_t = 0.01)]
    p_fail: f32,

    /// Probability with which a new node will be introduced each tick
    #[clap(long, default_value_t = 0.00)]
    p_add: f32,

    /// Message round-trip-time in ticks.
    #[clap(short, long, default_value_t = 1)]
    rtt: usize,

    /// Gossip interval in ticks
    #[clap(short, long, default_value_t = 6)]
    gossip_interval: usize,

    /// Suspicion period in ticks
    #[clap(short, long, default_value_t = 12)]
    suspicion_period: usize,
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    let mut nodes: HashMap<usize, Node> = (0..args.n)
        .map(|i| {
            (
                i,
                Node {
                    id: i,
                    clock: 0,
                    tick: 0,
                    pingreq_subgroup_sz: args.k,
                    last_pinged: args.n,
                    ping_interval: args.rtt,
                    gossip_interval: args.gossip_interval,
                    suspicion_period: args.suspicion_period,
                    memberlist: Vec::new(),
                    updates: VecDeque::new(),
                    membership: HashMap::new(),
                    pings: HashMap::new(),
                },
            )
        })
        .collect();
    for node in nodes.values_mut() {
        node.memberlist = (0..args.n).filter(|i| *i != node.id).collect();
        node.membership = (0..args.n)
            .filter(|i| *i != node.id)
            .map(|i| (i, (State::Alive, 0)))
            .collect();
    }
    info!("Created cluster of {} nodes", args.n);
    let mut messages: Vec<(usize, Message)> = Vec::new();
    let mut rng = thread_rng();
    loop {
        let mut next_msgs = Vec::new();
        for (sender, msg) in messages.into_iter() {
            if rng.gen::<f32>() < args.p_delay {
                trace!("{:03} -- {:?} -? {:03}", sender, msg.kind, msg.recipient);
                next_msgs.push((sender, msg));
                continue;
            } else if rng.gen::<f32>() < args.p_loss {
                trace!("{:03} -- {:?} -X {:03}", sender, msg.kind, msg.recipient);
                continue;
            }
            trace!("{:03} -- {:?} -> {:03}", sender, msg.kind, msg.recipient);
            let node = nodes.get_mut(&msg.recipient).unwrap();
            if let Some(m) = node.process(sender, msg) {
                next_msgs.push((node.id, m));
            }
        }
        for node in nodes.values_mut() {
            for msg in node.tick().into_iter() {
                next_msgs.push((node.id, msg));
            }
        }
        messages = next_msgs;
    }
}
