#[macro_use]
extern crate log;

use clap::Parser;
use humantime;
use rand::prelude::*;
use rand::thread_rng;
use std::collections::HashMap;
use std::iter::zip;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use surf::{Message, Rumor, Server};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Initial cluster size
    #[clap(short, long, default_value_t = 13)]
    n: usize,

    /// Failure detection subgroup size
    #[clap(short, long, default_value_t = 3)]
    k: usize,

    /// Probability that a message will be delayed by a round
    #[clap(long, default_value_t = 0.05)]
    p_delay: f32,

    /// Probability of message loss
    #[clap(long, default_value_t = 0.01)]
    p_loss: f32,

    /// Message round-trip-time
    #[clap(short, long)]
    rtt: humantime::Duration,

    /// How often the failure detection protocol should run
    #[clap(short, long)]
    protocol_period: humantime::Duration,
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    let ival: std::time::Duration = args.protocol_period.into();
    let sus_period = ival * 3 * ((args.n + 1) as f32).log10().ceil() as u32;
    let base_port: u16 = 32000;
    let mut nodes: HashMap<u64, Server> = (0..args.n)
        .map(|id| {
            (
                id as u64,
                Server::new(
                    id as u64,
                    SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        base_port + id as u16,
                    ),
                    args.rtt.into(),
                    args.k,
                    args.protocol_period.into(),
                    sus_period,
                ),
            )
        })
        .collect();
    let mut messages: Vec<(u64, Message)> = (1..args.n)
        .map(|id| {
            (
                id as u64,
                nodes.get_mut(&(id as u64)).unwrap().join(
                    0,
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), base_port),
                ),
            )
        })
        .map(|(id, opt)| (id, opt.unwrap()))
        .collect();
    // (# rumors, serialized rumors)
    let mut gossip: Vec<[u8; 64]> = (1..args.n).map(|_| [0; 64]).collect();

    info!("Created cluster of {} nodes", args.n);
    let mut rng = thread_rng();
    loop {
        let mut next_msgs = Vec::new();
        let mut next_gossip = Vec::new();
        for node in nodes.values_mut() {
            for msg in node.tick().into_iter() {
                next_msgs.push((node.id, msg));
                let mut goss = [0u8; 64];
                node.gossip(&mut goss);
                next_gossip.push(goss);
            }
        }
        for ((sender, msg), rumor_buf) in zip(messages, gossip) {
            if rng.gen::<f32>() < args.p_delay {
                trace!("{:03} -- {:?} -? {:03}", sender, msg.kind, msg.dest_id);
                next_msgs.push((sender, msg));
                continue;
            } else if rng.gen::<f32>() < args.p_loss {
                trace!("{:03} -- {:?} -X {:03}", sender, msg.kind, msg.dest_id);
                continue;
            }
            trace!("{:03} -- {:?} -> {:03}", sender, msg.kind, msg.dest_id);
            let dest = msg.dest_id;
            let node = nodes.get_mut(&dest).unwrap();
            if let Some(msg) = node.process(msg) {
                next_msgs.push((node.id, msg));
                let mut goss = [0u8; 64];
                node.gossip(&mut goss);
                next_gossip.push(goss);
            }
            node.process_gossip(&rumor_buf[..])
                .expect("should process rumors");
        }

        messages = next_msgs;
        gossip = next_gossip;
    }
}
