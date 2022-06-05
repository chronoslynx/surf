#[macro_use]
extern crate log;

use clap::Parser;
use humantime;
use rand::prelude::*;
use rand::thread_rng;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use swimmer::{Message, Server};

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
    let mut nodes: HashMap<usize, Server> = (0..args.n)
        .map(|id| {
            (
                id,
                Server::new(
                    id,
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
    for id in 1..args.n {
        nodes.get_mut(&id).unwrap().join(
            0,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), base_port),
        );
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
            node.process(msg);
        }
        for node in nodes.values_mut() {
            for msg in node.tick().into_iter() {
                next_msgs.push((node.id, msg));
            }
        }
        messages = next_msgs;
    }
}
