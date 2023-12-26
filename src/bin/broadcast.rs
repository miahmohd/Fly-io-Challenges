use flyio::{Body, Message, Node};

use anyhow::{bail, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{self},
    ops::Sub,
    thread,
    time::Duration,
};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast {
        message: i32,
    },
    BroadcastOk,

    Read,
    ReadOk {
        messages: HashSet<i32>,
    },

    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,

    Gossip {
        messages: HashSet<i32>,
    },
}

enum Event<P> {
    Message(Message<P>),
    Gossip,
}

#[derive(Debug, Default)]
struct BroadcastState {
    topology: HashMap<String, Vec<String>>,
    messages: HashSet<i32>,

    known_to_neighbors: HashMap<String, HashSet<i32>>,
}

fn main() -> Result<()> {
    let mut node: Node<BroadcastState, BroadcastPayload> = Node::from_init()?;
    let mut state = BroadcastState::default();

    let (tx, rx) = std::sync::mpsc::channel::<Event<BroadcastPayload>>();

    //
    // Spawn reader thread
    let r_tx = tx.clone();
    let r_th = thread::spawn(move || {
        let stdin = io::stdin().lock();
        let input =
            serde_json::Deserializer::from_reader(stdin).into_iter::<Message<BroadcastPayload>>();

        for msg in input {
            let msg = msg.expect("cannot deserialize input");
            let _ = r_tx.send(Event::Message(msg));
        }
    });

    //
    // Create gossip events
    let g_tx = tx.clone();
    let g_th = thread::spawn(move || loop {
        let mut rng = rand::thread_rng();
        thread::sleep(Duration::from_millis(100 + rng.gen_range(0..200)));
        let _ = g_tx.send(Event::Gossip);
    });

    //
    //
    for event in rx {
        match event {
            Event::Gossip => {
                // ignore given topology, each node is neighbor of half the nodes (next 13),
                // Include real neighbors to account for net partitions

                let new_neighbors = get_neighbors(&node, &state);

                eprintln!(
                    "nodes: {:?}, new neighbors: {:?}, oldneighbors: {:?}",
                    node.node_ids,
                    new_neighbors,
                    state.topology.get(&node.id).unwrap_or(&Vec::new())
                );

                for n in new_neighbors {
                    // Send only messages not known to n
                    let t = &HashSet::new();
                    let known_messages_to_n = state.known_to_neighbors.get(&n).unwrap_or(t);
                    node.send(&Message {
                        src: node.id.clone(),
                        dest: n.clone(),
                        body: Body {
                            msg_id: Some(node.message_id.clone()),
                            in_reply_to: None,
                            payload: BroadcastPayload::Gossip {
                                messages: state.messages.sub(known_messages_to_n),
                                // messages: state.messages.clone(),
                            },
                        },
                    })?;
                }
            }
            //
            //
            Event::Message(msg) => match msg.body.payload {
                BroadcastPayload::Gossip { messages } => {
                    state
                        .known_to_neighbors
                        .entry(msg.src)
                        .and_modify(|s| s.extend(messages.clone()))
                        .or_insert(HashSet::new());
                    state.messages.extend(messages)
                }

                BroadcastPayload::Broadcast { message } => {
                    let reply = Message {
                        src: node.id.clone(),
                        dest: msg.src.clone(),
                        body: Body {
                            msg_id: Some(node.message_id.clone()),
                            in_reply_to: msg.body.msg_id,
                            payload: BroadcastPayload::BroadcastOk,
                        },
                    };

                    node.send(&reply)?;

                    if !state.messages.contains(&message) {
                        // for neighbor in get_neighbors(&node, &state) {
                        //     if neighbor == msg.src {
                        //         continue;
                        //     }

                        //     let reply = Message {
                        //         src: node.id.clone(),
                        //         dest: neighbor,
                        //         body: Body {
                        //             msg_id: Some(node.message_id.clone()),
                        //             in_reply_to: None,
                        //             payload: BroadcastPayload::Broadcast { message },
                        //         },
                        //     };

                        //     node.send(&reply)?;
                        // }

                        state.messages.insert(message);
                    }
                }

                BroadcastPayload::Topology { topology } => {
                    eprintln!("Topology : {:?}", topology);
                    state.topology = topology;
                    let reply = Message {
                        src: node.id.clone(),
                        dest: msg.src,
                        body: Body {
                            msg_id: Some(node.message_id.clone()),
                            in_reply_to: msg.body.msg_id,
                            payload: BroadcastPayload::TopologyOk,
                        },
                    };

                    node.send(&reply)?;
                }

                BroadcastPayload::Read => {
                    let reply = Message {
                        src: node.id.clone(),
                        dest: msg.src,
                        body: Body {
                            msg_id: Some(node.message_id.clone()),
                            in_reply_to: msg.body.msg_id,
                            payload: BroadcastPayload::ReadOk {
                                messages: state.messages.clone(),
                            },
                        },
                    };

                    node.send(&reply)?;
                }

                BroadcastPayload::BroadcastOk => {
                    eprintln!("Broadcast Ok")
                }
                _ => bail!("unknown payload: {:?}", msg),
            },
        }
    }

    let _ = r_th.join();
    let _ = g_th.join();

    Ok(())
}

fn get_neighbors(
    node: &Node<BroadcastState, BroadcastPayload>,
    state: &BroadcastState,
) -> BTreeSet<String> {
    // let neighbors = BTreeSet::new();

    let i = node
        .node_ids
        .iter()
        .position(|n| node.id == *n)
        .expect("cannot fine node in nodes_ids");

    let t = Vec::new();

    // node.node_ids
    //     .clone()
    //     .iter()
    //     .map(|s| s.clone())
    //     .cycle()
    //     .skip(i + 1)
    //     .take(10)
    //     .chain(
    //         state
    //             .topology
    //             .get(&node.id)
    //             .unwrap_or(&t)
    //             .iter()
    //             .map(|s| s.clone()),
    //     )
    //     .collect::<BTreeSet<_>>()
    state
        .topology
        .get(&node.id)
        .unwrap_or(&t)
        .iter()
        .map(|s| s.clone())
        .collect()

    // neighbors
}
