use flyio::{Body, Message, Node};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{self},
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
        thread::sleep(Duration::from_millis(500));
        let _ = g_tx.send(Event::Gossip);
    });

    //
    //
    for event in rx {
        match event {
            Event::Gossip => {
                if let Some(neighbors) = state.topology.get(&node.id) {
                    for n in neighbors {
                        node.send(&Message {
                            src: node.id.clone(),
                            dest: n.clone(),
                            body: Body {
                                msg_id: Some(node.message_id.clone()),
                                in_reply_to: None,
                                payload: BroadcastPayload::Gossip {
                                    messages: state.messages.clone(),
                                },
                            },
                        })?;
                    }
                }
            }
            //
            //
            Event::Message(msg) => match msg.body.payload {
                BroadcastPayload::Gossip { messages } => state.messages.extend(messages),

                BroadcastPayload::Broadcast { message } => {
                    state.messages.insert(message);

                    let reply = Message {
                        src: node.id.clone(),
                        dest: msg.src,
                        body: Body {
                            msg_id: Some(node.message_id.clone()),
                            in_reply_to: msg.body.msg_id,
                            payload: BroadcastPayload::BroadcastOk,
                        },
                    };

                    node.send(&reply)?;
                }

                BroadcastPayload::Topology { topology } => {
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
                _ => bail!("unknown payload: {:?}", msg),
            },
        }
    }

    let _ = r_th.join();
    let _ = g_th.join();

    Ok(())
}
