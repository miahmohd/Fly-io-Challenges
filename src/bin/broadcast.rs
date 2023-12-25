use flyio::{Body, Message, Node};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{self},
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
        messages: Vec<i32>,
    },

    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

#[derive(Debug, Default)]
struct BroadcastState {
    topology: HashMap<String, Vec<String>>,
    messages: Vec<i32>,
}

fn main() -> Result<()> {
    let (tx, rx) = std::sync::mpsc::channel::<Message<BroadcastPayload>>();

    let mut node: Node<BroadcastState, BroadcastPayload> = Node::from_init()?;
    let mut state = BroadcastState::default();

    let stdin = io::stdin().lock();
    let input =
        serde_json::Deserializer::from_reader(stdin).into_iter::<Message<BroadcastPayload>>();
    // let mut ouput = BufWriter::new(stdout);

    for msg in input {
        let msg = msg.context("cannot deserialize input")?;

        match msg.body.payload {
            BroadcastPayload::Broadcast { message } => {
                state.messages.push(message);

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
        }
    }

    Ok(())
}
