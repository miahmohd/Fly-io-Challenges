use flyio::{Body, Message, Node};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::{self};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum EchoPayload {
    Echo { echo: String },

    EchoOk { echo: String },
}

fn main() -> Result<()> {
    let mut node: Node<(), EchoPayload> = Node::from_init()?;

    let stdin = io::stdin().lock();
    let input = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<EchoPayload>>();
    // let mut ouput = BufWriter::new(stdout);

    for msg in input {
        let msg = msg.context("cannot deserialize input")?;
        eprintln!("Received {:?}", msg);

        match msg.body.payload {
            EchoPayload::Echo { echo } => {
                let reply = Message {
                    src: node.id.clone(),
                    dest: msg.src,
                    body: Body {
                        msg_id: Some(node.message_id.clone()),
                        in_reply_to: msg.body.msg_id,
                        payload: EchoPayload::EchoOk { echo },
                    },
                };

                node.send(&reply)?;
            }

            _ => panic!("Unknown payload"),
        }
    }

    Ok(())
}
