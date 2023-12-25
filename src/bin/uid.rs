use flyio::{Body, Message, Node};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::{self};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum UIdPayload {
    Generate {},

    GenerateOk { id: String },
}

fn main() -> Result<()> {
    let mut node: Node<(), UIdPayload> = Node::from_init()?;

    let stdin = io::stdin().lock();
    let input = serde_json::Deserializer::from_reader(stdin).into_iter::<Message<UIdPayload>>();
    // let mut ouput = BufWriter::new(stdout);

    for msg in input {
        let msg = msg.context("cannot deserialize input")?;
        eprintln!("Received {:?}", msg);

        match msg.body.payload {
            UIdPayload::Generate {} => {
                let generated = format!("{}:{}", node.id, node.message_id);

                let reply = Message {
                    src: node.id.clone(),
                    dest: msg.src,
                    body: Body {
                        msg_id: Some(node.message_id),
                        in_reply_to: msg.body.msg_id,
                        payload: UIdPayload::GenerateOk { id: generated },
                    },
                };

                node.send(&reply)?;
            }

            _ => panic!("Unknown payload"),
        }
    }

    Ok(())
}
