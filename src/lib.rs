use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io};

pub struct Node<S, P> {
    pub id: String,
    pub node_ids: Vec<String>,
    pub message_id: i32,

    rpc_handlers: HashMap<i32, Box<dyn Fn(&Self, &mut S, &Message<P>) -> Result<()>>>,
}

impl<S, P> Node<S, P>
where
    P: Serialize,
{
    pub fn from_init() -> Result<Self> {
        let stdin = io::stdin().lock();

        let mut input =
            serde_json::Deserializer::from_reader(stdin).into_iter::<Message<InitPayload>>();

        if let Some(Ok(init)) = input.next() {
            match init.body.payload {
                InitPayload::Init { node_id, node_ids } => {
                    let mut s = Self {
                        id: node_id.clone(),
                        node_ids: node_ids,
                        message_id: 0,
                        rpc_handlers: Default::default(),
                    };

                    let reply = Message {
                        src: node_id,
                        dest: init.src,
                        body: Body {
                            msg_id: Some(s.message_id),
                            in_reply_to: init.body.msg_id,
                            payload: InitPayload::InitOk {},
                        },
                    };

                    println!("{}", serde_json::to_string(&reply)?);

                    s.message_id += 1;
                    return Ok(s);
                }

                _ => unreachable!(),
            }
        }

        Err(anyhow!("cannot initialize node"))
    }

    pub fn send(&mut self, to_send: &Message<P>) -> Result<()> {
        println!("{}", serde_json::to_string(&to_send)?);
        self.message_id += 1;
        Ok(())
    }

    pub fn rpc(
        &mut self,
        to_send: &Message<P>,
        res_handler: Box<dyn Fn(&Self, &mut S, &Message<P>) -> Result<()>>,
    ) -> Result<()> {
        self.rpc_handlers
            .insert(to_send.body.msg_id.unwrap(), res_handler);

        self.send(&to_send)
    }

    pub fn handle(&mut self, state: &mut S, msg: &Message<P>) -> Result<()> {
        if let Some((_, h)) = self
            .rpc_handlers
            .remove_entry(&msg.body.in_reply_to.unwrap())
        {
            return h(self, state, msg);
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<P> {
    /// "src":  A string identifying the node this message came from
    pub src: String,
    /// "dest": A string identifying the node this message is to
    pub dest: String,
    /// "body": An object: the payload of the message
    pub body: Body<P>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Body<P> {
    /// "msg_id": (optional)  A unique integer identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<i32>,

    /// "in_reply_to": (optional)  For req/response, the msg_id of the request
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<i32>,

    #[serde(flatten)]
    pub payload: P,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },

    InitOk {},
}
