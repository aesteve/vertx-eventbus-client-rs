use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::mpsc::Receiver;

pub struct MessageConsumer {
    pub(crate) msg_queue: Receiver<Message>,
}

impl Iterator for MessageConsumer {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.msg_queue.try_recv().ok()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all(serialize = "lowercase", deserialize = "lowercase"))]
#[serde(tag = "type")]
// FIXME: split outgoing/incoming + internal/user
pub enum Message {
    // internal use -> heartbeat
    Ping, // outgoing
    Pong, // incoming
    // internal, primitives associated to user actions, outgoing
    Register(RegisterMessage),
    Unregister(RegisterMessage),

    // user, incoming messages
    Err(ErrorMessage),
    Message(FullMessage),
    // user, outgoing message
    Send(SendMessage),
    Publish(FullMessage),
}

impl Message {
    pub(crate) fn address(&self) -> Option<String> {
        match self {
            Self::Message(msg) => Some(msg.address.clone()),
            Self::Send(sent) => Some(sent.address.clone()),
            Self::Publish(published) => Some(published.address.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct FullMessage {
    pub(crate) address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ErrorMessage {
    pub(crate) message: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SendMessage {
    pub(crate) address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reply_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) body: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct RegisterMessage {
    pub(crate) address: String,
}

#[cfg(test)]
mod tests {
    use crate::message::{Message, SendMessage};
    use serde_json::json;

    const JSON_PING: &str = r#"{"type":"ping"}"#;
    const JSON_SEND: &str = r#"{"type":"send","address":"the-address","body":{}}"#;

    #[test]
    fn unmarshall_messages() {
        assert_eq!(Message::Ping, serde_json::from_str(JSON_PING).unwrap());

        match serde_json::from_str(JSON_SEND).unwrap() {
            Message::Send(msg) => assert_eq!(
                SendMessage {
                    address: "the-address".to_string(),
                    body: Some(json!({})),
                    reply_address: None,
                    headers: None
                },
                msg
            ),
            other => panic!(format!("Expecting a SEND message, not {:?}", other)),
        };
    }

    #[test]
    fn marshall_messages() {
        let msg: String = serde_json::to_string(&Message::Ping).unwrap();
        assert_eq!(JSON_PING, msg);

        let msg: String = serde_json::to_string(&Message::Send(SendMessage {
            address: "the-address".to_string(),
            body: Some(json!({})),
            reply_address: None,
            headers: None,
        }))
        .unwrap();
        assert_eq!(JSON_SEND, msg);
    }
}
