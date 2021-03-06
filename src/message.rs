use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::mpsc::Receiver;

pub type UserMessage<T> = Result<T, ErrorKind>;

pub struct MessageConsumer<T> {
    pub msg_queue: Receiver<UserMessage<T>>,
}

impl<T> Iterator for MessageConsumer<T> {
    type Item = UserMessage<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.msg_queue.try_recv().ok()
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all(serialize = "lowercase", deserialize = "lowercase"))]
#[serde(tag = "type")]
pub enum InMessage {
    Pong,
    // user, incoming messages
    Err(ErrorMessage),
    Message(Message),
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(rename_all(serialize = "lowercase", deserialize = "lowercase"))]
#[serde(tag = "type")]
pub enum OutMessage {
    // internal, control
    Ping, // outgoing
    // internal, primitives associated to user actions, outgoing
    Register(RegisterMessage),
    Unregister(RegisterMessage),
    // user, outgoing message
    Send(SendMessage),
    Publish(Message),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ErrorMessage {
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SendMessage {
    pub address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct RegisterMessage {
    pub address: String,
}

#[cfg(test)]
mod tests {
    use crate::message::{InMessage, Message, OutMessage, SendMessage};
    use serde_json::json;

    const JSON_PING: &str = r#"{"type":"ping"}"#;
    const JSON_PONG: &str = r#"{"type":"pong"}"#;

    const JSON_SEND: &str =
        r#"{"type":"send","address":"the-address","replyAddress":"the-reply-address","body":{}}"#;
    const JSON_RECEIVED: &str = r#"{"type":"message","address":"the-address","body":{}}"#;

    #[test]
    fn unmarshall_messages() {
        assert_eq!(InMessage::Pong, serde_json::from_str(JSON_PONG).unwrap());

        match serde_json::from_str(JSON_RECEIVED).unwrap() {
            InMessage::Message(msg) => assert_eq!(
                Message {
                    address: "the-address".to_string(),
                    body: Some(json!({})),
                    headers: None
                },
                msg
            ),
            other => panic!(format!("Expecting a message, not {:?}", other)),
        };
    }

    #[test]
    fn marshall_messages() {
        let msg: String = serde_json::to_string(&OutMessage::Ping).unwrap();
        assert_eq!(JSON_PING, msg);

        let msg: String = serde_json::to_string(&OutMessage::Send(SendMessage {
            address: "the-address".to_string(),
            body: Some(json!({})),
            reply_address: Some("the-reply-address".to_string()),
            headers: None,
        }))
        .unwrap();
        assert_eq!(JSON_SEND, msg);
    }
}
