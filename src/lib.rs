use std::io;
mod listener;
mod message;
mod publisher;
mod utils;
use crate::listener::EventBusListener;
use crate::publisher::EventBusPublisher;
use std::net::{TcpStream, ToSocketAddrs};

pub fn eventbus<A: ToSocketAddrs>(address: A) -> io::Result<(EventBusPublisher, EventBusListener)> {
    let socket = TcpStream::connect(&address)?;
    socket.set_nonblocking(true)?;
    let control_socket = socket // used to send control messages (ping/pong ; register / unregister)
        .try_clone()?; // see: https://github.com/rust-lang/rust/issues/11165
    let w_socket = socket // used by the API user to publish / send outgoing messages
        .try_clone()?; // see: https://github.com/rust-lang/rust/issues/11165
    Ok((
        EventBusPublisher::new(w_socket),
        EventBusListener::new(control_socket),
    ))
}

#[cfg(test)]
mod tests {
    use crate::eventbus;
    use crate::message::{FullMessage, InMessage, SendMessage};
    use serde_json::json;

    /// These are integration test (should be moved to another cfg?)
    ///     to avoid the "observator bias" (testing our understanding of the protocol, rather than the real protocol)
    /// For them to work fine, one must first run ` java -jar testutils/vertx-eventbusbridge-test-1.0-SNAPSHOT-all.jar`
    #[test]
    fn pub_sub_pattern() {
        let (_, mut listener) = eventbus("127.0.0.1:7542").unwrap();
        let mut consumer = listener.consumer("out-address".to_string()).unwrap();
        let mut received_msgs = Vec::new();
        while received_msgs.len() < 3 {
            if let Some(msg) = consumer.next() {
                if let InMessage::Message(fm) = msg {
                    println!("From user code, message is: {:?}", fm);
                    assert!(received_msgs
                        .iter()
                        .find(|m: &&FullMessage| m.body == fm.body)
                        .is_none()); // same message has not been received twice
                    received_msgs.push(fm);
                }
            }
        }
        listener
            .unregister_consumer("out-address".to_string())
            .unwrap();
    }

    #[test]
    fn send_reply_pattern() {
        let (mut publisher, mut listener) = eventbus("127.0.0.1:7542").unwrap();
        let reply_address = "the-reply-address";
        let mut consumer = listener.consumer(reply_address.to_string()).unwrap();
        let payload = json!({"test": "value"});
        let expected_payload = payload.clone();
        publisher
            .send(SendMessage {
                address: "echo-address".to_string(),
                reply_address: Some(reply_address.to_string()),
                body: Some(payload),
                headers: None,
            })
            .unwrap();
        let mut received_msgs = 0;
        while received_msgs == 0 {
            if let Some(msg) = consumer.next() {
                if let InMessage::Message(fm) = msg {
                    assert_eq!(reply_address, fm.address);
                    assert_eq!(expected_payload, fm.body.unwrap());
                    received_msgs += 1;
                }
            }
        }
    }
}
