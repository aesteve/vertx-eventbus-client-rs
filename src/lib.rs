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
        .try_clone()?;
    let w_socket = socket // used by the API user to publish / send outgoing messages
        .try_clone()?;
    Ok((
        EventBusPublisher::new(w_socket)?,
        EventBusListener::new(control_socket)?,
    ))
}

#[cfg(test)]
mod tests {
    use crate::eventbus;
    use crate::message::{Message, SendMessage};
    use serde_json::json;

    const TCP_BRIDGE: &str = "127.0.0.1:7542";

    /// These are integration test (should be moved to another cfg?)
    ///     to avoid the "observator bias" (testing our understanding of the protocol, rather than the real protocol)
    /// For them to work fine, one must first run ` java -jar testutils/vertx-eventbusbridge-test-1.0-SNAPSHOT-all.jar`
    #[test]
    fn pub_sub_pattern() {
        let (_, mut listener) = eventbus(TCP_BRIDGE).expect("Event bus creation must not fail");
        let mut consumer = listener.consumer("out-address".to_string()).unwrap();
        let mut received_msgs = Vec::new();
        while received_msgs.len() < 3 {
            if let Some(Ok(msg)) = consumer.next() {
                assert!(received_msgs
                    .iter()
                    .find(|m: &&Message| m.body == msg.body)
                    .is_none()); // same message has not been received twice
                received_msgs.push(msg);
            }
        }
        listener
            .unregister_consumer("out-address".to_string())
            .expect("Unregistering consumer must not fail");
    }

    #[test]
    fn send_reply_pattern() {
        let (mut publisher, mut listener) =
            eventbus(TCP_BRIDGE).expect("Event bus creation must not fail");
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
            .expect("Sending a message to the event bus must work fine");
        let mut received_msgs = 0;
        while received_msgs == 0 {
            if let Some(Ok(msg)) = consumer.next() {
                assert_eq!(reply_address, msg.address);
                assert_eq!(
                    expected_payload,
                    msg.body.expect("Body should be extracted")
                );
                received_msgs += 1;
            }
        }
    }

    #[test]
    fn connect_to_an_unexisting_address_should_fail() {
        let eb = eventbus("127.0.0.1::1111");
        assert!(eb.is_err());
    }

    #[test]
    fn should_be_notified_of_errors() {
        let (_, mut listener) = eventbus(TCP_BRIDGE).expect("Event bus creation must not fail");
        let mut error_listener = listener
            .errors()
            .expect("Can ask for an iterator over error messages");
        listener
            .consumer("something_we_dont_have_access_to".to_string())
            .expect("Can subscribe to any address");
        let mut errors_received = 0;
        while errors_received < 1 {
            if let Some(Ok(error_msg)) = error_listener.next() {
                errors_received += 1;
                assert!(error_msg.message.contains("denied"))
            }
        }
    }
}
