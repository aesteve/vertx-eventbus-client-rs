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
    use testcontainers::images::generic::{GenericImage, WaitFor};
    use testcontainers::*;

    fn mock_eventbus_server() -> GenericImage {
        GenericImage::new("aesteve/tests:mock-eventbus-server")
            .with_wait_for(WaitFor::message_on_stdout("TCP bridge connected"))
    }

    #[test]
    fn pub_sub_pattern() {
        let docker = clients::Cli::default();
        let node = docker.run(mock_eventbus_server());
        let host_port = node
            .get_host_port(7542)
            .expect("Mock event bus server implementation needs to be up before running tests");
        let (_, mut listener) =
            eventbus(format!("localhost:{}", host_port)).expect("Event bus creation must not fail");
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
        let docker = clients::Cli::default();
        let node = docker.run(mock_eventbus_server());
        let host_port = node
            .get_host_port(7542)
            .expect("Mock event bus server implementation needs to be up before running tests");
        let (mut publisher, mut listener) =
            eventbus(format!("localhost:{}", host_port)).expect("Event bus creation must not fail");
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
        let docker = clients::Cli::default();
        let node = docker.run(mock_eventbus_server());
        let host_port = node
            .get_host_port(7542)
            .expect("Mock event bus server implementation needs to be up before running tests");
        let (_, mut listener) =
            eventbus(format!("localhost:{}", host_port)).expect("Event bus creation must not fail");
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
