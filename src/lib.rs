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
    use crate::message::SendMessage;
    use serde_json::json;

    /// These are integration test (should be moved to another cfg?)
    ///     to avoid the "observator bias" (testing our understanding of the protocol, rather than the real protocol)
    /// For them to work fine, one must first run ` java -jar testutils/vertx-eventbusbridge-test-1.0-SNAPSHOT-all.jar`
    #[test]
    fn bridge_integration() {
        let (mut publisher, mut listener) = eventbus("127.0.0.1:7542").unwrap();
        publisher.ping().unwrap();
        publisher.ping().unwrap();
        let mut consumer = listener.consumer("out-address".to_string()).unwrap();
        publisher
            .send(SendMessage {
                address: "echo-address".to_string(),
                reply_address: None,
                body: Some(json!({"test": "value"})),
                headers: None,
            })
            .unwrap();
        let mut received_msgs = 0;
        while received_msgs < 3 {
            if let Some(msg) = consumer.next() {
                println!("From user code, message is: {:?}", msg);
                received_msgs += 1;
            }
        }
    }
}
