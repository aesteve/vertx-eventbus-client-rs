use std::io;
mod listener;
mod message;
mod publisher;
mod utils;
use crate::listener::EventBusListener;
use crate::publisher::EventBusPublisher;
use std::net::{TcpStream, ToSocketAddrs};

pub fn eventbus<A: ToSocketAddrs + Send + Sync>(
    address: A,
) -> io::Result<(EventBusPublisher, EventBusListener)> {
    TcpStream::connect(&address).map(|socket| {
        socket.set_nonblocking(true).unwrap();
        let notif_socket =
            socket // used to send control messages (ping/pong ; register / unregister)
                .try_clone() // see: https://github.com/rust-lang/rust/issues/11165
                .expect("Could not clone TCP connection to send control frames");
        let write_stream = socket // used by the API user to publish / send outgoing messages
            .try_clone() // see: https://github.com/rust-lang/rust/issues/11165
            .expect("Could not clone TCP connection to send messages");
        (
            EventBusPublisher::new(write_stream),
            EventBusListener::new(notif_socket),
        )
    })
}

#[cfg(test)]
mod tests {
    use crate::eventbus;
    use crate::message::{Message, SendMessage};
    use serde_json::json;
    use std::thread;
    use std::time::Duration;

    fn print_msg(msg: Message) {
        println!("From user code, message is: {:?}", msg);
    }

    /// These are integration test (should be moved to another cfg?) to avoid the "observator bias" (testing our understanding of the procotol, rather than the real protocol)
    /// For them to work fine, one must first run ` java -jar testutils/vertx-eventbusbridge-test-1.0-SNAPSHOT-all.jar`
    #[test]
    fn can_create_the_bridge() {
        let (mut publisher, mut listener) = eventbus("127.0.0.1:7542").unwrap();
        publisher.ping().unwrap();
        publisher.ping().unwrap();
        listener
            .register_consumer("out-address".to_string(), &print_msg)
            .unwrap();
        publisher
            .send(SendMessage {
                address: "echo-address".to_string(),
                reply_address: None,
                body: Some(json!({"test": "value"})),
                headers: None,
            })
            .unwrap();
        thread::sleep(Duration::from_secs(5));
    }
}
