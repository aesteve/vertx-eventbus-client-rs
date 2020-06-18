use crate::message::{Message, RegisterMessage, SendMessage};
use byteorder::{BigEndian, ByteOrder};
use std::io::Write;
use std::{io, thread};
mod message;
use buffered_reader::BufferedReader;
use std::collections::HashMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};

type MessageHandler = Box<dyn Fn(Message) + Send + 'static>;
pub struct EventBus {
    socket: TcpStream,
    handlers: Arc<Mutex<HashMap<String, MessageHandler>>>,
}

impl EventBus {
    pub fn create<A: ToSocketAddrs + Send + Sync>(address: A) -> io::Result<Self> {
        TcpStream::connect(&address).map(|socket| {
            socket.set_nonblocking(true).unwrap();
            let created = EventBus {
                socket,
                handlers: Arc::new(Mutex::new(HashMap::new())),
            };
            let read_stream = created
                .socket
                .try_clone() // see: https://github.com/rust-lang/rust/issues/11165
                .expect("Could not listen to TCP connection");
            let consumers = created.handlers.clone();
            thread::spawn(move || {
                reader_loop(read_stream, consumers);
            });
            created
        })
    }

    pub fn send(&mut self, msg: SendMessage) -> io::Result<&mut Self> {
        write(&self.socket, &Message::Send(msg)).map(|_| self)
    }

    pub fn ping(&mut self) -> io::Result<&mut Self> {
        write(&self.socket, &Message::Ping).map(|_| self)
    }

    pub fn register_consumer(
        &mut self,
        address: String,
        callback: &'static (dyn Fn(Message) + Send + Sync),
    ) -> io::Result<&mut Self> {
        self.handlers
            .lock()
            .expect("Could not add the callback to the list of consumers")
            .insert(address.clone(), Box::new(callback));
        write(
            &self.socket,
            &Message::Register(RegisterMessage { address }),
        )
        .map(|_| self)
    }
}

fn reader_loop(read_stream: TcpStream, handlers: Arc<Mutex<HashMap<String, MessageHandler>>>) {
    let mut socket = buffered_reader::Generic::new(&read_stream, Some(4096));
    loop {
        // first, read the 4 bytes indicating message length: `len`
        if let Ok(len) = socket.read_be_u32() {
            // then consume `len` bytes of data => it's a whole message
            if let Ok(read) = socket.data_consume(len as usize) {
                // event bus protocol is JSON encoded
                let json = std::str::from_utf8(&read[..len as usize]).unwrap();
                match serde_json::from_str::<Message>(&json) {
                    Ok(msg) => {
                        if let Some(address) = msg.address() {
                            if let Some(handler) = handlers
                                .lock()
                                .expect("Could not retrieve message handler for this address")
                                .get(address.as_str())
                            {
                                handler(msg)
                            }
                        }
                    }
                    Err(err) => println!(
                        "Invalid JSON received from EventBus: {}. Error: {:?}",
                        json, err
                    ),
                }
            }
        }
    }
}

fn write(mut socket: &TcpStream, msg: &Message) -> io::Result<()> {
    let mut buf = [0u8; 4];
    let msg_str = serde_json::to_string(msg).unwrap();
    BigEndian::write_u32(&mut buf, msg_str.len() as u32);
    socket.write_all(&buf).unwrap(); // write message length as Big Endian
    socket.write_all(&msg_str.as_bytes()).unwrap(); // write message
    socket.flush()
}

#[cfg(test)]
mod tests {
    use crate::message::{Message, SendMessage};
    use crate::EventBus;
    use serde_json::json;
    use std::thread;
    use std::time::Duration;

    fn print_msg(msg: Message) {
        println!("From user code, message is: {:?}", msg);
    }

    #[test]
    fn can_create_the_bridge() {
        let mut eb = EventBus::create("127.0.0.1:7542").unwrap();
        eb.ping().unwrap();
        eb.ping().unwrap();
        eb.ping().unwrap();
        eb.ping().unwrap();
        eb.register_consumer("out-address".to_string(), &print_msg)
            .unwrap();
        eb.send(SendMessage {
            address: "echo-address".to_string(),
            reply_address: None,
            body: Some(json!({"test": "value"})),
            headers: None,
        })
        .unwrap();
        thread::sleep(Duration::from_secs(5));
    }
}
