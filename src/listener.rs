use crate::message::{Message, MessageConsumer, RegisterMessage};
use crate::utils::write_msg;
use buffered_reader::BufferedReader;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::{io, thread};

type MessageHandlersByAddress = Arc<Mutex<HashMap<String, Sender<Message>>>>;

pub struct EventBusListener {
    socket: TcpStream,
    handlers: MessageHandlersByAddress,
}

impl EventBusListener {
    pub(crate) fn new(socket: TcpStream) -> Self {
        let msg_dispatcher = socket
            .try_clone()
            .expect("Could not clone TCP connection for dispatching incoming messages");
        let listener = EventBusListener {
            socket,
            handlers: Arc::new(Mutex::new(HashMap::new())),
        };
        let consumers = listener.handlers.clone();
        thread::spawn(move || {
            reader_loop(msg_dispatcher, consumers);
        });
        listener
    }

    pub fn consumer(&mut self, address: String) -> io::Result<MessageConsumer> {
        let (tx, rx) = channel::<Message>();
        let handler = MessageConsumer { msg_queue: rx };
        self.handlers
            .lock()
            .expect("Could not add the callback to the list of consumers")
            .insert(address.clone(), tx);
        write_msg(
            &self.socket,
            &Message::Register(RegisterMessage { address }),
        )?;
        Ok(handler)
    }

    pub fn unregister_consumer(&mut self, address: String) -> io::Result<&mut Self> {
        self.handlers
            .lock()
            .expect("Could not add the callback to the list of consumers")
            .remove(address.as_str());
        write_msg(
            &self.socket,
            &Message::Unregister(RegisterMessage { address }),
        )
        .map(|_| self)
    }
}

fn reader_loop(read_stream: TcpStream, handlers: MessageHandlersByAddress) {
    let mut socket = buffered_reader::Generic::new(&read_stream, Some(4096));
    loop {
        // first, read the 4 bytes indicating message length: `len`
        if let Ok(len) = socket.read_be_u32() {
            // then consume `len` bytes of data => it's a whole message
            if let Ok(read) = socket.data_consume(len as usize) {
                // event bus protocol is JSON encoded
                let json = std::str::from_utf8(&read[..len as usize]).unwrap();
                println!("JSON received {}", json);
                match serde_json::from_str::<Message>(&json) {
                    Ok(msg) => {
                        if let Some(address) = msg.address() {
                            if let Some(handler) = handlers
                                .lock()
                                .expect("Could not retrieve message handler for this address")
                                .get(address.as_str())
                            {
                                handler
                                    .send(msg)
                                    .expect("Could not notify a new message has been received");
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
