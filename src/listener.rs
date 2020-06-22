use crate::error::Error;
use crate::message::{InMessage, MessageConsumer, OutMessage, RegisterMessage};
use crate::utils::write_msg;
use buffered_reader::BufferedReader;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::{io, thread};

type MessageHandlersByAddress = Arc<Mutex<HashMap<String, Sender<Result<InMessage, Error>>>>>;

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
        let (tx, rx) = channel::<Result<InMessage, Error>>();
        let handler = MessageConsumer { msg_queue: rx };
        self.handlers
            .lock()
            .expect("Could not add the callback to the list of consumers")
            .insert(address.clone(), tx);
        write_msg(
            &self.socket,
            &OutMessage::Register(RegisterMessage { address }),
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
            &OutMessage::Unregister(RegisterMessage { address }),
        )
        .map(|_| self)
    }
}

fn reader_loop(read_stream: TcpStream, handlers: MessageHandlersByAddress) {
    let mut socket = buffered_reader::Generic::new(&read_stream, Some(4096));
    loop {
        // first, read the 4 bytes indicating message length: `len`
        match socket.read_be_u32() {
            Ok(len) =>
            // then consume `len` bytes of data => it's a whole message
            if let Ok(read) = socket.data_consume(len as usize) {
                // event bus protocol is JSON encoded
                let json = std::str::from_utf8(&read[..len as usize]).unwrap();
                match serde_json::from_str::<InMessage>(&json) {
                    Ok(msg) => {
                        if let Some(address) = msg.address() {
                            if let Some(handler) = handlers
                                .lock()
                                .expect("Could not retrieve message handler for this address")
                                .get(address.as_str())
                            {
                                handler
                                    .send(Ok(msg))
                                    .expect("Could not notify a new message has been received");
                            }
                        }
                    }
                    Err(err) => {
                        println!(
                            "Invalid JSON received from EventBus: {}. Error: {:?}",
                            json, err
                        );
                    }
                }
            },
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => {}, // transient failure, not to be propagated to the end-user
                _ =>
                for (_, handler) in handlers.lock().expect("Could retrieve message handlers").iter() {
                    if handler.send(Err(Error::Io)).is_err() {
                        println!("WARN: could not notify message handlers of an IO error {:?}", e)
                    }
                }
            },
        }
    }
}
