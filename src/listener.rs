use crate::message::{
    ErrorMessage, InMessage, Message, MessageConsumer, OutMessage, RegisterMessage, UserMessage,
};
use crate::utils::write_msg;
use buffered_reader::BufferedReader;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};
use std::{io, thread};

type MessageHandlersByAddress = Arc<Mutex<HashMap<String, Sender<UserMessage<Message>>>>>;
type ErrorNotifier = Mutex<Option<Sender<UserMessage<ErrorMessage>>>>;

pub struct EventBusListener {
    socket: TcpStream,
    handlers: MessageHandlersByAddress,
    error_handler: Arc<ErrorNotifier>,
}

impl EventBusListener {
    pub(crate) fn new(socket: TcpStream) -> io::Result<Self> {
        let msg_dispatcher = socket.try_clone()?;
        let error_notifier = Arc::new(Mutex::new(None));
        let notifier = error_notifier.clone();
        let listener = EventBusListener {
            socket,
            handlers: Arc::new(Mutex::new(HashMap::new())),
            error_handler: error_notifier,
        };
        let consumers = listener.handlers.clone();

        thread::spawn(move || {
            reader_loop(msg_dispatcher, consumers, notifier.as_ref());
        });
        Ok(listener)
    }

    pub fn consumer(&mut self, address: String) -> io::Result<MessageConsumer<Message>> {
        let (tx, rx) = channel::<UserMessage<Message>>();
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

    pub fn errors(&mut self) -> io::Result<MessageConsumer<ErrorMessage>> {
        let (errors_notifier, errors_receiver) = channel::<UserMessage<ErrorMessage>>();
        self.error_handler
            .lock()
            .expect("Could not replace the value of the error notifier")
            .replace(errors_notifier);
        Ok(MessageConsumer {
            msg_queue: errors_receiver,
        })
    }
}

fn reader_loop(
    read_stream: TcpStream,
    handlers: MessageHandlersByAddress,
    error_notifier: &ErrorNotifier,
) {
    let mut socket = buffered_reader::Generic::new(&read_stream, Some(4096));
    loop {
        // first, read the 4 bytes indicating message length into `len`
        match socket.read_be_u32() {
            Ok(len) =>
            // then consume `len` bytes of data => it's a whole message
            if let Ok(bytes_read) = socket.data_consume(len as usize) {
                forward_json(&bytes_read[..len as usize], &handlers, error_notifier);
            },
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => {}, // transient failure, not to be propagated to the end-user
                kind =>
                for (_, handler) in handlers.lock().expect("Could retrieve message handlers to notify of an I/O error").iter() {
                    if handler.send(Err(kind)).is_err() {}
                }
            },
        }
    }
}

fn forward_json(
    bytes_read: &[u8],
    handlers: &MessageHandlersByAddress,
    error_notifier: &ErrorNotifier,
) {
    // event bus protocol is JSON encoded
    if let Ok(json) = std::str::from_utf8(bytes_read) {
        match serde_json::from_str::<InMessage>(&json) {
            Ok(in_msg) => {
                if let InMessage::Message(msg) = in_msg {
                    if let Some(handler) = handlers
                        .lock()
                        .expect("Could not retrieve message handler for this address")
                        .get(msg.address.as_str())
                    {
                        handler
                            .send(Ok(msg))
                            .expect("Could not notify a new message has been received");
                    }
                } else if let InMessage::Err(err) = in_msg {
                    let notifier = error_notifier
                        .lock()
                        .expect("Could not acquire error notifier to propagate an error message");
                    if notifier.is_some() {
                        notifier
                            .as_ref()
                            .unwrap()
                            .send(Ok(err))
                            .expect("Could not notify of an incoming error message");
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
    }
}
