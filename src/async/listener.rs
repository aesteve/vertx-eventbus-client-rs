use crate::message::{ErrorMessage, InMessage, Message, OutMessage, RegisterMessage, UserMessage};
use crate::r#async::message::MessageConsumer;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio_util::codec::LengthDelimitedCodec;

type MessageHandlersByAddress = Arc<Mutex<HashMap<String, Sender<UserMessage<Message>>>>>;
type ErrorNotifier = Mutex<Option<Sender<UserMessage<ErrorMessage>>>>;

pub struct EventBusListener {
    tcp_sender: Sender<OutMessage>,
    handlers: MessageHandlersByAddress,
    error_handler: Arc<ErrorNotifier>,
}

impl EventBusListener {
    pub(crate) async fn new(address: String, sender: Sender<OutMessage>) -> io::Result<Self> {
        let stream = TcpStream::connect(&address).await?;
        let error_notifier = Arc::new(Mutex::new(None));
        let notifier = error_notifier.clone();
        let listener = EventBusListener {
            tcp_sender: sender,
            handlers: Arc::new(Mutex::new(HashMap::new())),
            error_handler: error_notifier,
        };
        let consumers = listener.handlers.clone();
        tokio::task::spawn(async move {
            let (reader, _) = stream.into_split();
            let mut frame_decoder = LengthDelimitedCodec::builder()
                .big_endian()
                .new_read(reader);
            loop {
                match frame_decoder.next().await {
                    Some(Ok(read)) => {
                        println!("Received msg over tcp");
                        forward_json(&read, &consumers, &notifier).await;
                    }
                    Some(Err(e)) => match e.kind() {
                        std::io::ErrorKind::WouldBlock => {} // transient failure, not to be propagated to the end-user
                        kind => {
                            println!("Received error over tcp");
                            for (_, handler) in consumers.lock().await.iter_mut() {
                                if handler.send(Err(kind)).await.is_err() {}
                            }
                        }
                    },
                    _ => {}
                }
            }
        });
        Ok(listener)
    }

    pub async fn consumer(
        &mut self,
        address: String,
    ) -> Result<MessageConsumer<Message>, SendError<OutMessage>> {
        let (tx, rx) = tokio::sync::mpsc::channel::<UserMessage<Message>>(128);
        let handler = MessageConsumer { msg_queue: rx };
        self.handlers.lock().await.insert(address.clone(), tx);
        println!("Added to the list of handlers");
        self.tcp_sender
            .send(OutMessage::Register(RegisterMessage { address }))
            .await?;
        println!("After sending msg to publisher");
        Ok(handler)
    }

    pub async fn unregister_consumer(
        &mut self,
        address: String,
    ) -> Result<&mut Self, SendError<OutMessage>> {
        self.handlers.lock().await.remove(address.as_str());
        self.tcp_sender
            .send(OutMessage::Unregister(RegisterMessage { address }))
            .await?;
        Ok(self)
    }

    pub async fn errors(&mut self) -> MessageConsumer<ErrorMessage> {
        let (errors_notifier, errors_receiver) =
            tokio::sync::mpsc::channel::<UserMessage<ErrorMessage>>(128);
        self.error_handler.lock().await.replace(errors_notifier);
        MessageConsumer {
            msg_queue: errors_receiver,
        }
    }
}

async fn forward_json(
    bytes_read: &[u8],
    handlers: &MessageHandlersByAddress,
    error_notifier: &ErrorNotifier,
) {
    // event bus protocol is JSON encoded
    if let Ok(json) = std::str::from_utf8(bytes_read) {
        match serde_json::from_str::<InMessage>(&json) {
            Ok(in_msg) => {
                if let InMessage::Message(msg) = in_msg {
                    if let Some(handler) = handlers.lock().await.get_mut(msg.address.as_str()) {
                        if handler.send(Ok(msg)).await.is_err() {};
                    }
                } else if let InMessage::Err(err) = in_msg {
                    let mut notifier = error_notifier.lock().await;
                    if notifier.is_some() && notifier.as_mut().unwrap().send(Ok(err)).await.is_err()
                    {
                    };
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
