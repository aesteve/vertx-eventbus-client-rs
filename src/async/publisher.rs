use crate::message::{Message, OutMessage, SendMessage};
use crate::utils::write_msg_async;
use tokio::io;
use tokio::net::TcpStream;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use tokio::time::Duration;

pub struct EventBusPublisher {
    pub(crate) sender: Sender<OutMessage>,
}

impl EventBusPublisher {
    pub async fn new(address: String) -> io::Result<Self> {
        let stream = TcpStream::connect(&address).await?;
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<OutMessage>(128);
        tokio::task::spawn(async move {
            let (_, mut writer) = stream.into_split();
            let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    msg = receiver.recv() => {
                        if let Some(out_msg) = msg {
                            println!("sending a message through tcp {:?}", out_msg);
                            match write_msg_async(&mut writer, &out_msg).await {
                                Ok(_) => println!("after sending msg {:?}", out_msg),
                                Err(e) => println!("could not send msg {:?}", e)
                                }

                        } else { // dropped
                            println!("Dropped");
                            break;
                        }
                    },
                    _ = heartbeat_interval.tick() => {
                        println!("sending heartbeat");
                        match write_msg_async(&mut writer, &OutMessage::Ping).await {
                            Ok(_) => println!("after sending heartbeat"),
                            Err(e) => println!("could not send heartbeat {:?}", e)
                        };

                    }
                };
            }
        });
        Ok(EventBusPublisher { sender })
    }

    pub async fn ping(&mut self) -> Result<(), SendError<OutMessage>> {
        self.sender.send(OutMessage::Ping).await
    }

    pub async fn send(&mut self, msg: SendMessage) -> Result<(), SendError<OutMessage>> {
        self.sender.send(OutMessage::Send(msg)).await
    }

    pub async fn publish(&mut self, msg: Message) -> Result<(), SendError<OutMessage>> {
        self.sender.send(OutMessage::Publish(msg)).await
    }
}
