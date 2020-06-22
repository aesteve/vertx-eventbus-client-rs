use crate::message::{FullMessage, OutMessage, SendMessage};
use crate::utils::write_msg;
use std::io;
use std::net::TcpStream;

pub struct EventBusPublisher {
    socket: TcpStream,
}

impl EventBusPublisher {
    pub(crate) fn new(socket: TcpStream) -> Self {
        let mut created = EventBusPublisher { socket };
        created.ping().unwrap();
        created
    }

    pub fn send(&mut self, msg: SendMessage) -> io::Result<&mut Self> {
        write_msg(&self.socket, &OutMessage::Send(msg)).map(|_| self)
    }

    pub fn publish(&mut self, msg: FullMessage) -> io::Result<&mut Self> {
        write_msg(&self.socket, &OutMessage::Publish(msg)).map(|_| self)
    }

    pub fn ping(&mut self) -> io::Result<&mut Self> {
        write_msg(&self.socket, &OutMessage::Ping).map(|_| self)
    }
}
