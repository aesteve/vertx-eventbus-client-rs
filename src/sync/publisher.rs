use crate::message::{Message, OutMessage, SendMessage};
use crate::utils::write_msg;
use std::net::TcpStream;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;
use std::{io, thread};

pub struct EventBusPublisher {
    socket: TcpStream,
    tx: Sender<()>,
}

impl EventBusPublisher {
    pub(crate) fn new(socket: TcpStream) -> io::Result<Self> {
        let (tx, rx) = channel::<()>();
        let mut created = EventBusPublisher { socket, tx };
        created.send_heartbeat_periodically(rx)?;
        Ok(created)
    }

    pub fn send(&mut self, msg: SendMessage) -> io::Result<&mut Self> {
        write_msg(&self.socket, &OutMessage::Send(msg)).map(|_| self)
    }

    pub fn publish(&mut self, msg: Message) -> io::Result<&mut Self> {
        write_msg(&self.socket, &OutMessage::Publish(msg)).map(|_| self)
    }

    pub fn ping(&mut self) -> io::Result<&mut Self> {
        write_msg(&self.socket, &OutMessage::Ping).map(|_| self)
    }

    fn send_heartbeat_periodically(&mut self, rx: Receiver<()>) -> io::Result<()> {
        let heartbeat_socket = self.socket.try_clone()?;
        thread::spawn(move || loop {
            if write_msg(&heartbeat_socket, &OutMessage::Ping).is_err() {
                println!("Could not send periodic heartbeat to TCP server")
            }
            if rx.try_recv().ok().is_none() {
                thread::sleep(Duration::from_secs(10));
            } else {
                break;
            }
        });
        Ok(())
    }
}

impl Drop for EventBusPublisher {
    fn drop(&mut self) {
        if self.tx.send(()).is_err() {}
    }
}
