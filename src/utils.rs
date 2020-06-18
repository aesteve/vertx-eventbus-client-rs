use crate::message::Message;
use byteorder::{BigEndian, ByteOrder};
use std::io;
use std::io::Write;
use std::net::TcpStream;

pub(crate) fn write_msg(mut socket: &TcpStream, msg: &Message) -> io::Result<()> {
    let mut buf = [0u8; 4];
    let msg_str = serde_json::to_string(msg).unwrap();
    BigEndian::write_u32(&mut buf, msg_str.len() as u32);
    socket.write_all(&buf).unwrap(); // write message length as Big Endian
    socket.write_all(&msg_str.as_bytes()).unwrap(); // write message
    socket.flush()
}
