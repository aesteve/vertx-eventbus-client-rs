use crate::message::OutMessage;
use byteorder::{BigEndian, ByteOrder};
use std::io;
use std::io::Write;
use std::net::TcpStream;

pub(crate) fn write_msg(mut socket: &TcpStream, msg: &OutMessage) -> io::Result<()> {
    let mut buf = [0u8; 4];
    let msg_str = serde_json::to_string(msg)?;
    BigEndian::write_u32(&mut buf, msg_str.len() as u32);
    buf.to_vec().extend(msg_str.as_bytes());
    let a = [&buf[..], &msg_str.as_bytes()[..]].concat();
    socket.write_all(&a)?; // write message length as Big Endian then msg
    socket.flush()
}
