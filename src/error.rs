use crate::message::ErrorMessage;
// use std::io;

pub enum Error {
    Io,
    Msg(ErrorMessage),
}
