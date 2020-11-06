use crate::message::UserMessage;
use std::task::Context;
use tokio::macros::support::{Pin, Poll};
use tokio::stream::Stream;
use tokio::sync::mpsc::Receiver;

pub struct MessageConsumer<T> {
    pub(crate) msg_queue: Receiver<UserMessage<T>>,
}

impl<T> Stream for MessageConsumer<T> {
    type Item = UserMessage<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.msg_queue.poll_recv(cx)
    }
}
