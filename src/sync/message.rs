use crate::message::UserMessage;
use std::sync::mpsc::Receiver;

pub struct MessageConsumer<T> {
    pub(crate) msg_queue: Receiver<UserMessage<T>>,
}

impl<T> Iterator for MessageConsumer<T> {
    type Item = UserMessage<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.msg_queue.try_recv().ok()
    }
}
