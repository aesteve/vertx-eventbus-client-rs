#[cfg(feature = "async")]
pub mod r#async;
mod message;
pub mod sync;
mod utils;

#[cfg(test)]
mod tests {
    use testcontainers::images::generic::{GenericImage, WaitFor};

    pub(crate) fn mock_eventbus_server() -> GenericImage {
        GenericImage::new("aesteve/tests:mock-eventbus-server")
            .with_wait_for(WaitFor::message_on_stdout("TCP bridge connected"))
    }
}
