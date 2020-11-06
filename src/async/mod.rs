mod listener;
mod message;
mod publisher;
use crate::r#async::{listener::EventBusListener, publisher::EventBusPublisher};
use std::io;

pub async fn eventbus(address: String) -> io::Result<(EventBusPublisher, EventBusListener)> {
    let write_addr = address.clone();
    let read_addr = address.clone();
    let publisher = EventBusPublisher::new(write_addr).await?;
    let sender = publisher.sender.clone();
    Ok((publisher, EventBusListener::new(read_addr, sender).await?))
}

#[cfg(test)]
#[cfg(feature = "async")]
mod tests {
    use crate::message::Message;
    use crate::r#async::eventbus;
    use crate::tests::mock_eventbus_server;
    use testcontainers::*;
    use tokio::stream::StreamExt;

    #[tokio::test]
    async fn test_ping_async() {
        let docker = clients::Cli::default();
        let node = docker.run(mock_eventbus_server());
        let host_port = node
            .get_host_port(7542)
            .expect("Mock event bus server implementation needs to be up before running tests");
        let addr = format!("localhost:{}", host_port);
        println!("Mock server (async) running on {}", addr);
        let (mut publisher, _) = eventbus(addr).await.expect("Can create an async eventbus");
        publisher
            .ping()
            .await
            .expect("Should be able to send ping to the server");
    }

    #[tokio::test]
    async fn consumer_test_async() {
        let docker = clients::Cli::default();
        let node = docker.run(mock_eventbus_server());
        let host_port = node
            .get_host_port(7542)
            .expect("Mock event bus server implementation needs to be up before running tests");
        let addr = format!("localhost:{}", host_port);
        println!("Mock server (async) running on {}", addr);
        let (_, mut listener) = eventbus(addr)
            .await
            .expect("Event bus creation must not fail");
        println!("Creating consumer");
        let mut consumer = listener.consumer("out-address".to_string()).await.unwrap();
        println!("Consumer created");
        let mut received_msgs = Vec::new();
        while received_msgs.len() < 3 {
            println!("next");
            if let Some(Ok(msg)) = consumer.next().await {
                assert!(received_msgs
                    .iter()
                    .find(|m: &&Message| m.body == msg.body)
                    .is_none()); // same message has not been received twice
                received_msgs.push(msg);
            }
        }
        listener
            .unregister_consumer("out-address".to_string())
            .await
            .expect("Unregistering consumer must not fail");
    }
}
