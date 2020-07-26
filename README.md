[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Crates.io Version](https://img.shields.io/crates/v/vertx-eventbus-bridge.svg)](https://crates.io/crates/vertx-eventbus-bridge)
[![codecov](https://codecov.io/gh/aesteve/vertx-eventbus-client-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/aesteve/vertx-eventbus-client-rs)



## Vert.x TCP EventBus client for Rust


Vert.x allows to expose its event-bus through a [TCP socket](https://vertx.io/docs/vertx-tcp-eventbus-bridge/java/).
This crate offers primitives in the Rust programming language to interact with the event bus over this TCP connection. 

This allows to send messages from a Rust codebase (embedded sensors for instance) to a running Vert.x instance, or reacting to event bus messages published from the Vert.x application in a Rust codebase.


## Early stage

This project is still in early development phase, although it's available as a [crate](https://crates.io/crates/vertx-eventbus-bridge) if you need it. 
The design (at the moment: iterators over incoming messages) is highly suggest to change.

Any proposition or technical comment is highly welcomed in the issues.

## Testing

In order to test the client against a "real" TCP Event-Bus bridge (and avoid observator-bias: testing my own understanding of the protocol), [this docker image](https://hub.docker.com/layers/aesteve/tests/mock-eventbus-server/images/sha256-90eff1e74362118b41ec7fc7a22ecd180a6c2c07206083dec9885d6b61edd24c?context=repo) is used, through testcontainers.
The code for this image can be found [here](https://github.com/aesteve/vertx-eventbus-mock-server), it's a simple Vert.x application listening/publishing to the Event-Bus and exposing it through TCP on port 7542.
