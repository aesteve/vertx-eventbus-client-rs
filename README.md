## Vert.x TCP EventBus client for Rust


Vert.x allows to expose its event-bus through a [TCP socket](https://vertx.io/docs/vertx-tcp-eventbus-bridge/java/).
This crate offers primitives in the Rust programming language to interact with the event bus over this TCP connection. 

This allows to send messages from a Rust codebase (embedded sensors for instance) to a running Vert.x instance, or reacting to event bus messages published from the Vert.x application in a Rust codebase.


## WIP

This project is in very early development phase, and is not (yet) published as a crate.
The design (at the moment: iterators over incoming messages) is highly suggest to change.

Any proposition or technical comment is highly welcomed in the issues. 