## Setup

## Multi-node Cluster Examples

## Example: Short URL Service
This example demonstrates a simple URL shortening service using the Distacean's distributed key value store.

In one terminal, build & run the shorturl example:
```bash
./examples/run_shorturl.sh run
```

In another terminal, run the test:
```bash
./examples/run_shorturl.sh test
```

## Single Node Examples

### FIFO Queue
This example demonstrates a simple FIFO queue using Distacean's distributed key value store.
```bash
cargo run --example fifo
```

### Atomic Compare and Swap
This example demonstrates an atomic compare-and-swap operation using Distacean's distributed key value store.
```bash
cargo run --example cas
```