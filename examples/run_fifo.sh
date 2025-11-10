#!/bin/sh

set -o errexit

# Parse command
COMMAND=${1:-}

if [ -z "$COMMAND" ]; then
    echo "Usage: $0 {run|kill}"
    echo "  run  - Build and start the fifo cluster (1 producer, 2 consumers)"
    echo "  kill - Stop all running fifo servers"
    exit 1
fi

# Default to 'info' level logging, but allow override via environment variable
export RUST_LOG=${RUST_LOG:-info}
export RUST_BACKTRACE=${RUST_BACKTRACE:-1}

kill_servers() {
    echo "Killing all running fifo servers..."
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='fifo'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall fifo
        set -e
    fi
    echo "Servers stopped"
}

run_servers() {
    echo "Building fifo example..."
    cargo build --example fifo

    echo "Killing any existing fifo servers..."
    kill_servers

    sleep 1

    echo "Starting 3 fifo servers (1 producer, 2 consumers)..."
    echo ""

    # Start servers in background, writing to both log files and stdout with prefix
    ./target/debug/examples/fifo cluster --tcp-port 22001 --node-id 1 --role producer 2>&1 | sed -u 's/^/[Server 1] /' | tee fifo1.log &
    sleep 1
    echo "Server 1 started on TCP port 22001 (producer)"

    ./target/debug/examples/fifo cluster --tcp-port 22002 --node-id 2 --role consumer 2>&1 | sed -u 's/^/[Server 2] /' | tee fifo2.log &
    sleep 1
    echo "Server 2 started on TCP port 22002 (consumer)"

    ./target/debug/examples/fifo cluster --tcp-port 22003 --node-id 3 --role consumer 2>&1 | sed -u 's/^/[Server 3] /' | tee fifo3.log &
    sleep 1
    echo "Server 3 started on TCP port 22003 (consumer)"

    echo ""
    echo "Waiting for cluster to initialize..."
    sleep 3

    echo ""
    echo "Cluster ready!"
    echo "All servers running. Press Ctrl+C to stop."
    echo "Logs are being written to fifo1.log, fifo2.log, and fifo3.log"
    echo ""
    echo "Server 1 (producer) will enqueue items to 'my_queue'"
    echo "Servers 2 and 3 (consumers) will dequeue items from 'my_queue'"
    echo ""

    # Wait for all background jobs
    wait
}

# Set up trap to kill servers on exit (Ctrl+C or script termination)
trap 'echo ""; echo "Caught signal, stopping servers..."; kill_servers; exit 0' INT TERM

case "$COMMAND" in
    run)
        run_servers
        ;;
    kill)
        kill_servers
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Usage: $0 {run|kill}"
        exit 1
        ;;
esac
