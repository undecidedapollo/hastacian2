#!/bin/sh

set -o errexit

# Parse command
COMMAND=${1:-}

if [ -z "$COMMAND" ]; then
    echo "Usage: $0 {run|kill|test}"
    echo "  run  - Build and start the shorturl cluster (keeps servers running)"
    echo "  kill - Stop all running shorturl servers"
    echo "  test - Run interactive tests against the cluster"
    exit 1
fi

# Default to 'info' level logging, but allow override via environment variable
export RUST_LOG=${RUST_LOG:-info}
export RUST_BACKTRACE=${RUST_BACKTRACE:-1}

kill_servers() {
    echo "Killing all running distacean servers..."
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='shorturl'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall shorturl
        set -e
    fi
    echo "Servers stopped"
}

run_servers() {
    echo "Building shorturl example..."
    cargo build --example shorturl

    echo "Killing any existing shorturl servers..."
    kill_servers

    sleep 1

    echo "Starting 3 shorturl servers..."
    echo ""

    # Start servers in background, writing to both log files and stdout with prefix
    ./target/debug/examples/shorturl cluster --tcp-port 22001 --node-id 1 --http-port 8001 2>&1 | sed -u 's/^/[Server 1] /' | tee shorturl1.log &
    sleep 1
    echo "Server 1 started on HTTP port 8001 (TCP 22001)"

    ./target/debug/examples/shorturl cluster --tcp-port 22002 --node-id 2 --http-port 8002 2>&1 | sed -u 's/^/[Server 2] /' | tee shorturl2.log &
    sleep 1
    echo "Server 2 started on HTTP port 8002 (TCP 22002)"

    ./target/debug/examples/shorturl cluster --tcp-port 22003 --node-id 3 --http-port 8003 2>&1 | sed -u 's/^/[Server 3] /' | tee shorturl3.log &
    sleep 1
    echo "Server 3 started on HTTP port 8003 (TCP 22003)"

    echo ""
    echo "Waiting for cluster to initialize..."
    sleep 3

    echo ""
    echo "Cluster ready!"
    echo "All servers running. Press Ctrl+C to stop."
    echo "Logs are being written to shorturl1.log, shorturl2.log, and shorturl3.log"
    echo ""
    echo "Example usage:"
    echo "  # Shorten a URL on server 1"
    echo "  curl -X POST http://localhost:8001/shorten -H \"Content-Type: application/json\" -d '\"https://example.com\"'"
    echo ""
    echo "  # Retrieve shortened URL (on any server)"
    echo "  curl -vv http://localhost:8002/l/{hash}"
    echo ""

    # Wait for all background jobs
    wait
}

test_cluster() {
    echo "Testing shorturl cluster..."
    echo ""

    echo "Creating shortened URL on server 1..."
    RESULT1=$(curl -s -X POST http://localhost:8001/shorten -H "Content-Type: application/json" -d '"https://test1.com"')
    echo "Result: $RESULT1"
    HASH1=$(echo "$RESULT1" | grep -o '[^/]*$')
    echo "Hash: $HASH1"
    echo ""

    echo "Creating shortened URL on server 2..."
    RESULT2=$(curl -s -X POST http://localhost:8002/shorten -H "Content-Type: application/json" -d '"https://test2.com"')
    echo "Result: $RESULT2"
    HASH2=$(echo "$RESULT2" | grep -o '[^/]*$')
    echo "Hash: $HASH2"
    echo ""

    echo "Creating shortened URL on server 3..."
    RESULT3=$(curl -s -X POST http://localhost:8003/shorten -H "Content-Type: application/json" -d '"https://test3.com"')
    echo "Result: $RESULT3"
    HASH3=$(echo "$RESULT3" | grep -o '[^/]*$')
    echo "Hash: $HASH3"
    echo ""

    sleep 1

    echo "Retrieving URLs from different servers to test replication..."
    echo ""

    echo "Reading hash1 from server 1:"
    curl -vv "http://localhost:8001/l/$HASH1" 2>&1 | grep -E '(< HTTP|< location)'
    echo ""

    echo "Reading hash2 from server 2:"
    curl -vv "http://localhost:8002/l/$HASH2" 2>&1 | grep -E '(< HTTP|< location)'
    echo ""

    echo "Reading hash3 from server 3:"
    curl -vv "http://localhost:8003/l/$HASH3" 2>&1 | grep -E '(< HTTP|< location)'
    echo ""

    echo "Cross-server read test (reading hash1 from server 3):"
    curl -vv "http://localhost:8003/l/$HASH1" 2>&1 | grep -E '(< HTTP|< location)'
    echo ""

    echo "Tests completed!"
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
    test)
        test_cluster
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Usage: $0 {run|kill|test}"
        exit 1
        ;;
esac
