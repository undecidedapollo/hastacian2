#!/bin/sh

set -o errexit

# Parse command
COMMAND=${1:-}

if [ -z "$COMMAND" ]; then
    echo "Usage: $0 {run|kill}"
    echo "  run  - Build and start the cluster (keeps servers running)"
    echo "  kill - Stop all running servers"
    exit 1
fi

# Default to 'info' level logging, but allow override via environment variable
export RUST_LOG=${RUST_LOG:-info}
export RUST_BACKTRACE=${RUST_BACKTRACE:-1}

rpc() {
    local uri=$1
    local body="$2"

    echo '---'" rpc(:$uri, $body)"

    {
        if [ ".$body" = "." ]; then
            time curl --silent "127.0.0.1:$uri"
        else
            time curl --silent "127.0.0.1:$uri" -H "Content-Type: application/json" -d "$body"
        fi
    } | {
        if type jq > /dev/null 2>&1; then
            jq
        else
            cat
        fi
    }

    echo
    echo
}

kill_servers() {
    echo "Killing all running distacian servers..."
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='distacian'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall distacian
        set -e
    fi
    echo "Servers stopped"
}

run_servers() {
    echo "Building distacian..."
    cargo build

    echo "Killing any existing distacian servers..."
    kill_servers

    sleep 1

    echo "Starting 3 distacian servers..."
    echo ""

    # Start servers in background, writing to both log files and stdout
    ./target/debug/distacian --id 1 --http-addr 127.0.0.1:21001 2>&1 | tee n1.log &
    sleep 1
    echo "Server 1 started on port 21001"

    ./target/debug/distacian --id 2 --http-addr 127.0.0.1:21002 2>&1 | tee n2.log &
    sleep 1
    echo "Server 2 started on port 21002"

    ./target/debug/distacian --id 3 --http-addr 127.0.0.1:21003 2>&1 | tee n3.log &
    sleep 1
    echo "Server 3 started on port 21003"

    echo ""
    echo "Initializing servers 1,2,3 as a 3-node cluster..."
    sleep 2
    echo ""

    rpc 21001/init '[[1, "127.0.0.1:21001"], [2, "127.0.0.1:21002"], [3, "127.0.0.1:21003"]]'

    echo ""
    echo "Cluster initialized! Server 1 is now the leader."
    echo "All servers running. Press Ctrl+C to stop."
    echo "Logs are being written to n1.log, n2.log, and n3.log"
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
