#!/bin/sh

set -o errexit

cargo build

kill() {
    if [ "$(uname)" = "Darwin" ]; then
        SERVICE='distacean'
        if pgrep -xq -- "${SERVICE}"; then
            pkill -f "${SERVICE}"
        fi
    else
        set +e # killall will error if finds no process to kill
        killall distacean
        set -e
    fi
}

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
        # JQ might fail if the output is not valid JSON, resort to cat in that case
            jq -R '. as $line | try (fromjson) catch $line'
        else
            cat
        fi
    }

    echo
    echo
}

export RUST_LOG=trace
export RUST_BACKTRACE=full

echo "Killing all running distacean"

kill

sleep 1

echo "Start 3 uninitialized distacean servers..."

nohup ./target/debug/distacean  --id 1 --http-addr 127.0.0.1:21001 > n1.log &
sleep 1
echo "Server 1 started"

nohup ./target/debug/distacean  --id 2 --http-addr 127.0.0.1:21002 > n2.log &
sleep 1
echo "Server 2 started"

nohup ./target/debug/distacean  --id 3 --http-addr 127.0.0.1:21003 > n3.log &
sleep 1
echo "Server 3 started"
sleep 1

echo "Initialize servers 1,2,3 as a 3-nodes cluster"
sleep 2
echo

rpc 21001/init '[[1, "127.0.0.1:21001"], [2, "127.0.0.1:21002"], [3, "127.0.0.1:21003"]]'
# if you want to initialize server 1 as a single cluster, use `rpc 21001/init '[]'` or `rpc 21001/init '[[1, "127.0.0.1:21001"]]'`

echo "Server 1 is a leader now"

sleep 2

echo "Get metrics from the leader"
sleep 2
echo
rpc 21001/metrics
sleep 1


echo "Write data on leader"
sleep 1
echo
rpc 21001/write '{"Set":{"key":"foo","value":"bar"}}'
sleep 1
echo "Data written"
sleep 1

echo "Read on every node, including the leader"
sleep 1
echo "Read from node 1"
echo
rpc 21001/read  '"foo"'
echo "Read from node 2"
echo
rpc 21002/read  '"foo"'
echo "Read from node 3"
echo
rpc 21003/read  '"foo"'



echo "Deleting data on leader"
sleep 1
echo
rpc 21001/write '{"Del":{"key":"foo"}}'

echo "Read on leader after deletion"
sleep 1
echo
rpc 21001/read  '"foo"'

sleep 1
echo "Killing all nodes..."
kill
