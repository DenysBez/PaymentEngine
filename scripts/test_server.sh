#!/bin/bash

# Enable logging
export RUST_LOG=info

echo "Starting payment server on port 9999..."
cargo run --bin payments_server 127.0.0.1:9999 &
SERVER_PID=$!

echo "Waiting for server to start..."
sleep 3

echo ""
echo "Sending test transactions..."
echo "type,client,tx,amount
deposit,1,1,100.0
deposit,2,1,200.0
withdrawal,1,2,50.0
dispute,1,1," | nc -q 1 127.0.0.1 9999

echo ""
echo "Stopping server..."
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

echo "Test complete!"
