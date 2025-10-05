#!/bin/bash

# Enable logging
export RUST_LOG=info

echo "=== Testing Concurrent TCP Connections (Verbose) ==="
echo ""
echo "This test shows all server logs for debugging"
echo ""

# Kill any existing servers on port 9999
pkill -f "payments_server 127.0.0.1:9999" 2>/dev/null || true
sleep 1

# Create log file
SERVER_LOG="/tmp/payment_server_$$.log"

# Start server in background with logs to file
echo "Starting payment server (logging to $SERVER_LOG)..."
echo "----------------------------------------"

cargo run --release --bin payments_server 127.0.0.1:9999 > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
sleep 5

# Check if server started successfully
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo "ERROR: Server failed to start. Log contents:"
    cat "$SERVER_LOG"
    rm -f /tmp/client_*.out
    exit 1
fi

echo ""
echo "----------------------------------------"
echo "Sending 100 concurrent connections..."
echo "----------------------------------------"
echo ""

# Send 100 concurrent connections
CLIENT_PIDS=()
for i in {1..100}; do
  (
    echo "type,client,tx,amount
deposit,$i,1,100.0
withdrawal,$i,2,25.0" | nc -q 1 127.0.0.1 9999 > /tmp/client_$i.out 2>&1
  ) &
  CLIENT_PIDS+=($!)
done

# Wait for all client connections to complete
for pid in "${CLIENT_PIDS[@]}"; do
  wait $pid 2>/dev/null
done

# Give server time to log all messages
sleep 5

echo ""
echo "----------------------------------------"
echo "TEST RESULTS"
echo "----------------------------------------"

# Count successful responses
SUCCESS_COUNT=$(grep -l "client,available" /tmp/client_*.out 2>/dev/null | wc -l)

echo "  Total connections sent: 100"
echo "  Successful responses: $SUCCESS_COUNT"

# Show sample output
if [ -f /tmp/client_1.out ]; then
  echo ""
  echo "Sample output from client 1:"
  echo "----------------------------"
  cat /tmp/client_1.out
fi

echo ""
echo "----------------------------------------"
echo "SERVER LOG SUMMARY"
echo "----------------------------------------"

# Count connection logs
ACCEPTED=$(grep -c "Connection accepted" "$SERVER_LOG" 2>/dev/null || echo 0)
CLOSED=$(grep -c "Connection closed" "$SERVER_LOG" 2>/dev/null || echo 0)
PROCESSED=$(grep -c "Processed" "$SERVER_LOG" 2>/dev/null || echo 0)

echo "  Connections accepted: $ACCEPTED"
echo "  Connections closed: $CLOSED"
echo "  Transactions processed: $PROCESSED"

echo ""
echo "----------------------------------------"
echo "FULL SERVER LOG"
echo "----------------------------------------"
cat "$SERVER_LOG"

# Cleanup
rm -f /tmp/client_*.out

echo ""
echo "=== Test Complete ==="
