#!/usr/bin/env bash
# Build a backlog, then drain it with one slow consumer and time the recovery.
#   LAG_ORDERS=16667 scripts/lag.sh     16,667 orders x 3 events = 50,001 events (the lesson's 50,000)
set -euo pipefail
orders="${LAG_ORDERS:-16667}"
delay="${LAG_DELAY:-0.01}"
group="${GROUP_ID:-order-history}"
python="${PYTHON_BIN:-.venv/bin/python}"
mkdir -p logs

kafka_groups=(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092)
total_lag() { "${kafka_groups[@]}" --describe --group "$group" 2>/dev/null | awk '$1 != "GROUP" && $6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }'; }

echo "== 1. bring the group to the end of the log, then stop every consumer"
"$python" -m order_events.interfaces.consume_cli --until-idle 5 --name catch-up
echo "== 2. produce $orders orders with no consumer running"
"$python" -m order_events.interfaces.produce_cli --orders "$orders" --prefix "backlog-$(date +%s)" --max-delay 0
echo "== 3. the backlog is the lag, per partition"
"${kafka_groups[@]}" --describe --group "$group"

echo "== 4. one consumer with ${delay}s of work per event; total lag every 10s"
"$python" -m order_events.interfaces.consume_cli --name slow-consumer --delay "$delay" > logs/slow-consumer.log 2>&1 &
consumer_pid=$!
trap 'kill "$consumer_pid" 2>/dev/null || true; wait 2>/dev/null || true' EXIT
started=$(date +%s)
while true; do
  sleep 10
  lag="$(total_lag)"
  echo "  +$(( $(date +%s) - started ))s  total lag: $lag"
  [ "$lag" -eq 0 ] && break
done
echo "== recovered: lag reached 0 after about $(( $(date +%s) - started ))s"
