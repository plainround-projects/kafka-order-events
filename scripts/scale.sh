#!/usr/bin/env bash
# Run N consumers in the group, then describe the group: who owns which partitions.
#   scripts/scale.sh 3      three consumers, two partitions each
#   scripts/scale.sh 8      eight consumers on six partitions: two sit idle
set -euo pipefail
count="${1:-3}"
group="${GROUP_ID:-order-history}"
python="${PYTHON_BIN:-.venv/bin/python}"
mkdir -p logs
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true' EXIT

for number in $(seq 1 "$count"); do
  "$python" -m order_events.interfaces.consume_cli --name "consumer-$number" > "logs/consumer-$number.log" 2>&1 &
  pids+=("$!")
  sleep 2   # one join at a time, so each is its own rebalance
done
sleep 8     # let the last rebalance settle before reading the assignment

kafka_groups=(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092)
echo "== $count consumers: members and their partitions"
"${kafka_groups[@]}" --describe --group "$group" --members --verbose
echo
echo "== $count consumers: partitions, owner and lag"
"${kafka_groups[@]}" --describe --group "$group"
