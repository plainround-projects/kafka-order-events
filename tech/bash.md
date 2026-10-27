# Bash (shell scripts)

## What it is

A **shell** is a program that reads the commands you type in a terminal and runs them. **Bash** is the most common shell on Linux and was the default on macOS for years. A **shell script** is a text file of such commands, run top to bottom, so a long sequence can be repeated with one name. Two small helper programs appear with it: `awk` (reads lines of text and picks out columns) and `grep`/`sed` (find or change text).

## Why this repository uses it

Two demos need several programs running at once and a clock: `scripts/scale.sh` starts N consumers and then asks Kafka to describe the group, and `scripts/lag.sh` builds a backlog and times how long one slow consumer needs to clear it. Doing that in Python would hide the point; in Bash each step is one visible command. Short shell lines also appear inside the `Makefile`.

## Where it appears

| Path | What it does with Bash |
| --- | --- |
| `scripts/scale.sh` | Starts N consumer processes in the background, waits, describes the group, cleans up. |
| `scripts/lag.sh` | Catches the group up, produces a backlog, starts a slow consumer, prints the total lag every ten seconds. |
| `Makefile` (`run-unkeyed`, `order`, `help`) | One-line shell snippets inside recipes. |
| `.github/workflows/ci.yml` | Each `run:` line is run by Bash on the CI machine. |

## How it works underneath

When you run `scripts/scale.sh 8`, the operating system starts a new **process** (a running program) and Bash reads the file line by line. For a normal command Bash starts another process, waits until it ends, and records its **exit code** in the variable `$?`: `0` means success, anything else means failure. A trailing `&` starts the command in the **background**: Bash does not wait, and `$!` holds the new process's id (PID). `|` (a **pipe**) connects the output of one command to the input of the next. `>` writes output to a file. Each process has two output streams: **standard output** (number 1) and **standard error** (number 2); `2>&1` sends the second into the first.

## Walkthrough of this repository's use

### Safety switches and defaults in `scale.sh`

```bash
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
```

- `#!/usr/bin/env bash` (the **shebang**) tells the system to run the file with Bash. The executable bit (`chmod +x`) lets you run it as `scripts/scale.sh`.
- `set -euo pipefail` turns on three safety rules. `-e`: stop the script when a command fails. `-u`: using a variable that was never set is an error (this catches typos). `-o pipefail`: a pipeline fails if *any* command in it fails, not only the last.
- `count="${1:-3}"`: `$1` is the first argument. `${1:-3}` means "`$1`, or `3` if it is missing or empty". `${GROUP_ID:-order-history}` does the same for an environment variable.
- `pids=()` makes an empty **array**. `pids+=("$!")` (later) appends the PID of the last background command.
- `trap '...' EXIT` registers a command to run when the script ends for any reason. Here it kills the consumers: `kill "${pids[@]}"` sends them SIGTERM (a polite "please stop"), `2>/dev/null` throws away error messages, `|| true` makes a failure of `kill` harmless under `set -e`, and `wait` waits for the children to finish.

### Starting the consumers

```bash
for number in $(seq 1 "$count"); do
  "$python" -m order_events.interfaces.consume_cli --name "consumer-$number" > "logs/consumer-$number.log" 2>&1 &
  pids+=("$!")
  sleep 2   # one join at a time, so each is its own rebalance
done
sleep 8     # let the last rebalance settle before reading the assignment
```

- `$(seq 1 "$count")` is **command substitution**: it runs `seq` (prints 1 to N) and puts its output in place. `for number in ...` loops over those numbers.
- `"$python" -m ... --name "consumer-$number" > "logs/consumer-$number.log" 2>&1 &` starts one consumer, sends everything it prints to its own log file, and puts it in the background. Quotes around `"$python"` keep a path with spaces in one piece.
- `sleep 2` waits two seconds between starts so each join is a separate rebalance. `sleep 8` after the loop lets the last rebalance finish before asking Kafka.

### Asking Kafka, with an array as a command

```bash
kafka_groups=(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092)
echo "== $count consumers: members and their partitions"
"${kafka_groups[@]}" --describe --group "$group" --members --verbose
echo
echo "== $count consumers: partitions, owner and lag"
"${kafka_groups[@]}" --describe --group "$group"
```

`kafka_groups=(docker compose exec -T kafka ...)` stores a whole command in an array so it can be reused. `"${kafka_groups[@]}"` expands to every element, each kept as one word. `-T` stops Docker from asking for a terminal (see `tech/docker-compose.md`).

### A function, a pipeline and `awk` in `lag.sh`

```bash
total_lag() { "${kafka_groups[@]}" --describe --group "$group" 2>/dev/null | awk '$1 != "GROUP" && $6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }'; }
```

`total_lag() { ...; }` defines a function. The pipeline runs the description, discards errors, and gives the text to `awk`. The `awk` program reads each line split into columns (`$1`, `$6`, ...). The condition `$1 != "GROUP" && $6 ~ /^[0-9]+$/` keeps lines whose first column is not the header word and whose sixth column (`LAG`) is all digits. `{ sum += $6 }` adds it up and `END { print sum + 0 }` prints the total once all lines are read (`+ 0` makes an empty sum print `0`).

### The wait loop

```bash
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
```

- `consumer_pid=$!` saves the PID of the slow consumer; the `trap` kills it on exit.
- `date +%s` prints the current time as seconds since 1970. `$(( ... ))` is **arithmetic**: `$(( $(date +%s) - started ))` is the elapsed seconds.
- `while true; do ... done` loops until `break`. `[ "$lag" -eq 0 ] && break` leaves the loop when lag equals zero (`-eq` compares numbers). The `&&` means "run the right side only if the left side succeeded".

### Shell inside the Makefile

```make
run-unkeyed: $(STAMP) ## the failure mode: the same events with no key
	@prefix=unkeyed-$$(date +%s); \
	$(PRODUCE) --orders 1000 --no-key --prefix $$prefix && \
	$(CONSUME) --until-idle 5 --group unkeyed-check-$$prefix --prefix $$prefix
```

Make passes each recipe line to a shell. `prefix=unkeyed-$$(date +%s)` makes a name from the time; Make needs `$$` to mean one literal `$` for the shell. The trailing `\` joins lines, and `&&` chains commands so the consumer runs only if the producer succeeded.

## Try it yourself

1. Check a script for syntax errors without running it: `bash -n scripts/scale.sh && echo ok` prints `ok`.
2. See the default argument rule: `bash -c 'count="${1:-3}"; echo $count' _` prints `3`; `bash -c 'count="${1:-3}"; echo $count' _ 8` prints `8`.
3. See a pipe and `awk` add numbers: `printf 'GROUP a b c d 7\nx a b c d 5\n' | awk '$1 != "GROUP" && $6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }'` prints `5`.
4. See `set -e` stop a script: `bash -c 'set -e; false; echo not reached'; echo "exit $?"` prints only `exit 1`.
5. With Kafka running: `make scale N=3`; the members table shows three consumers with two partitions each.

## Mistakes people make

- Leaving variables unquoted (`$file` instead of `"$file"`), which breaks on spaces. This repository quotes them.
- A failing command in the middle of a script that goes unnoticed. `set -euo pipefail` prevents it.
- Starting background processes and never stopping them. The `trap ... EXIT` cleans up.
- Using `kill -9` first. `kill` (SIGTERM) lets the Python program run its shutdown code (`tech/python.md`); the consumer commits its offsets and leaves the group.

## Check yourself

1. What does `set -e` do? *Stops the script when a command fails.*
2. What does `${1:-3}` mean? *The first argument, or 3 if there is none.*
3. What does `&` at the end of a line do, and what is `$!`? *Runs the command in the background; `$!` is its process id.*
4. What does `trap '...' EXIT` do? *Runs the given command when the script ends, however it ends.*
5. Why `$$` in a Makefile recipe? *Make would otherwise read `$`; `$$` passes one `$` to the shell.*

## Further reading

- Bash reference manual (pinned: Bash 3.2 on macOS and 5.x on Linux behave the same for everything used here): https://www.gnu.org/software/bash/manual/bash.html
- `awk` (POSIX): https://pubs.opengroup.org/onlinepubs/9699919799/utilities/awk.html
