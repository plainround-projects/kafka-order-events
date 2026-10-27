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

Both scripts are read below, line by line. Before them, six ideas that every line leans on.

- A **variable** in Bash is a name that holds text. You set it with `name=value` (no spaces around `=`) and read it with `$name` or `"${name}"`. Always put double quotes around a variable you read (`"$name"`): without quotes, a value with a space inside would be split into two words.
- **Parameter expansion with a default.** `"${LAG_ORDERS:-16667}"` means "the value of `LAG_ORDERS` if it is set and not empty, otherwise `16667`". It is how a script offers a setting that a person can change from outside (`LAG_ORDERS=100 scripts/lag.sh`) and still works with no setting.
- **Command substitution.** `$(command)` runs the command and is replaced by what it prints. `$(( expression ))` does whole-number arithmetic.
- An **array** is a numbered list of values: `list=(a b c)`. `"${list[@]}"` expands to all of them, each as its own word. Using an array to store a *command* keeps every word separate even if one contains a space.
- A **background job.** A command followed by `&` starts and the script goes on without waiting. `$!` is the process id (a number the operating system gives each running process, a **PID**) of the last background job. `wait` waits for background jobs to end. `kill PID` asks a process to stop.
- A **trap.** `trap 'commands' EXIT` registers commands to run when the script ends for any reason: normal end, an error stop, or Ctrl+C. It is how the scripts clean up the consumers they started.

### `scripts/scale.sh`

```bash
#!/usr/bin/env bash
# Run N consumers in the group, then describe the group: who owns which partitions.
#   scripts/scale.sh 3      three consumers, two partitions each
#   scripts/scale.sh 8      eight consumers on six partitions: two sit idle
set -euo pipefail
```

- Line 1: `#!/usr/bin/env bash` is the **shebang**. When the file is run as a program, the operating system reads these first two characters and the path after them and starts that program to interpret the file. `/usr/bin/env bash` means "find `bash` on the search path", which works on both macOS and Linux.
- Lines 2 to 4: comments. They show how to call the script.
- Line 5: `set -euo pipefail` switches on three safety options in one line. `-e`: stop the script at the first command that fails (exits non-zero). `-u`: treat reading an unset variable as an error (it catches typos). `-o pipefail`: a pipeline `a | b` counts as failed if *any* stage fails, not only the last one.

```bash
count="${1:-3}"
group="${GROUP_ID:-order-history}"
python="${PYTHON_BIN:-.venv/bin/python}"
mkdir -p logs
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true' EXIT
```

- Line 6: `count="${1:-3}"`. `$1` is the first argument given to the script (`scripts/scale.sh 8` gives `8`). If none was given, use `3`.
- Line 7: `group="${GROUP_ID:-order-history}"` is the consumer group name, taken from the environment or defaulting to `order-history`.
- Line 8: `python="${PYTHON_BIN:-.venv/bin/python}"` is the Python to use. The `Makefile` runs the script from the project folder, so this relative path works.
- Line 9: `mkdir -p logs` creates the `logs` folder; `-p` means no error if it already exists.
- Line 10: `pids=()` starts an empty array that will hold the process ids of the consumers.
- Line 11: `trap 'kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true' EXIT`. When the script ends, kill every consumer it started and wait for them. `2>/dev/null` hides error messages (for example if a consumer already ended). `|| true` turns a failure into success, because with `set -e` a failing `kill` would otherwise make the clean-up itself fail.

```bash
for number in $(seq 1 "$count"); do
  "$python" -m order_events.interfaces.consume_cli --name "consumer-$number" > "logs/consumer-$number.log" 2>&1 &
  pids+=("$!")
  sleep 2   # one join at a time, so each is its own rebalance
done
sleep 8     # let the last rebalance settle before reading the assignment
```

- Line 13: `for number in $(seq 1 "$count"); do ... done` loops. `seq 1 3` prints 1, 2, 3, one per line, and the loop takes them in turn.
- Line 14: starts one consumer: `"$python" -m order_events.interfaces.consume_cli --name "consumer-$number" > "logs/consumer-$number.log" 2>&1 &`. Reading from the left: the Python program with the name `consumer-1`, `consumer-2`, `consumer-3` as the loop counts; `>` sends its standard output to a file; `2>&1` sends standard error to the same place as standard output (stream 1), so the file holds both; the final `&` runs it in the background so the loop can go on.
- Line 15: `pids+=("$!")` adds the new process id to the array.
- Line 16: `sleep 2` waits two seconds, so each consumer joins the group on its own and causes its own rebalance. The comment says so.
- Line 18: `sleep 8` lets the last rebalance finish before asking the broker who owns what.

```bash
kafka_groups=(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092)
echo "== $count consumers: members and their partitions"
"${kafka_groups[@]}" --describe --group "$group" --members --verbose
echo
echo "== $count consumers: partitions, owner and lag"
"${kafka_groups[@]}" --describe --group "$group"
```

- Line 20: `kafka_groups=(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092)` stores the start of a command in an array: run Kafka's group tool inside the broker container.
- Line 22: `"${kafka_groups[@]}" --describe --group "$group" --members --verbose` runs it and prints one line per member of the group, with its client id and, with `--verbose`, the partitions assigned to it. With three consumers each shows two partitions; with eight, two show none.
- Line 23: `echo` with nothing prints an empty line.
- Line 25: the same tool without `--members` prints a line per partition: its current offset, the log-end offset, the lag and the consumer that owns it.
- When the script ends, the `trap` from line 11 stops the consumers.

### `scripts/lag.sh`

```bash
#!/usr/bin/env bash
# Build a backlog, then drain it with one slow consumer and time the recovery.
#   LAG_ORDERS=16667 scripts/lag.sh     16,667 orders x 3 events = 50,001 events (the lesson's 50,000)
set -euo pipefail
orders="${LAG_ORDERS:-16667}"
delay="${LAG_DELAY:-0.01}"
group="${GROUP_ID:-order-history}"
python="${PYTHON_BIN:-.venv/bin/python}"
mkdir -p logs
```

- Lines 1 to 4: the shebang, comments (the numbers: 16,667 orders of three events each is 50,001 events, the lesson's 50,000) and the three safety options explained above.
- Lines 5 to 8: four settings, each with an outside override and a default: the number of orders, the artificial delay in seconds per event (0.01 is 10 ms), the group, and the Python path.
- Line 9: make the `logs` folder.

```bash
kafka_groups=(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092)
total_lag() { "${kafka_groups[@]}" --describe --group "$group" 2>/dev/null | awk '$1 != "GROUP" && $6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }'; }
```

- Line 11: the same array as in `scale.sh`, holding the group tool command.
- Line 12: defines a **function**, a named block of commands: `total_lag() { ...; }`. Inside, `"${kafka_groups[@]}" --describe --group "$group" 2>/dev/null` prints the group table (error text hidden). The pipe `|` sends it to **awk**, a small language for working on lines of text split into columns. The awk program `$1 != "GROUP" && $6 ~ /^[0-9]+$/ { sum += $6 } END { print sum + 0 }` reads: for every line whose first column is not the header word `GROUP` and whose sixth column (the LAG column) consists only of digits, add that column to `sum`; when all lines are read (`END`), print `sum + 0`. The `+ 0` makes sure that a table with no matching lines prints `0` and not an empty line. Calling `total_lag` anywhere later prints the group's total lag, and `lag="$(total_lag)"` captures it in a variable.

```bash
echo "== 1. bring the group to the end of the log, then stop every consumer"
"$python" -m order_events.interfaces.consume_cli --until-idle 5 --name catch-up
echo "== 2. produce $orders orders with no consumer running"
"$python" -m order_events.interfaces.produce_cli --orders "$orders" --prefix "backlog-$(date +%s)" --max-delay 0
echo "== 3. the backlog is the lag, per partition"
"${kafka_groups[@]}" --describe --group "$group"
```

- Lines 14 and 15: step 1 prints a heading with `echo` and runs a consumer with `--until-idle 5` and the name `catch-up`, so the group reads to the end of the log and then stops. Now the group's lag is 0.
- Lines 16 and 17: step 2 produces the backlog while no consumer runs. `--prefix "backlog-$(date +%s)"` makes the ids unique with the current time; `--max-delay 0` removes the random pauses so it is quick.
- Lines 18 and 19: step 3 prints the group table. Each partition now shows its lag, about 8,000 each.

```bash
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
```

- Line 21: step 4's heading; `${delay}` inside the quotes is written with braces so the `s` after it is not read as part of the name.
- Line 22: starts the slow consumer in the background with `--delay "$delay"`, and sends its output to a log file.
- Line 23: `consumer_pid=$!` saves its PID.
- Line 24: the `trap` kills that consumer when the script ends, however it ends.
- Line 25: `started=$(date +%s)` stores the start time in seconds.
- Lines 26 to 31: `while true; do ... done` loops forever until `break`. Each round: `sleep 10`, read the total lag with `total_lag`, print `+<seconds>s  total lag: <n>` (the seconds are `$(( $(date +%s) - started ))`, now minus the start), and `[ "$lag" -eq 0 ] && break` leaves the loop when the lag is exactly 0. `[ A -eq B ]` is the test "A equals B as numbers". With `set -e` a false test inside `&&` does not stop the script.
- Line 32: prints how long the whole recovery took.

### Shell snippets inside the `Makefile`

These are explained where they appear in `tech/make.md`: the `command -v` fallbacks (lines 2 and 8 of the Makefile), the `grep ... | sed ... | sort` pipeline of `make help`, the `prefix=...; \` chain with `&&` in `run-unkeyed`, and the `| grep` filter in `order`. The same pipe, redirect and `&&` rules from this note apply.

### Shell lines inside the CI workflow

Each `run:` line of `.github/workflows/ci.yml` is run by Bash on the CI machine; see `tech/github-actions.md`.

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
