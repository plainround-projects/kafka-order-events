# YAML

## What it is

YAML is a plain-text format for settings. It describes the same things as JSON (text, numbers, lists, name-value maps) but uses indentation instead of braces, so it is easier to read. `#` starts a comment.

## Why this repository uses it

Two tools read their instructions as YAML: Docker Compose (`docker-compose.yml`) and GitHub Actions (`.github/workflows/ci.yml`).

## Where it appears

| Path | What it holds |
| --- | --- |
| `docker-compose.yml` | The Kafka container (see `tech/docker-compose.md`). |
| `.github/workflows/ci.yml` | The continuous-integration jobs (see `tech/github-actions.md`). |

## How it works underneath

A YAML file is a tree. A line `name: value` is a **mapping** entry. Lines starting with `- ` are a **list**. Indentation (spaces only, never tabs) shows which entries belong to which parent. Text rarely needs quotes, but quotes are needed when a value could be misread: `"9092:9092"` is quoted because some YAML readers treat `number:number` as a base-60 number. A `>-` after a key starts a **folded block**: following indented lines are joined into one line with single spaces.

## Walkthrough of this repository's use

### The five things YAML is made of

YAML describes data with indentation. There are only a few building blocks, and each is shown below in a line of this repository's own files.

1. **A mapping**: `key: value` pairs, one per line. The key is a name; the value is whatever follows the colon and a space. `image: apache/kafka:4.0.0` is a mapping entry with key `image`. A mapping can hold other mappings: put the nested keys on the following lines, indented further than the parent.
2. **A sequence** (a list): lines that start with `- `. In `ports:` followed by `- "9092:9092"`, the key `ports` holds a list of one item.
3. **A scalar** (a single value): text, a number, `true` or `false`, or null. Text normally needs no quotes (`image: apache/kafka:4.0.0`). Quotes become necessary when a value contains a special character, or would be read as something else. `"9092:9092"` is quoted because an unquoted `9092:9092` can be read as a number in base 60 by some YAML readers. `"3.12"` is quoted so it stays the text `3.12` and does not become the number 3.12 (which would turn `3.10` into `3.1`).
4. **A flow list**: a list on one line in square brackets, `[push, pull_request]`; and a **flow mapping** in curly braces. They are shorthand for the line-by-line forms.
5. **A block scalar**: a long text spread over many lines. `>-` ("folded") joins the lines into one with spaces and drops the final newline. `|` ("literal") keeps the line breaks.

Two rules matter more than all others:

- **Indentation means structure.** Use spaces, never tabs. Every line at the same depth must have the same number of spaces. One space too few can move a setting into the wrong parent. This repository uses two spaces per level (`.editorconfig` enforces it).
- A `#` begins a comment that runs to the end of the line.

### In `docker-compose.yml`

```yaml
# Single-node Kafka 4 in KRaft mode: one process is both broker and controller, no ZooKeeper.
name: kafka-order-events

services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
```

- Line 1: a comment. Line 2: `name:` is a mapping entry at the top level; its value is the text `kafka-order-events`.
- Line 4: `services:` has no value on its line, so its value is the **indented block** below it: a mapping with one key, `kafka`, whose value is another indented mapping with keys `image` and `ports`.
- Lines 7 and 8: `ports:` holds a sequence with one item, the quoted text `"9092:9092"`.

```yaml
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      # Clients on this Mac connect to localhost:9092, so the broker advertises exactly that.
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
```

`environment:` is a mapping of variable names to values. Values such as `1` are read as numbers and `broker,controller` as text; Compose turns every value into text for the container anyway. The comment lines inside the mapping are ignored.

```yaml
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20
      start_period: 10s
```

`healthcheck:` is a mapping. Its `test:` entry is a flow list with two quoted strings, `["CMD-SHELL", "..."]`: the first says how to run the second. `interval: 5s` is the text `5s`, which Compose understands as five seconds.

### In `.github/workflows/ci.yml`

```yaml
name: ci

on: [push, pull_request]

jobs:
  check:
    name: Format, lint, types, import rules, unit tests with coverage, dependency audit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: make check
      - run: make audit
```

- `on: [push, pull_request]` is a flow list. (The word `on` is a famous trap: older YAML readers turned a bare `on` into the boolean `true`. GitHub's reader takes it as the key `on`, so it works, but it is why some projects quote it.)
- `jobs:` holds a mapping of job names. `check:` is a job whose entries are `name`, `runs-on` and `steps`.
- `steps:` is a sequence. Each item starts with `- `; an item can be a mapping that continues on the following lines at a deeper indentation: `- uses: actions/setup-python@v5`, then `with:` (aligned with `uses:`) holding `python-version: "3.12"`.

```yaml
        options: >-
          --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
          --health-interval 5s
          --health-timeout 10s
          --health-retries 20
          --health-start-period 10s
```

`options: >-` starts a folded block scalar. The five indented lines become one long line, `--health-cmd "..." --health-interval 5s --health-timeout 10s --health-retries 20 --health-start-period 10s`, which GitHub passes to Docker. The `-` after `>` removes the final newline.

### A mistake worth seeing once

Open a copy of `docker-compose.yml` and replace the two spaces before `image:` with a tab character. Running `docker compose -f THAT-FILE config` on it then stops with `yaml: line 6: found character that cannot start any token`: the tab. Nothing is wrong with the file's meaning; YAML does not allow tabs in indentation.

## Try it yourself

1. Parse a YAML file with the same tool the repository uses for checking: `docker compose config > /dev/null && echo valid`.
2. See Compose's own normalised view of the file: `docker compose config | head -12`.
3. Break it on purpose: copy the file, replace the indentation of `image:` with a tab, run `docker compose -f copy.yml config`; it reports `yaml: line 6: found character that cannot start any token`.

## Mistakes people make

- Tabs for indentation: not allowed.
- Wrong indentation moves an entry to another parent without any warning.
- Unquoted values that YAML reinterprets (`no`, `on`, `1e3`, `09`). The Actions key `on` is special-cased by GitHub.
- Forgetting the space after `:`. `key:value` is one text value.

## Check yourself

1. What shows nesting in YAML? *Indentation with spaces.*
2. How is a list written? *Lines starting with `- `, or `[a, b]` on one line.*
3. What does `>-` do? *Starts a folded block: lines joined by spaces, no trailing newline.*
4. Why quote `"9092:9092"`? *So a parser cannot read it as a number.*
5. What starts a comment? *`#`.*

## Further reading

- YAML 1.2.2 specification: https://yaml.org/spec/1.2.2/
