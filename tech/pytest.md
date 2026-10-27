# pytest

## What it is

pytest is a Python tool that finds and runs tests. A **test** is a function whose name starts with `test_`. It runs some of your code and uses `assert` to say what must be true. If an `assert` is false, the test fails and pytest shows the values. This repository pins pytest 9.1.1.

## Why this repository uses it

The template asks for tests at three levels. pytest runs all three with different commands and keeps them apart with folders and markers.

## Where it appears

| Path | What it does |
| --- | --- |
| `tests/unit/` | Fast tests with no Kafka: domain, application (with fake ports), infrastructure helpers, CLI arguments. |
| `tests/integration/` | Tests of the Kafka adapters against the real broker. |
| `tests/e2e/` | The lesson's scenario through the command-line programs. |
| `tests/conftest.py` | Shared **fixtures** (setup code the tests can ask for). |
| `pyproject.toml` | `[tool.pytest.ini_options]`: where tests are and the marker names. |
| `Makefile` | `test`, `test-integration`, `test-e2e`. |

## How it works underneath

pytest imports every `test_*.py` file under the folders you give it, collects the functions that start with `test_`, and runs each. When a test function has a parameter (such as `settings` or `topic`), pytest looks for a fixture with that name, runs it first, and passes the result in. A fixture that uses `yield` runs its code before the `yield` as setup and the code after it as cleanup, even if the test fails. **Markers** (`@pytest.mark.integration`) label tests so you can choose them with `-m`.

## Walkthrough of this repository's use

### Plain tests that read as behavior

```python
def event(order_id: str, sequence: int, type_: EventType = EventType.PLACED) -> OrderEvent:
    return OrderEvent(order_id, type_, sequence, 0.0)


def test_events_in_sequence_log_nothing():
    checker = OrderChecker()
    assert all(checker.observe(event("order-1", sequence), partition=3) for sequence in (1, 2, 3))
    assert checker.out_of_order_count == 0
    assert checker.report().orders_split_across_partitions == 0


def test_an_event_that_skips_ahead_is_out_of_order():
    checker = OrderChecker()
    checker.observe(event("order-1", 1), partition=0)
    assert checker.observe(event("order-1", 3), partition=1) is False
    assert checker.report().out_of_order == (OutOfOrder("order-1", expected=2, got=3),)
```

Each function name is a sentence describing the behavior. `assert` compares; pytest rewrites it so a failure prints both sides. `checker.observe(...) is False` checks the exact value `False`.

### Parametrized tests: one test, many inputs

```python
@pytest.mark.parametrize(
    "raw",
    [
        b"x",
        b"[]",
        b"{}",
        b'{"orderId": "a", "type": "OrderRefunded", "sequence": 1, "ts": 1}',
        b'{"orderId": "a", "type": "OrderPaid", "sequence": "two", "ts": 1}',
        b'{"orderId": "", "type": "OrderPaid", "sequence": 1, "ts": 1}',
        b'{"orderId": "a", "type": "OrderPaid", "sequence": 0, "ts": 1}',
    ],
)
def test_anything_that_is_not_a_valid_event_is_rejected(raw):
    with pytest.raises(MalformedEventError):
        decode_event(raw)
```

`@pytest.mark.parametrize("raw", [...])` runs the test once for each item in the list, each shown as its own result. `with pytest.raises(MalformedEventError):` passes only if the code inside raises that error.

### Fixtures with setup and cleanup

```python
@pytest.fixture
def settings() -> Settings:
    return Settings.from_env({"BOOTSTRAP_SERVERS": BOOTSTRAP})


@pytest.fixture
def topic() -> Iterator[str]:
    """A fresh six-partition topic per test, so no test sees another's events."""
    name = f"test-order-events-{uuid.uuid4().hex[:8]}"
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    admin.create_topics([NewTopic(name, num_partitions=6, replication_factor=1)])
    yield name
    admin.delete_topics([name])
    admin.close()
```

`@pytest.fixture` marks a function as a fixture. The first one, `settings`, builds a settings object. The second, `topic`, creates a topic with a random name, `yield`s its name to the test, and after the test deletes the topic. No test sees another's data. A file named `conftest.py` is read automatically; its fixtures are available to every test below it.

### Markers and selecting levels

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers"
markers = [
    "integration: needs the broker from docker-compose.yml (make up)",
    "e2e: runs the lesson's scenario through the command-line programs against the broker",
]
```

- `testpaths = ["tests"]`: where to look when no folder is given.
- `addopts = "-ra --strict-markers"`: options added to every run. `-ra` prints a summary of non-passing tests. `--strict-markers` makes an unknown marker an error, which catches typos.
- `markers = [...]`: the two allowed markers. A test file sets `pytestmark = pytest.mark.integration` once to label every test in it.

`make test` runs `pytest tests/unit` only. `make test-integration` runs `pytest tests/integration -m integration`.

## Try it yourself

1. `.venv/bin/python -m pytest tests/unit -q` prints a row of dots and `81 passed`.
2. `.venv/bin/python -m pytest tests/unit/domain/test_ordering.py -v` lists each test name with `PASSED`.
3. `.venv/bin/python -m pytest -k late_event -v` runs only tests with `late_event` in their name.
4. `.venv/bin/python -m pytest --collect-only -q -m integration tests | tail -2` prints `6/90 tests collected (84 deselected)`.
5. Break a test on purpose: change `expected=2` in `test_an_event_that_skips_ahead_is_out_of_order` to `3` and watch pytest print both values; undo it with `git checkout tests`.

## Mistakes people make

- Tests that depend on each other's data. Fixtures with unique topic names avoid it.
- Giving a test the same name in two files without `__init__.py`, which confuses pytest. This repository has `__init__.py` in the test folders.
- Calling real time or randomness in unit tests. The code takes a clock and a `Random` as arguments, so tests are repeatable.
- Forgetting the marker, so the slow tests run with the fast ones.

## Check yourself

1. How does pytest know a function is a test? *Its name starts with `test_` in a `test_*.py` file.*
2. What does `yield` do in a fixture? *Hands the value to the test; the code after it is cleanup.*
3. What does `-m integration` do? *Runs only tests marked `integration`.*
4. What does `parametrize` do? *Runs one test function once per listed input.*
5. Why `--strict-markers`? *An unknown marker name becomes an error instead of being silently ignored.*

## Further reading

- pytest 9.1 documentation: https://docs.pytest.org/en/9.1.x/
- Fixtures: https://docs.pytest.org/en/9.1.x/how-to/fixtures.html
- Markers: https://docs.pytest.org/en/9.1.x/how-to/mark.html
