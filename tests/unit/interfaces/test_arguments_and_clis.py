import argparse

import pytest

from order_events.interfaces import consume_cli, produce_cli
from order_events.interfaces.arguments import non_negative_float, non_negative_int, positive_int


def test_validators_accept_valid_numbers():
    assert positive_int("3") == 3
    assert non_negative_int("0") == 0
    assert non_negative_float("0.5") == 0.5


@pytest.mark.parametrize(
    ("validator", "text"), [(positive_int, "0"), (non_negative_int, "-1"), (non_negative_float, "-0.1")]
)
def test_validators_reject_out_of_range_numbers(validator, text):
    with pytest.raises(argparse.ArgumentTypeError):
        validator(text)


def test_the_producer_defaults_match_the_lessons_run():
    args = produce_cli.build_parser().parse_args([])
    assert (args.orders, args.start, args.prefix, args.no_key, args.window) == (1000, 1, "order", False, 50)


def test_the_producer_refuses_zero_orders():
    with pytest.raises(SystemExit):
        produce_cli.build_parser().parse_args(["--orders", "0"])


def test_the_consumer_runs_until_stopped_by_default():
    args = consume_cli.build_parser().parse_args([])
    assert args.until_idle is None
    assert args.delay == 0.0
    assert args.print_history == 0


def test_a_bad_environment_stops_every_program_with_exit_code_two(monkeypatch, capsys):
    monkeypatch.setenv("BOOTSTRAP_SERVERS", "not-a-server")
    assert produce_cli.main(["--orders", "1"]) == 2
    assert consume_cli.main([]) == 2
    assert "configuration error" in capsys.readouterr().err
