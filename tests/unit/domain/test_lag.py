from order_events.domain.lag import compute_lag


def test_lag_is_log_end_minus_committed():
    assert compute_lag({0: 100, 1: 80}, {0: 40, 1: 80}) == {0: 60, 1: 0}


def test_a_partition_with_no_commit_counts_from_zero():
    assert compute_lag({0: 25}, {0: None}) == {0: 25}
    assert compute_lag({0: 25}, {}) == {0: 25}
