import orjson

from pm_arb.capture_online import assign_shards, split_subscribe_groups
from pm_arb.clob_ws import build_subscribe_payload


def test_assign_shards_deterministic():
    tokens = ["tokenA", "tokenB", "tokenC", "tokenD", "tokenE"]
    first = assign_shards(tokens, 3)
    second = assign_shards(tokens, 3)
    assert first == second
    flattened = sorted(token for group in first.values() for token in group)
    assert flattened == sorted(tokens)


def test_split_subscribe_groups_limits():
    tokens = [f"token{i}" for i in range(10)]
    groups = split_subscribe_groups(tokens, max_tokens=3, max_bytes=200, variant="A")
    assert groups
    for group in groups:
        assert len(group) <= 3
        payload_bytes = orjson.dumps(build_subscribe_payload("A", group))
        assert len(payload_bytes) <= 200
    assert split_subscribe_groups(tokens, max_tokens=3, max_bytes=200, variant="A") == groups
