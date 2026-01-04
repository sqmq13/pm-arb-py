import orjson
import pytest
from collections import deque

from pm_arb.capture_online import (
    ShardAssignmentError,
    _extract_minimal_fields,
    _quantile_from_samples,
    _stable_hash,
    assign_shards_by_market,
    split_subscribe_groups,
)
from pm_arb.clob_ws import build_subscribe_payload


def test_assign_shards_by_market_deterministic():
    markets = [
        {"id": "m1", "token_ids": ["tokenA", "tokenB"]},
        {"id": "m2", "token_ids": ["tokenC", "tokenD"]},
        {"id": "m3", "token_ids": ["tokenE", "tokenF"]},
    ]
    first, first_market_shards = assign_shards_by_market(markets, 3)
    second, second_market_shards = assign_shards_by_market(markets, 3)
    assert first == second
    assert first_market_shards == second_market_shards
    for market in markets:
        shard_id = first_market_shards[str(market["id"])]
        for token_id in market["token_ids"]:
            assert token_id in first[shard_id]


def test_assign_shards_by_market_token_conflict():
    shard_count = 2
    ids_by_shard: dict[int, str] = {}
    for idx in range(1000):
        market_id = f"m{idx}"
        shard_id = _stable_hash(market_id) % shard_count
        ids_by_shard.setdefault(shard_id, market_id)
        if len(ids_by_shard) >= 2:
            break
    assert len(ids_by_shard) >= 2
    shard_ids = sorted(ids_by_shard)
    market_a = ids_by_shard[shard_ids[0]]
    market_b = ids_by_shard[shard_ids[1]]
    markets = [
        {"id": market_a, "token_ids": ["tokenA", "tokenB"]},
        {"id": market_b, "token_ids": ["tokenB", "tokenC"]},
    ]
    with pytest.raises(ShardAssignmentError):
        assign_shards_by_market(markets, shard_count)


def test_split_subscribe_groups_limits():
    tokens = [f"token{i}" for i in range(10)]
    groups = split_subscribe_groups(tokens, max_tokens=3, max_bytes=200, variant="A")
    assert groups
    for group in groups:
        assert len(group) <= 3
        payload_bytes = orjson.dumps(build_subscribe_payload("A", group))
        assert len(payload_bytes) <= 200
    assert split_subscribe_groups(tokens, max_tokens=3, max_bytes=200, variant="A") == groups


def test_extract_minimal_fields_book_event():
    payload = {"event_type": "book", "asset_id": "123", "bids": [], "asks": []}
    token_pairs, msg_type_counts = _extract_minimal_fields(payload)
    assert ("123", "book") in token_pairs
    assert msg_type_counts["book"] == 1


def test_extract_minimal_fields_price_change_event():
    payload = {
        "event_type": "price_change",
        "price_changes": [{"asset_id": "a"}, {"asset_id": "b"}],
    }
    token_pairs, msg_type_counts = _extract_minimal_fields(payload)
    assert ("a", "price_change") in token_pairs
    assert ("b", "price_change") in token_pairs
    assert msg_type_counts["price_change"] == 1


def test_extract_minimal_fields_infers_book():
    payload = {"asset_id": "456", "bids": [], "asks": []}
    token_pairs, msg_type_counts = _extract_minimal_fields(payload)
    assert ("456", "book") in token_pairs
    assert msg_type_counts["book"] == 1


def test_quantile_from_samples_ignores_zero():
    samples = deque([0, 0, 5, 10])
    assert _quantile_from_samples(samples, 50) == 5


def test_quantile_from_samples_all_zero():
    samples = deque([0, 0, 0])
    assert _quantile_from_samples(samples, 95) == 0
