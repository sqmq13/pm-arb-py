from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence

from .intents import CancelIntent, Intent, PlaceOrderIntent


@dataclass(slots=True)
class ExecutionEvent:
    kind: Literal["ack"]
    order_id: str
    market_id: str | None
    intent_tag: str | None
    ts_ns: int


class SimExecutionBackend:
    def __init__(self) -> None:
        self._next_order_id = 1

    def submit(self, intents: Sequence[Intent], ts_ns: int) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        for intent in intents:
            order_id = f"sim-{self._next_order_id:08d}"
            self._next_order_id += 1
            market_id: str | None
            intent_tag: str | None
            if isinstance(intent, PlaceOrderIntent):
                market_id = intent.market_id
                intent_tag = intent.tag
            elif isinstance(intent, CancelIntent):
                market_id = intent.market_id
                intent_tag = intent.tag
                if intent.order_id is not None:
                    order_id = intent.order_id
            else:
                market_id = None
                intent_tag = None
            events.append(
                ExecutionEvent(
                    kind="ack",
                    order_id=order_id,
                    market_id=market_id,
                    intent_tag=intent_tag,
                    ts_ns=ts_ns,
                )
            )
        return events
