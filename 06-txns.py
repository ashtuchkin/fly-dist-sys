#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import sys
from async_node import AsyncNode


class TransactionsNode(AsyncNode):
    def start_node(self) -> None:
        self.store = {}

    def handle_txn(self, txn: list[tuple[str, int, int|None]]) -> dict:
        res_txn = []
        for mode, key, value in txn:
            if mode == "r":
                assert value is None
                res_txn.append((mode, key, self.store.get(key)))
            elif mode == "w":
                self.store[key] = value
                res_txn.append((mode, key, value))
            else:
                raise ValueError(f"Unknown mode: {mode}")
        return dict(txn=txn)


if __name__ == "__main__":
    node = TransactionsNode()
    print(f"Starting node {node.__class__.__name__}", file=sys.stderr)
    asyncio.run(AsyncNode.process_streams(node, stream_in=sys.stdin, stream_out=sys.stdout))
