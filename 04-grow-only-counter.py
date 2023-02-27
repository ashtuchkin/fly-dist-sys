#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import random
import sys
from async_node import AsyncNode, KVStore


class CounterNode(AsyncNode):
    def start_node(self) -> None:
        self.store = KVStore(self, "seq-kv")

    async def handle_add(self, delta: int) -> dict:
        while True:
            try:
                cur_val = await self.store.read("counter")
            except KeyError:
                cur_val = 0
            if await self.store.cas("counter", cur_val, cur_val + delta, create_if_not_exists=True):
                break
            await asyncio.sleep(0.1)
        return dict()

    async def handle_read(self) -> dict:
        # Do a "sync" to read latest values. See https://github.com/jepsen-io/maelstrom/issues/39#issuecomment-1445414521
        # Looks like seq-kv is sequential across all keys.
        await self.store.write("sync", random.randrange(1000_000_000))
        try:
            val = await self.store.read("counter")
        except KeyError:
            val = 0
        return dict(value=val)


if __name__ == "__main__":
    node = CounterNode()
    print(f"Starting node {node.__class__.__name__}", file=sys.stderr)
    asyncio.run(AsyncNode.process_streams(node, stream_in=sys.stdin, stream_out=sys.stdout))
