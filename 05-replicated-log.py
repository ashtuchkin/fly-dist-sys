#!/usr/bin/env python3
from __future__ import annotations
import asyncio
from collections import defaultdict
import random
import sys
from typing import Any
from async_node import AsyncNode, KVStore


class ReplicatedLogNode(AsyncNode):
    def start_node(self) -> None:
        self.seq_store = KVStore(self, "seq-kv")
        self.lin_store = KVStore(self, "lin-kv")

    async def handle_send(self, key: str, msg: Any) -> dict:
        while True:
            try:
                log = await self.lin_store.read(f"log.{key}")
            except KeyError:
                log = []
            new_log = log + [msg]
            if await self.lin_store.cas(f"log.{key}", log, new_log, create_if_not_exists=True):
                break
            await asyncio.sleep(0.1)
        return dict(offset=len(new_log)-1)

    async def handle_poll(self, offsets: dict[str, int]) -> dict:
        futures = [
            self.lin_store.read(f"log.{key}")
            for key in offsets.keys()
        ]
        results = await asyncio.gather(*futures, return_exceptions=True)
        for i in range(len(results)):
            if isinstance(results[i], Exception):
                results[i] = []

        msgs = {
            key: [[offset, msg] for offset, msg in enumerate(log) if offset >= start_offset]
            for log, (key, start_offset) in zip(results, offsets.items())
        }
        return dict(msgs=msgs)

    async def handle_commit_offsets(self, offsets: dict[str, int]) -> dict:
        futures = [
            self.seq_store.write(f"commit.{key}", offset)
            for key, offset in offsets.items()
        ]
        await asyncio.gather(*futures)
        return dict()

    async def handle_list_committed_offsets(self, keys: list[str]) -> dict:
        await self.seq_store.write("commit-sync", random.randrange(1000_000_000))
        futures = [
            self.seq_store.read(f"commit.{key}")
            for key in keys
        ]
        results = await asyncio.gather(*futures, return_exceptions=True)

        offsets = {}
        for key, result in zip(keys, results):
            if not isinstance(result, Exception):
                offsets[key] = result

        return dict(offsets=offsets)


if __name__ == "__main__":
    node = ReplicatedLogNode()
    print(f"Starting node {node.__class__.__name__}", file=sys.stderr)
    asyncio.run(AsyncNode.process_streams(node, stream_in=sys.stdin, stream_out=sys.stdout))
