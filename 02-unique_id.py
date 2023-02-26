#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import sys
from async_node import AsyncNode, Message


class GenerateMessage(Message):
    TYPE = "generate"

class GenerateOkMessage(Message):
    TYPE = "generate_ok"
    id: str


class UniqueIdGeneratorNode(AsyncNode):
    def __init__(self) -> None:
        super().__init__()
        self._next_id = 0

    def handle_generate(self, msg: GenerateMessage) -> GenerateOkMessage:
        self._next_id += 1
        return GenerateOkMessage(id=f"{self.node_id}-{self._next_id}")


if __name__ == "__main__":
    node = UniqueIdGeneratorNode()
    print(f"Starting node {node.__class__.__name__}", file=sys.stderr)
    asyncio.run(AsyncNode.process_streams(node, stream_in=sys.stdin, stream_out=sys.stdout))
