#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import sys
from async_node import AsyncNode


class EchoNode(AsyncNode):
    def handle_echo(self, echo: str) -> dict:
        return {"echo": echo}


if __name__ == "__main__":
    print("Starting echo node", file=sys.stderr)
    node = EchoNode()
    asyncio.run(AsyncNode.process_streams(node, stream_in=sys.stdin, stream_out=sys.stdout))
