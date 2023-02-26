#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import sys
from async_node import AsyncNode, Message


class EchoMessage(Message):
    TYPE = "echo"
    echo: str

class EchoOkMessage(Message):
    TYPE = "echo_ok"
    echo: str


class EchoNode(AsyncNode):
    def handle_echo(self, msg: EchoMessage) -> EchoOkMessage:
        return EchoOkMessage(echo=msg.echo)

if __name__ == "__main__":
    print("Starting echo node", file=sys.stderr)
    node = EchoNode()
    asyncio.run(AsyncNode.process_streams(node, stream_in=sys.stdin, stream_out=sys.stdout))
