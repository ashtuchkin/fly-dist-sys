#!/usr/bin/env python3
from __future__ import annotations
import sys
from typing import Iterable

from maelstrom_node import ErrorMessage, Message, Node, process_streams


class EchoMessage(Message):
    TYPE = "echo"
    echo: str

class EchoOkMessage(Message):
    TYPE = "echo_ok"
    echo: str


class EchoNode(Node):
    def process_msgs(self, msgs: Iterable[Message]) -> Iterable[Message]:
        for msg in msgs:
            match msg:
                case EchoMessage():
                    reply_msg = EchoOkMessage(echo=msg.echo)
                case _:
                    reply_msg = ErrorMessage(code=12, text=f"Unknown message type: {msg!r}")

            yield msg.reply(reply_msg)


if __name__ == "__main__":
    print("Starting echo node", file=sys.stderr)
    process_streams(EchoNode)
