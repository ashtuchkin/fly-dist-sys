#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import sys
from async_node import AsyncNode, Message


class BroadcastMessage(Message):
    TYPE = "broadcast"
    message: int

class BroadcastOkMessage(Message):
    TYPE = "broadcast_ok"


class ReadMessage(Message):
    TYPE = "read"

class ReadOkMessage(Message):
    TYPE = "read_ok"
    messages: list[int]

class TopologyMessage(Message):
    TYPE = "topology"
    topology: dict[str, list[str]]

class TopologyOkMessage(Message):
    TYPE = "topology_ok"


class GossipMessage(Message):
    TYPE = "gossip"
    messages: list[int]



class BroadcastNode(AsyncNode):
    def __init__(self) -> None:
        super().__init__()
        self.messages: set[int] = set()

    async def handle_broadcast(self, msg: BroadcastMessage) -> BroadcastOkMessage:
        self.messages.add(msg.message)
        gossip_msg = GossipMessage(messages=[msg.message])
        for node_id in self.all_node_ids:
            if node_id != self.node_id:
                await self.send_msg(gossip_msg, node_id)
        return BroadcastOkMessage()

    def handle_read(self, msg: ReadMessage) -> ReadOkMessage:
        return ReadOkMessage(messages=list(self.messages))

    def handle_topology(self, msg: TopologyMessage) -> TopologyOkMessage:
        return TopologyOkMessage()

    def handle_gossip(self, msg: GossipMessage) -> None:
        self.messages.update(msg.messages)

if __name__ == "__main__":
    node = BroadcastNode()
    print(f"Starting node {node.__class__.__name__}", file=sys.stderr)
    asyncio.run(AsyncNode.process_streams(node, stream_in=sys.stdin, stream_out=sys.stdout))
