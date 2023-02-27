#!/usr/bin/env python3
import asyncio
import math
import random
import sys
from async_node import AsyncNode

GOSSIP_INTERVAL_SEC = 0.2
NUM_GOSSIP_NODES = lambda total_nodes: int(math.ceil(math.sqrt(total_nodes)))

class BroadcastNode(AsyncNode):
    def start_node(self) -> None:
        self.messages: set[int] = set()
        self.num_gossip_nodes = NUM_GOSSIP_NODES(len(self.all_node_ids))
        self.peer_node_ids = [node_id for node_id in self.all_node_ids if node_id != self.node_id]
        self._task_group.create_task(self.gossip())

    async def gossip(self) -> None:
        while True:
            await asyncio.sleep(GOSSIP_INTERVAL_SEC)

            for peer_id in random.sample(self.peer_node_ids, self.num_gossip_nodes):
                await self.send_msg(peer_id, "gossip", messages=list(self.messages))

    async def handle_broadcast(self, message: int) -> dict:
        self.messages.add(message)
        return dict()

    def handle_read(self) -> dict:
        return dict(messages=list(self.messages))

    def handle_topology(self, topology: dict[str, list[str]]) -> dict:
        return dict()

    def handle_gossip(self, messages: list[int]) -> None:
        self.messages.update(messages)


if __name__ == "__main__":
    node = BroadcastNode()
    print(f"Starting node {node.__class__.__name__}", file=sys.stderr)
    asyncio.run(AsyncNode.process_streams(node, stream_in=sys.stdin, stream_out=sys.stdout))
