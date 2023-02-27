from __future__ import annotations
import asyncio
import inspect
import json
import sys
from typing import Awaitable, Callable, TypedDict

from utils import connect_input_stream, connect_output_stream

class Message(TypedDict):
    src: str
    dest: str
    body: dict


class AsyncNode:
    node_id: str
    all_node_ids: list[str]
    _send_msg: Callable[[Message], Awaitable[None]] | None
    _task_group: asyncio.TaskGroup

    def __init__(self):
        self._next_msg_id = 1
        self._rpcs = {}
        self._msg_types = {}
        for name, method in inspect.getmembers(self, inspect.ismethod):
            if name.startswith("handle_"):
                msg_type = name[7:]
                sig = inspect.signature(method)
                assert msg_type not in self._msg_types, "Duplicate message type"
                self._msg_types[msg_type] = (method, list(sig.parameters.keys()))

    def set_send_msg(self, send_msg: Callable[[Message], Awaitable[None]] | None):
        self._send_msg = send_msg

    def set_task_group(self, task_group: asyncio.TaskGroup):
        self._task_group = task_group

    def start_node(self):
        """ To be overridden by subclasses """
        pass

    async def receive_msg(self, msg: Message):
        #print("receive_msg", msg, file=sys.stderr)
        assert not hasattr(self, "node_id") or msg["dest"] == self.node_id
        msg_src = msg["src"]
        body = msg["body"]
        msg_type = body.pop("type")
        msg_id = body.pop("msg_id", None)
        msg_reply_to = body.pop("in_reply_to", None)

        if msg_reply_to is not None:
            fut, expected_type = self._rpcs[(msg_src, msg_reply_to)]
            if msg_type == "error":
                code = body.pop("code", None)
                text = body.pop("text", None)
                fut.set_exception(Exception(text))  # TODO: rpc exception code
            else:
                if msg_type != expected_type:
                    body["type"] = msg_type
                fut.set_result(body)
            return

        handler, param_names = self._msg_types[msg_type]

        if "src" in param_names:
            body["src"] = msg_src
        if "msg_id" in param_names:
            body["msg_id"] = msg_id

        try:
            resp = handler(**body)
            if inspect.isawaitable(resp):
                resp = await resp
        except Exception as e:
            resp = {
                "type": "error",
                # TODO: map code https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
                "code": 1000,
                "text": str(e),
            }

        if resp is not None:
            assert isinstance(resp, dict), f"Expected dict, got {type(resp)}"
            resp_type = resp.pop("type", msg_type+"_ok")
            resp["in_reply_to"] = msg_id
            await self.send_msg(msg_src, resp_type, **resp)

    async def send_msg(self, dest: str, type: str, **params) -> int:
        body = params
        assert "msg_id" not in body, "msg_id is reserved"
        body["msg_id"] = self._next_msg_id
        self._next_msg_id += 1
        body["type"] = type

        msg: Message = {
            "src": self.node_id,
            "dest": dest,
            "body": body,
        }
        assert self._send_msg is not None, "send_msg not set"
        #print("send_msg", msg, file=sys.stderr)
        await self._send_msg(msg)
        return body["msg_id"]

    async def send_rpc(self, dest: str, type: str, timeout: float, **params) -> dict:  # TODO: typing
        msg_id = await self.send_msg(dest, type, **params)
        fut = asyncio.get_running_loop().create_future()
        self._rpcs[(dest, msg_id)] = (fut, type+"_ok")

        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            del self._rpcs[(dest, msg_id)]

    def handle_init(self, node_id: str, node_ids: list[str]) -> dict:
        assert not hasattr(self, "node_id"), "Already initialized"
        self.node_id = node_id
        self.all_node_ids = node_ids
        self.start_node()
        return {}

    @classmethod
    async def process_streams(
        cls,
        node: AsyncNode,
        stream_in = sys.stdin,
        stream_out = sys.stdout,
    ):
        reader = await connect_input_stream(stream_in)
        writer = await connect_output_stream(stream_out)

        async def send_msg(msg: Message):
            writer.write(json.dumps(msg).encode("utf8") + b"\n")
            await writer.drain()

        async with asyncio.TaskGroup() as tg:
            node.set_send_msg(send_msg)
            node.set_task_group(tg)
            async for line in reader:
                print("input", line.decode("utf8"), file=sys.stderr, flush=True)
                msg: Message = json.loads(line.decode("utf8"))
                tg.create_task(node.receive_msg(msg))
