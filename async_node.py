from __future__ import annotations
import asyncio
import inspect
import json
import sys
from typing import Any, Awaitable, Callable, Literal, TypedDict

from utils import connect_input_stream, connect_output_stream

DEFAULT_TIMEOUT_S = 1.0

class Message(TypedDict):
    src: str
    dest: str
    body: dict


class RPCError(Exception):
    """ Error coming from a remote RPC call"""
    def __init__(self, text: str, code: int):
        self.code = code
        super().__init__(text)


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
                fut.set_exception(RPCError(body["text"], body["code"]))
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
        except KeyError as e:
            resp = {"type": "error", "code": 20, "text": f"Key {e} not found"}
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

    async def send_rpc(self, dest: str, type: str, timeout: float = DEFAULT_TIMEOUT_S, **params) -> dict:  # TODO: typing
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


class KVStore:
    def __init__(self, node: AsyncNode, service: Literal["lin-kv", "seq-kv", "lww-kv"]) -> None:
        self._node = node
        self._service = service

    async def read(self, key: str, timeout: float = DEFAULT_TIMEOUT_S) -> Any:
        """ Read a value from a key. Raises KeyError if the key does not exist. """
        try:
            data = await self._node.send_rpc(self._service, "read", timeout, key=key)
            return data["value"]
        except RPCError as e:
            if e.code == 20:
                raise KeyError(key)
            else:
                raise

    async def write(self, key: str, value: Any, timeout: float = DEFAULT_TIMEOUT_S) -> None:
        """ Write a value to a key. Creates a key if it does not exist. """
        await self._node.send_rpc(self._service, "write", timeout, key=key, value=value)

    async def cas(self, key: str, old_value: Any, new_value: Any, create_if_not_exists: bool = False, timeout: float = DEFAULT_TIMEOUT_S) -> bool:
        """ Compare and swap. Returns True if the value was updated, False if the value was not updated."""
        params = {"key": key, "from": old_value, "to": new_value, "create_if_not_exists": create_if_not_exists}
        try:
            await self._node.send_rpc(self._service, "cas", timeout, **params)
        except RPCError as e:
            if e.code == 20:
                raise KeyError(key)
            elif e.code == 22:
                return False
            else:
                raise

        return True
