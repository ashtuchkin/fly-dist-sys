from __future__ import annotations
import asyncio
import inspect
import json
import sys
from typing import Awaitable, Callable, ClassVar, Dict, Iterable, Type, TypeVar, get_type_hints
from pydantic import BaseModel

from utils import connect_input_stream, connect_output_stream


class EnvelopeMessage(BaseModel):
    src: str
    dest: str
    body: dict


class Message(BaseModel):
    TYPE: ClassVar[str]
    REPLY_TYPE: ClassVar[Type[Message]] | None = None


class ErrorMessage(Message):
    """ Returned by the node when it cannot service a request """
    TYPE = "error"
    code: int  # 0=timeout, .. see https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
    text: str

class InitMessage(Message):
    TYPE = "init"
    node_id: str
    node_ids: list[str]

class InitOkMessage(Message):
    TYPE = "init_ok"


class AsyncNode:
    node_id: str
    all_node_ids: list[str]
    _send_msg: Callable[[EnvelopeMessage], Awaitable[None]] | None
    _task_group: asyncio.TaskGroup

    def __init__(self):
        self._next_msg_id = 1
        self._rpcs = {}
        self._msg_types = {}
        for name, method in inspect.getmembers(self, inspect.ismethod):
            if name.startswith("handle_"):
                msg_type = name[7:]
                sig = inspect.signature(method, eval_str=True)
                assert len(sig.parameters) == 1
                _, input_param = next(iter(sig.parameters.items()))
                input_type = input_param.annotation
                assert not isinstance(input_type, str), f"{input_type} can't be parsed"
                assert issubclass(input_type, Message), f"{input_type} is not a Message"
                assert input_type.TYPE == msg_type

                output_type = sig.return_annotation
                if output_type is inspect.Signature.empty:
                    output_type = None
                assert output_type is None or issubclass(output_type, Message)

                assert msg_type not in self._msg_types, "Duplicate message type"
                self._msg_types[msg_type] = (method, input_type, output_type)

    def set_send_msg(self, send_msg: Callable[[EnvelopeMessage], Awaitable[None]] | None):
        self._send_msg = send_msg

    def set_task_group(self, task_group: asyncio.TaskGroup):
        self._task_group = task_group

    async def receive_msg(self, msg: EnvelopeMessage):
        #print("receive_msg", msg, file=sys.stderr)
        msg_type = msg.body.pop("type")
        msg_id = msg.body.pop("msg_id", None)
        msg_reply_to = msg.body.pop("in_reply_to", None)

        if msg_reply_to is not None:
            fut, input_class = self._rpcs[(msg.src, msg_reply_to)]
            output_class = None
            if msg_type == "error":
                handler = lambda body: fut.set_exception(Exception(body.text))
            else:
                handler = lambda body: fut.set_result(body)
        else:
            handler, input_class, output_class = self._msg_types[msg_type]

        body = input_class.parse_obj(msg.body)
        if hasattr(body, "src"):
            body.src = msg.src
        if hasattr(body, "msg_id"):
            body.msg_id = msg_id
        assert not hasattr(self, "node_id") or msg.dest == self.node_id

        resp = handler(body)
        if inspect.isawaitable(resp):
            resp = await resp

        if output_class is not None:
            assert isinstance(resp, output_class), f"Expected {output_class}, got {type(resp)}"
            await self.send_msg(resp, dest=msg.src, in_reply_to=msg_id)
        else:
            assert resp is None

    async def send_msg(self, msg: Message, dest: str, in_reply_to: int | None = None) -> int:
        assert msg is not None, "Message must not be None"
        assert dest is not None, "Destination must not be None"
        body = msg.dict(exclude_none=True)
        body["msg_id"] = self._next_msg_id
        self._next_msg_id += 1
        body["type"] = msg.TYPE
        if in_reply_to is not None:
            body["in_reply_to"] = in_reply_to

        env_msg = EnvelopeMessage(
            src=self.node_id,
            dest=dest,
            body=body,
        )
        assert self._send_msg is not None, "send_msg not set"
        #print("send_msg", env_msg, file=sys.stderr)
        await self._send_msg(env_msg)
        return body["msg_id"]

    async def send_rpc(self, msg: Message, dest: str, timeout: float) -> Message:  # TODO: typing
        msg_id = await self.send_msg(msg, dest=dest)
        fut = asyncio.get_running_loop().create_future()
        self._rpcs[(dest, msg_id)] = (fut, msg.REPLY_TYPE)

        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            del self._rpcs[(dest, msg_id)]

    def handle_init(self, msg: InitMessage) -> InitOkMessage:
        assert not hasattr(self, "node_id"), "Already initialized"
        self.node_id = msg.node_id
        self.all_node_ids = msg.node_ids
        return InitOkMessage()

    @classmethod
    async def process_streams(
        cls,
        node: AsyncNode,
        stream_in = sys.stdin,
        stream_out = sys.stdout,
    ):
        reader = await connect_input_stream(stream_in)
        writer = await connect_output_stream(stream_out)

        async def send_msg(msg: EnvelopeMessage):
            writer.write(msg.json().encode("utf-8") + b"\n")
            await writer.drain()
        node.set_send_msg(send_msg)

        try:
            async with asyncio.TaskGroup() as tg:
                node.set_task_group(tg)
                async for line in reader:
                    msg = EnvelopeMessage.parse_raw(line)
                    tg.create_task(node.receive_msg(msg))

        finally:
            node.set_send_msg(None)
