from __future__ import annotations
import sys
from typing import Callable, ClassVar, Dict, Iterable, Type
from pydantic import BaseModel


class EnvelopeMessage(BaseModel):
    src: str
    dest: str
    body: dict


class Message(BaseModel, underscore_attrs_are_private = True):
    TYPE: ClassVar[str]
    _src: str | None = None
    _dest: str | None = None

    msg_id: int | None = None
    in_reply_to: int | None = None

    def reply(self, msg: Message) -> Message:
        msg.in_reply_to = self.msg_id
        msg._dest = self._src
        return msg


class ErrorMessage(Message):
    """ Returned by the node when it cannot service a request """
    TYPE = "error"
    code: int  # 0=timeout, .. see https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md
    text: str


class Node:
    def __init__(self, node_id: str, all_node_ids: list[str]):
        self.node_id = node_id
        self.all_node_ids = all_node_ids
        self.next_msg_id = 1
        self._msg_types = {
            msg_class.TYPE: msg_class
            for msg_class in Message.__subclasses__()
        }

    def process_msgs(self, msgs: Iterable[Message]) -> Iterable[Message]:
        raise NotImplementedError

    def unwrap_msg(self, msg: EnvelopeMessage) -> Message:
        msg_type = msg.body.pop("type")
        msg_cls = self._msg_types[msg_type]
        body = msg_cls.parse_obj(msg.body)
        body._src = msg.src
        assert msg.dest == self.node_id
        return body

    def wrap_msg(self, msg: Message) -> EnvelopeMessage:
        msg.msg_id = self.next_msg_id
        self.next_msg_id += 1
        assert msg._dest is not None, f"Message must have a destination: {msg}"
        body = msg.dict(exclude_none=True)
        body["type"] = msg.TYPE
        return EnvelopeMessage(
            src=self.node_id,
            dest=msg._dest,
            body=body,
        )

    def process_wrapped_msgs(self, msgs: Iterable[EnvelopeMessage]) -> Iterable[EnvelopeMessage]:
        return map(self.wrap_msg, self.process_msgs(map(self.unwrap_msg, msgs)))


def process_init_msg(msg: EnvelopeMessage) -> tuple[str, list[str], EnvelopeMessage]:
    assert msg.body["type"] == "init"
    msg_id = msg.body["msg_id"]
    node_id = msg.body["node_id"]
    node_ids = msg.body["node_ids"]
    assert node_id == msg.dest
    ack_msg = EnvelopeMessage(
        src=node_id,
        dest=msg.src,
        body={"type": "init_ok", "msg_id": 0, "in_reply_to": msg_id},
    )
    return node_id, node_ids, ack_msg

def process_streams(node_class: Callable[[str, list[str]], Node], stream_in = sys.stdin, stream_out = sys.stdout):
    msgs = map(EnvelopeMessage.parse_raw, stream_in)

    node_id, node_ids, init_reply = process_init_msg(next(msgs))
    str = init_reply.json(exclude_none=True)
    print(str, file=sys.stderr, flush=True)
    print(str, file=stream_out, flush=True)

    node = node_class(node_id, node_ids)
    for msg in node.process_wrapped_msgs(msgs):
        print(msg.json(exclude_none=True), file=stream_out, flush=True)
