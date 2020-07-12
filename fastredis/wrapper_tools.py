from typing import AnyStr, Union

from fastredis.exceptions import *
import fastredis.hiredis as hiredis
import fastredis.hiredisb as hiredisb
from fastredis.hiredis import (
    REDIS_REPLY_STRING,
    REDIS_REPLY_ARRAY,
    REDIS_REPLY_INTEGER,
    REDIS_REPLY_NIL,
    REDIS_REPLY_STATUS,
    REDIS_REPLY_ERROR
)


ReplyValue = Union[AnyStr, int, tuple, None]
AnyReply = Union[hiredis.redisReply, hiredisb.redisReply_b]


def reduce_reply(rep: hiredis.redisReply) -> ReplyValue:
    if rep.type == REDIS_REPLY_STATUS:
        return rep.str
    elif rep.type == REDIS_REPLY_STRING:
        return rep.str
    elif rep.type == REDIS_REPLY_INTEGER:
        return rep.integer
    elif rep.type == REDIS_REPLY_NIL:
        return None
    elif rep.type == REDIS_REPLY_ARRAY:
        return tuple(
            reduce_reply(hiredis.replies_index(rep.element, i))
            for i in range(rep.elements)
        )
    elif rep.type == REDIS_REPLY_ERROR:
        raise ReplyError(rep.str)
    else:
        raise FastredisError(f'Invalid reply type: {rep.type}')


def reduce_reply_b(rep: hiredisb.redisReply_b) -> ReplyValue:
    if rep.type == REDIS_REPLY_STATUS:
        return rep.str
    elif rep.type == REDIS_REPLY_STRING:
        return rep.str
    elif rep.type == REDIS_REPLY_INTEGER:
        return rep.integer
    elif rep.type == REDIS_REPLY_NIL:
        return None
    elif rep.type == REDIS_REPLY_ARRAY:
        return tuple(
            reduce_reply(hiredisb.replies_index_b(rep.element, i))
            for i in range(rep.elements)
        )
    elif rep.type == REDIS_REPLY_ERROR:
        raise ReplyError(rep.str)
    else:
        raise FastredisError(f'Invalid reply type: {rep.type}')
