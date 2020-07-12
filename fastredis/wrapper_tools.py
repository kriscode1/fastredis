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



def convert_reply_array(rep: hiredis.redisReply) -> None:
    """Converts redisReply.element ** into python objects.

    The result is stored in rep.pyelements.
    """

    if rep.type != REDIS_REPLY_ARRAY or rep.elements == 0:
        rep.pyelements = tuple()
        return
    rep.pyelements = tuple(
        hiredis.replies_index(rep.element, i) for i in range(rep.elements)
    )


def convert_reply_array_b(rep: hiredisb.redisReply_b) -> None:
    """Converts redisReply.element ** into python objects.

    The result is stored in rep.pyelements.
    """

    if rep.type != REDIS_REPLY_ARRAY or rep.elements == 0:
        rep.pyelements = tuple()
        return
    rep.pyelements = tuple(
        hiredisb.replies_index_b(rep.element, i) for i in range(rep.elements)
    )


def get_reply_value(rep: AnyReply) -> ReplyValue:
    """Gets the contents of a reply or raises the appropriate error.

    Raises:
        * ReplyError (Redis server is returning an error)
        * FastredisError (Unknown reply type)
    """

    if rep.type == REDIS_REPLY_STATUS:
        return rep.str
    elif rep.type == REDIS_REPLY_STRING:
        return rep.str
    elif rep.type == REDIS_REPLY_INTEGER:
        return rep.integer
    elif rep.type == REDIS_REPLY_NIL:
        return None
    elif rep.type == REDIS_REPLY_ARRAY:
        return tuple(get_reply_value(subr) for subr in rep.pyelements)
    elif rep.type == REDIS_REPLY_ERROR:
        raise ReplyError
    else:
        raise FastredisError(f'Invalid reply type: {rep.type}')
