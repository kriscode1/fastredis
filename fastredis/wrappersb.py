"""Low-level wrappers around the exposed hiredis API."""

from typing import AnyStr, Union

from fastredis.exceptions import *
import fastredis.hiredis as hiredis
import fastredis.hiredisb as hiredisb
from fastredis.wrapper_tools import (
    get_reply_value,
    ReplyValue,
    convert_reply_array_b
)


def redis_connect(
        ip: bytes,
        port: int = 6379,
        timeout: Optional[float] = None
    ) -> hiredisb.redisContext_b:
    """Connects to redis.

    Wrapper around hiredisb.redisConnect_b().
    Raises:
        * ContextError (any type)
    In the event of a timeout, the subclass of ContextError raised is IOError.
    """

    if timeout is None:
        context = hiredisb.redisConnect_b(ip, port)
    else:
        timeval = hiredis.timeval()
        timeval.tv_sec = int(timeout)
        timeval.tv_usec = (timeout - int(timeout)) * (10 ** 6)
        context = hiredisb.redisConnectWithTimeout_b(ip, port, timeval)
    raise_context_error(context)
    return context


def redis_free(context: hiredisb.redisContext_b) -> None:
    """Frees the redis context object.

    Necessary for preventing memory leaks.
    """

    hiredisb.redisFree_b(context)


def redis_command(
        context: hiredisb.redisContext_b,
        command: bytes
    ) -> ReplyValue:
    """Bytes version of redis_command().

    Wrapper around hiredisb.redisCommand_b().
    """

    rep = hiredisb.redisCommand_b(context, command)
    raise_reply_error(context, rep)
    convert_reply_array_b(rep)
    ret = get_reply_value(rep)
    hiredisb.freeReplyObject_b(rep)
    return ret


def redis_write(context: hiredisb.redisContext_b, command: bytes) -> None:
    """Bytes version of redis_write().

    Wrapper around hiredisb.redisAppendCommand_b().
    """

    if hiredisb.redisAppendCommand_b(context, command) == REDIS_ERR:
        raise_context_error(context)
        raise ContextError('redisAppendCommand error and no error code is set.')


def redis_read(context: hiredisb.redisContext_b) -> ReplyValue:
    """Bytes version of redis_read().

    Wrapper around hiredisb.redisGetReplyOL()."""

    out = hiredisb.redisReplyOut_b()
    hiredisb.redisGetReplyOL_b(context, out)
    if out.ret == REDIS_ERR:
        raise_context_error(context)
        raise ContextError('redisGetReply error and no error code is set.')
    rep = out.reply
    raise_reply_error(context, rep)
    convert_reply_array_b(rep)
    ret = get_reply_value(rep)
    hiredisb.freeReplyObject_b(rep)
    return ret
