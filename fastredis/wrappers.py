"""Low-level wrappers around the exposed hiredis API."""

from typing import AnyStr, Union

from fastredis.exceptions import *
import fastredis.hiredis as hiredis
from fastredis.hiredis import (
    REDIS_REPLY_STRING,
    REDIS_REPLY_ARRAY,
    REDIS_REPLY_INTEGER,
    REDIS_REPLY_NIL,
    REDIS_REPLY_STATUS,
    REDIS_REPLY_ERROR
)


ReplyValue = Union[AnyStr, int, tuple, None]


def get_reply_value(rep: hiredis.redisReply) -> ReplyValue:
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
        return tuple(map(serialize_reply, rep.elements))
    elif rep.type == REDIS_REPLY_ERROR:
        raise ReplyError
    else:
        raise FastredisError(f'Invalid reply type: {rep.type}')


def redis_connect(
        ip: str,
        port: int = 6379,
        timeout: Optional[float] = None
    ) -> hiredis.redisContext:
    """Connects to redis.

    Wrapper around hiredis.redisConnect().
    Raises:
        * ContextError (any type)
    In the event of a timeout, the subclass of ContextError raised is IOError.
    """

    if timeout is None:
        context = hiredis.redisConnect(ip, port)
    else:
        timeval = hiredis.timeval()
        timeval.tv_sec = int(timeout)
        timeval.tv_usec = (timeout - int(timeout)) * (10 ** 6)
        context = hiredis.redisConnectWithTimeout(ip, port, timeval)
    raise_context_error(context)
    return context


def redis_free(context: hiredis.redisContext) -> None:
    """Frees the redis context object.

    Necessary for preventing memory leaks.
    """

    hiredis.redisFree(context)


def redis_command(
        context: hiredis.redisContext,
        command: str
    ) -> ReplyValue:
    """Sends the command and retrieves the response (write and read).

    Wrapper around hiredis.redisCommand().
    Raises:
        * HiredisError (any type)
        * FastredisError
    """

    rep = hiredis.redisCommand(context, command)
    raise_reply_error(context, rep)
    ret = get_reply_value(rep)
    hiredis.freeReplyObject(rep)
    return ret


def redis_write(context: hiredis.redisContext, command: str) -> None:
    """Writes a command to the redis send buffer.

    Wrapper around hiredis.redisAppendCommand().
    Raises:
        * ContextError (any type)
    """

    if hiredis.redisAppendCommand(context, command) == hiredis.REDIS_ERR:
        raise_context_error(context)
        raise ContextError('redisAppendCommand error and no error code is set.')


def redis_read(context: hiredis.redisContext) -> ReplyValue:
    """Reads a reply value from the redis receive buffer.

    Wrapper around hiredis.redisGetReply().
    Raises:
        * HiredisError (any type)
        * FastredisError
    """

    out = hiredis.redisReplyOut()
    hiredis.redisGetReplyOL(context, out)
    if out.ret == hiredis.REDIS_ERR:
        raise_context_error(context)
        raise ContextError('redisGetReply error and no error code is set.')
    ret = get_reply_value(out.reply)
    hiredis.freeReplyObject(out.reply)
    return ret
