"""Low-level wrappers around the exposed hiredis API."""

from fastredis.exceptions import *
import fastredis.hiredis as hiredis
from fastredis.wrapper_tools import (
    get_reply_value,
    ReplyValue,
    convert_reply_array
)


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
    convert_reply_array(rep)
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

    if hiredis.redisAppendCommand(context, command) == REDIS_ERR:
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
    if out.ret == REDIS_ERR:
        raise_context_error(context)
        raise ContextError('redisGetReply error and no error code is set.')
    rep = out.reply
    raise_reply_error(context, rep)
    convert_reply_array(rep)
    ret = get_reply_value(rep)
    hiredis.freeReplyObject(rep)
    return ret
