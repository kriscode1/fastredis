"""Low-level wrappers around the exposed hiredis API."""

import asyncio
import ctypes
from typing import Any, AnyStr, Callable, Tuple, Union

from fastredis.exceptions import *
import fastredis.hiredis as hiredis
from fastredis.hiredis import REDIS_OK
from fastredis.wrapper_tools import (
    get_reply_value,
    convert_reply_array,
    ReplyValue
)


def copy_reply(reply: hiredis.redisReply) -> hiredis.redisReply:
    new = hiredis.redisReply()
    new.type = reply.type
    new.integer = reply.integer
    new.len = reply.len
    new.str = reply.str
    new.elements = reply.elements
    new.element = reply.element
    return new


def redis_connect(
        ip: str,
        port: int = 6379,
    ) -> Tuple[hiredis.redisAsyncContext, list]:
    """Initiate an asynchronous connection to redis.

    Wrapper around hiredis.asyncRedisConnect(). This returns before the
    connection is established. To determine when redis is connected, set a
    connected callback. The second element returned is a list of callback
    objects that should not be garbage collected while the underlying
    hiredis library is still using them.

    Raises:
        * ContextError (any type)
    """

    loop = asyncio.get_event_loop()
    context = hiredis.redisAsyncConnect(ip, port)
    raise_context_error(context)
    create_callback = ctypes.CFUNCTYPE(None, ctypes.c_void_p)

    def fd_ready_for_read():
        hiredis.redisAsyncHandleRead(context)

    def fd_ready_for_write():
        hiredis.redisAsyncHandleWrite(context)

    def addread(privdata):
        loop.add_reader(context.c.fd, fd_ready_for_read)

    def delread(privdata):
        loop.remove_reader(context.c.fd)

    def addwrite(privdata):
        loop.add_writer(context.c.fd, fd_ready_for_write)

    def delwrite(privdata):
        loop.remove_writer(context.c.fd)

    def cleanup(privdata):
        delwrite(None)
        delread(None)

    def get_cb_ptr(cb):
        c_cb = create_callback(cb)
        ptr = ctypes.cast(c_cb, ctypes.c_void_p).value
        return c_cb, ptr

    arcb, context.ev.addRead = get_cb_ptr(addread)
    drcb, context.ev.delRead = get_cb_ptr(delread)
    awcb, context.ev.addWrite = get_cb_ptr(addwrite)
    dwcb, context.ev.delWrite = get_cb_ptr(delwrite)
    cucb, context.ev.cleanup = get_cb_ptr(cleanup)

    return context, [
        addread,
        delread,
        addwrite,
        delwrite,
        cleanup,
        arcb,
        drcb,
        awcb,
        dwcb,
        cucb
    ]


def redis_set_connect_cb(
        context: hiredis.redisAsyncConnect,
        cb: Callable[[Any, int], None]
    ) -> list:
    """Sets a callback to run after the connection is established.

    Wrapper around hiredis.redisAsyncSetConnectCallback(). Returns a set of
    callback objects that should not be garbage collected while the underlying
    hiredis library is still using them.

    Raises:
        * ContextError (any type)
    """

    cb_c = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_int)(cb)
    cb_ptr = ctypes.cast(cb_c, ctypes.c_void_p).value

    if hiredis.redisAsyncSetConnectCallback(context, cb_ptr) == REDIS_ERR:
        raise_context_error(context)
        raise ContextError('Failed to set connect callback but no error code is set.')

    return [cb_c, cb]


def redis_set_disconnect_cb(
        context: hiredis.redisAsyncConnect,
        cb: Callable[[Any, int], None]
    ) -> list:
    """Sets a callback to run after the connection is disconnected.

    Wrapper around hiredis.redisAsyncSetDisconnectCallback(). Returns a set of
    callback objects that should not be garbage collected while the underlying
    hiredis library is still using them.

    Raises:
        * ContextError (any type)
    """

    cb_c = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_int)(cb)
    cb_ptr = ctypes.cast(cb_c, ctypes.c_void_p).value

    if hiredis.redisAsyncSetDisconnectCallback(context, cb_ptr) == REDIS_ERR:
        raise_context_error(context)
        raise ContextError('Failed to set disconnect callback but no error code is set.')

    return [cb_c, cb]


def redis_disconnect(context: hiredis.redisAsyncConnect) -> None:
    """Sends all pending commands, waits for their replies, and disconnects.

    This also cleans up the context object, so a call to
    hiredis.redisAsyncFree() should not be made.
    Wrapper around hiredis.redisAsyncDisconnect().
    """

    hiredis.redisAsyncDisconnect(context)


def redis_free(context: hiredis.redisAsyncConnect) -> None:
    """Frees the redis context object.

    Forces a disconnection if not disconnected already.
    Wrapper around hiredis.redisAsyncFree().
    """

    hiredis.redisAsyncFree(context)


def convert_and_copy_reply_array(rep):
    """Recursively converts nested reply arrays into reply object copies."""

    rep = copy_reply(rep)
    convert_reply_array(rep)
    if len(rep.pyelements) > 0:
        rep.pyelements = tuple(
            convert_and_copy_reply_array(subr) for subr in rep.pyelements
        )
    return rep


async def redis_command(
        context: hiredis.redisAsyncContext,
        command: str
    ) -> ReplyValue:
    """Sends the command and retrieves the response.

    Wrapper around hiredis.redisAsyncCommand().
    Raises:
        * HiredisError (any type)
        * FastredisError
    """

    reply_fut = asyncio.get_event_loop().create_future()
    def reply_cb(reply):
        try:
            reply = hiredis.castRedisReply(reply)
            # A copy is required because hiredis deletes the reply after this
            # callback is finished.
            reply_fut.set_result(convert_and_copy_reply_array(reply))
        except Exception as e:
            reply_fut.set_exception(e)

    c_cb = ctypes.CFUNCTYPE(None, ctypes.c_void_p)(reply_cb)
    ptr = ctypes.cast(c_cb, ctypes.c_void_p).value

    status = hiredis.redisAsyncCommandOL(context, command, ptr)
    if status != hiredis.REDIS_OK:
        raise_context_error(context)
        raise ContextError('Cannot add command to write queue.')

    reply = await reply_fut
    raise_reply_error(context, reply)
    return get_reply_value(reply)
