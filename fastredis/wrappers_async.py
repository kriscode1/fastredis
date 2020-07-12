"""Low-level wrappers around the exposed hiredis API."""

import asyncio
import ctypes
from typing import Any, AnyStr, Callable, Tuple, Union

from fastredis.exceptions import *
import fastredis.hiredis as hiredis
from fastredis.hiredis import REDIS_OK
from fastredis.wrapper_tools import (
    ReplyValue,
    reduce_reply
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
    fd_cannot_write = False

    def fd_ready_for_read():
        hiredis.redisAsyncHandleRead(context)

    def fd_ready_for_write():
        nonlocal fd_cannot_write
        fd_cannot_write = False
        hiredis.redisAsyncHandleWrite(context)

    def addread(privdata):
        loop.add_reader(context.c.fd, fd_ready_for_read)

    def delread(privdata):
        loop.remove_reader(context.c.fd)

    def addwrite(privdata):
        """Callback ran when hiredis has a message to send.

        `fd_cannot_write` is an optimization. Most of the time, the fd is ready
        for writing and can be written to immediately without churning the
        event loop. When redisAsyncHandleWrite() is not able to write the full
        buffer, it calls this callback function. On the second call of
        addwrite(), the loop writer gets added. Only a successful flush of the
        entire buffer or `fd_ready_for_write()` can set `fd_cannot_write` to
        False.
        """

        nonlocal fd_cannot_write

        if fd_cannot_write:
            loop.add_writer(context.c.fd, fd_ready_for_write)
            return

        fd_cannot_write = True
        hiredis.redisAsyncHandleWrite(context)
        if hiredis.writeBufferLen(context) == 0:
            fd_cannot_write = False

    def delwrite(privdata):
        loop.remove_writer(context.c.fd)

    def cleanup(privdata):
        delwrite(None)
        delread(None)

    def get_cb_ptr(cb):
        c_cb = create_callback(cb)
        ptr = ctypes.cast(c_cb, ctypes.c_void_p).value
        return c_cb, ptr

    # addread() is written as a callback for hiredis, but hiredis does not
    # ever call the delread() callback itself. By adding the loop reader
    # once here, and not passing addread() to hiredis, the number of
    # unnecessary callbacks is greatly reduced.
    addread(None)

    # arcb, context.ev.addRead = get_cb_ptr(addread)
    drcb, context.ev.delRead = get_cb_ptr(delread)
    awcb, context.ev.addWrite = get_cb_ptr(addwrite)
    dwcb, context.ev.delWrite = get_cb_ptr(delwrite)
    cucb, context.ev.cleanup = get_cb_ptr(cleanup)

    return context, [
        # addread,
        delread,
        addwrite,
        delwrite,
        cleanup,
        # arcb,
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


# Helper func for redis_command()
_create_reply_callback = ctypes.CFUNCTYPE(None, ctypes.c_void_p)


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
    def reply_cb(reply: int):
        if reply is None:
            try:
                raise_empty_reply_error(context)
            except Exception as e:
                reply_fut.set_exception(e)
            return
        try:
            reply: hiredis.redisReply = hiredis.castRedisReply(reply)
            # A copy is required because hiredis deletes the reply after this
            # callback is finished. reduce_reply() will return the reply value.
            reply_fut.set_result(reduce_reply(reply))
        except Exception as e:
            reply_fut.set_exception(e)

    c_cb = _create_reply_callback(reply_cb)
    ptr = ctypes.cast(c_cb, ctypes.c_void_p).value

    status = hiredis.redisAsyncCommandOL(context, command, ptr)
    if status != hiredis.REDIS_OK:
        raise_context_error(context)
        raise ContextError('Cannot add command to write queue.')

    return await reply_fut
