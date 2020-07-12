"""Connection class for a synchronous client."""

import asyncio
from abc import ABC, abstractmethod
from functools import wraps
from typing import Optional

import fastredis.wrappers as wrappers
from fastredis.wrappers import ReplyValue
import fastredis.wrappersb as wrappersb
import fastredis.wrappers_async as wa


class SyncConnection(ABC):

    def __init__(self,
            ip: bytes,
            port: int = 6379,
            connect_timeout: float = None
        ):
        self.ip = ip
        self.port = port
        self.connect_timeout = connect_timeout

        self.context = None


    @abstractmethod
    def _redis_connect():
        pass
    @abstractmethod
    def _redis_free():
        pass
    @abstractmethod
    def _redis_command():
        pass
    @abstractmethod
    def _redis_write():
        pass
    @abstractmethod
    def _redis_read():
        pass

    def connect(self) -> None:
        """Connect to redis.

        If already connected, disconnect first.

        This method raises ContextError (of any type) on errors. If a
        connection timeout was specified, and connecting times out, then
        IOError (a subclass of ContextError) will be raised.
        """

        if self.context is not None:
            self.disconnect()

        self.context = self._redis_connect(
            ip=self.ip,
            port=self.port,
            timeout=self.connect_timeout
        )

    def disconnect(self) -> None:
        """Disconnect from redis.

        This call is idempotent.
        """

        if self.context is None:
            return
        self._redis_free(self.context)
        self.context = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def command(self, command: str) -> ReplyValue:
        """Send a command to redis and retrieve the reply.

        Raises:
            * Any type of HiredisError
                * ReplyError if the server replies with an error
                * ContextError if there are connection issues
            * FastredisError if invalid response types
        """

        return self._redis_command(context=self.context, command=command)

    def write(self, command: str) -> None:
        """Write a command to the send buffer, but do not send yet.

        Use this for pipelining. To flush the buffer and actually send its
        contents, call read() or command().

        Raises:
            * ContextError (any type)
        """

        self._redis_write(self.context, command)

    def read(self) -> ReplyValue:
        """Flush the send buffer and read a reply from the receive buffer.

        If no message in the buffer, block until one arrives.

        Raises:
            * HiredisError (any type)
            * FastredisError
        """

        return self._redis_read(self.context)


def makemethod(func):
    @wraps(func)
    def method(self, *args, **kwargs):
        return func(*args, **kwargs)
    return method


class SyncConnectionStr(SyncConnection):

    def __init__(self,
            ip: str,
            port: int = 6379,
            connect_timeout: float = None,
        ):
        self.ip = ip
        self.port = port
        self.connect_timeout = connect_timeout

        self.context = None

    _redis_connect = makemethod(wrappers.redis_connect)
    _redis_free = makemethod(wrappers.redis_free)
    _redis_command = makemethod(wrappers.redis_command)
    _redis_write = makemethod(wrappers.redis_write)
    _redis_read = makemethod(wrappers.redis_read)


class SyncConnectionBytes(SyncConnection):

    def __init__(self,
            ip: bytes,
            port: int = 6379,
            connect_timeout: float = None
        ):
        self.ip = ip
        self.port = port
        self.connect_timeout = connect_timeout

        self.context = None

    _redis_connect = makemethod(wrappersb.redis_connect)
    _redis_free = makemethod(wrappersb.redis_free)
    _redis_command = makemethod(wrappersb.redis_command)
    _redis_write = makemethod(wrappersb.redis_write)
    _redis_read = makemethod(wrappersb.redis_read)


def SyncConnection(*args, **kwargs):
    """Create a synchronous connection object."""

    if 'encoding' in kwargs:
        encoding = kwargs['encoding']
        del kwargs['encoding']
        if encoding is not None:
            encoding = encoding.lower()
    else:
        encoding = 'utf-8'

    if encoding == 'utf-8':
        return SyncConnectionStr(*args, **kwargs)
    elif encoding is None:
        return SyncConnectionBytes(*args, **kwargs)
    else:
        raise ValueError('`encoding` must be "utf-8" or None')


class AsyncConnectionStr:

    def __init__(self,
            ip: str,
            port: int = 6379,
            connect_timeout: float = None
        ):
        self.ip = ip
        self.port = port
        self.connect_timeout = connect_timeout

        self.context = None

    async def connect(self) -> None:
        """Connect to redis.

        If already connected, disconnect first.

        This method raises ContextError (of any type) on errors. If a
        connection timeout was specified, and connecting times out, then
        IOError (a subclass of ContextError) will be raised.
        """

        loop = asyncio.get_event_loop()

        if self.context is not None:
            await self.disconnect()

        self.context, self.not_garbage = wa.redis_connect(
            ip=self.ip,
            port=self.port
        )
        connected = loop.create_future()
        self.disconnected = loop.create_future()

        def connected_cb(_context, status):
            if status != wa.REDIS_OK:
                try:
                    raise_context_error(self.context)
                except Exception as e:
                    connected.set_exception(e)
                    return
            connected.set_result(True)

        def disconnected_cb(_context, status):
            if status != wa.REDIS_OK:
                try:
                    raise_context_error(self.context)
                except Exception as e:
                    self.disconnected.set_exception(e)
                    return
            self.disconnected.set_result(True)

        self.not_garbage += wa.redis_set_connect_cb(self.context, connected_cb)
        self.not_garbage += wa.redis_set_disconnect_cb(self.context, disconnected_cb)

        done, pending = await asyncio.wait(
            {connected},
            timeout=self.connect_timeout
        )
        if len(done) == 1:
            return

        raise FastredisError('timeout exceeded while connecting')

    async def disconnect(self, timeout=None) -> None:
        """Disconnect from redis.

        This call is idempotent.
        """

        if self.context is None:
            return

        if timeout is not None and timeout < 0:
            raise ValueError('timeout must be nonnegative or None')

        if (timeout is None) or (timeout is not None and timeout > 0):
            # attempt a graceful disconnect first
            wa.redis_disconnect(self.context)
            done, pending = await asyncio.wait(
                {self.disconnected},
                timeout=timeout
            )
            if len(done) > 0:
                self.context = None
                return

        # timeout = 0 or time ran up, disconnect now
        wa.redis_free(self.context)
        self.context = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.disconnect()

    async def command(self, command: str) -> ReplyValue:
        """Send a command to redis and retrieve the reply.

        Raises:
            * Any type of HiredisError
                * ReplyError if the server replies with an error
                * ContextError if there are connection issues
            * FastredisError if invalid response types
        """

        return await wa.redis_command(
            context=self.context,
            command=command
        )


def AsyncConnection(*args, **kwargs):
    """Create an asynchronous connection object."""

    if 'encoding' in kwargs:
        encoding = kwargs['encoding']
        del kwargs['encoding']
        if encoding is not None:
            encoding = encoding.lower()
    else:
        encoding = 'utf-8'

    if encoding == 'utf-8':
        return AsyncConnectionStr(*args, **kwargs)
    elif encoding is None:
        raise NotImplementedError
        # return AsyncConnectionBytes(*args, **kwargs)
    else:
        raise ValueError('`encoding` must be "utf-8" or None')
