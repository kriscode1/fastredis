"""Connection class for a synchronous client."""

from abc import ABC, abstractmethod
from functools import wraps
from typing import Optional

import fastredis.wrappers as wrappers
from fastredis.wrappers import ReplyValue
import fastredis.wrappersb as wrappersb


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
        raise NotImplementedError('`encoding` must be "utf-8" or None')


class AsyncConnection:

    def __init__(self,
            ip: str,
            port: int = 6379,
            connect_timeout: float = None
        ):
        raise NotImplementedError

    async def connect(self):
        raise NotImplementedError

    async def disconnect(self):
        raise NotImplementedError

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.disconnect()

    async def command(self, command: str) -> ReplyValue:
        raise NotImplementedError
