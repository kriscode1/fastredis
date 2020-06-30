"""Connection class for a synchronous client."""

from fastredis.wrappers import (
    redis_connect,
    redis_free,
    redis_command,
    ReplyValue,
    redis_read,
    redis_write
)

class Connection:

    pass


class SyncConnection:

    def __init__(self,
            ip: str,
            port: int = 6379,
            connect_timeout: float = None
        ):
        self.ip = ip
        self.port = port
        self.connect_timeout = connect_timeout

        self.context = None

    def connect(self) -> None:
        """Connect to redis.

        If already connected, disconnect first.

        This method raises ContextError (of any type) on errors. If a
        connection timeout was specified, and connecting times out, then
        IOError (a subclass of ContextError) will be raised.
        """

        if self.context is not None:
            self.disconnect()

        self.context = redis_connect(
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
        redis_free(self.context)
        self.context = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def command(self, command: str) -> ReplyValue:
        """Sends a command to redis and retrieves the reply.

        Raises:
            * Any type of HiredisError
                * ReplyError if the server replies with an error
                * ContextError if there are connection issues
            * FastredisError if invalid response types
        """

        return redis_command(context=self.context, command=command)

    def write(self, command: str) -> None:
        """Writes a command to the send buffer.

        Use this for pipelining. To send the buffer, call read() or command().

        Raises:
            * ContextError (any type)
        """

        redis_write(self.context, command)

    def read(self) -> ReplyValue:
        """Reads a reply from the receive buffer.

        If no message in the buffer, block until one arrives.

        Raises:
            * HiredisError (any type)
            * FastredisError
        """

        return redis_read(self.context)


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
