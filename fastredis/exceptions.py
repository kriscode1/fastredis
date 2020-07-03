"""Exceptions to raise instead of hiredis error codes.

The exception heirarchy is:

Excpetion
    FastredisError
        HiredisError
            ReplyError (REDIS_REPLY_ERROR)
            ContextError (NULL or REDIS_ERR)
                IOError (REDIS_ERR_IO)
                EOFError (REDIS_ERR_EOF)
                ProtocolError (REDIS_ERR_PROTOCOL)
                OtherError (REDIS_ERR_OTHER)

"""

from typing import Optional

from fastredis.hiredis import (
    REDIS_ERR_IO,
    REDIS_ERR_OTHER,
    REDIS_ERR_EOF,
    REDIS_ERR_PROTOCOL,
    REDIS_ERR_OOM,
    REDIS_REPLY_STRING,
    REDIS_REPLY_ARRAY,
    REDIS_REPLY_INTEGER,
    REDIS_REPLY_NIL,
    REDIS_REPLY_STATUS,
    REDIS_REPLY_ERROR
)


class FastredisError(Exception):
    """Parent of all exceptions raised from fastredis."""
    pass

class HiredisError(FastredisError):
    """Representing error codes returned directly from hiredis."""
    pass

class ReplyError(HiredisError):
    """Raised on REDIS_REPLY_ERROR."""
    pass

class ContextError(HiredisError):
    """Raised on NULL or REDIS_ERR."""
    pass

class IOError(ContextError):
    """Raised on REDIS_ERR_IO."""
    pass

class EOFError(ContextError):
    """Raised on REDIS_ERR_EOF."""
    pass

class ProtocolError(ContextError):
    """Raised on REDIS_ERR_PROTOCOL."""
    pass

class OtherError(ContextError):
    """Raised on REDIS_ERR_OTHER."""
    pass

class OutOfMemoryError(ContextError):
    """Raised on REDIS_ERR_OOM."""
    pass


def raise_context_error(context) -> None:
    """Raises an exception if `context` contains an error."""

    # print('context is', context)
    if context is None:
        # print('empty context')
        raise ContextError('Empty context')
    if context.err == 0:
        # print('no error')
        return
    elif context.err == REDIS_ERR_IO:
        raise IOError
    elif context.err == REDIS_ERR_EOF:
        raise EOFError
    elif context.err == REDIS_ERR_PROTOCOL:
        raise ProtocolError
    elif context.err == REDIS_ERR_OTHER:
        raise OtherError
    elif context.err == REDIS_ERR_OOM:
        raise OutOfMemoryError
    else:
        raise ContextError(f'Unknown error: {context.err}')


def raise_reply_error(
        context,
        reply
    ) -> None:
    """Raises an excpetion if `reply` or `context` contain an error."""

    if reply is None:
        raise_context_error(context)
        raise ContextError('Reply is empty and no error code is set.')
    if reply.type == REDIS_REPLY_ERROR:
        raise ReplyError(reply.str)
