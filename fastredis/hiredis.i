// SWIG mapping for hiredis version 0.14.0

%module hiredis
%{
#include <hiredis.h>
#include <async.h>
#include <sys/time.h> // struct timeval
%}

struct timeval {
    long tv_sec;
    long tv_usec;
};

// Error codes
#define REDIS_ERR -1
#define REDIS_OK 0
#define REDIS_ERR_IO 1
#define REDIS_ERR_OTHER 2
#define REDIS_ERR_EOF 3
#define REDIS_ERR_PROTOCOL 4
#define REDIS_ERR_OOM 5

// redisReply types
#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

typedef struct redisReply {
    int type;
    long long integer;
    size_t len;
    char* str;
    size_t elements;
    struct redisReply** element;
} redisReply;

%inline {
redisReply* replies_index(redisReply** replies, size_t index) {
    return replies[index];
}
}

typedef struct redisContext {
    int err;
    char errstr[128];
    int fd;
    int flags;
    char* obuf;
    redisReader* reader;
    enum redisConnectionType connection_type;
    struct timeval* timeout;
    struct {
        char* host;
        char* source_addr;
        int port;
    } tcp;
    struct {
        char* path;
    } unix_sock;
} redisContext;


redisContext* redisConnect(const char* ip, int port);
redisContext* redisConnectWithTimeout(
    const char* ip,
    int port,
    const struct timeval tv
);
redisReply* redisCommand(redisContext* c, const char* format);
void freeReplyObject(redisReply* reply);
void redisFree(redisContext* c);
int redisAppendCommand(redisContext* c, const char* format);

%inline {
struct redisReplyOut {
    redisReply* reply;
    int ret;
};

// Overloaded redisGetReply() to be more python friendly. Let python create
// the redisReplyOut object to fill in, so python can do the cleanup.
// freeReplyObject() still needs called for the pointed reply.
void redisGetReplyOL(redisContext* c, struct redisReplyOut* out) {
    out->ret = redisGetReply(c, (void**)&out->reply);
}
}


/****************************
 * Async mappings
 ****************************/


//typedef void (redisDisconnectCallback)(const struct redisAsyncContext*, int status);
//typedef void (redisConnectCallback)(const struct redisAsyncContext*, int status);
%typemap(in) void (*)(const struct redisAsyncContext*, int) {
    $1 = (void (*)(const struct redisAsyncContext*, int))PyLong_AsVoidPtr($input);
}

// For setting redisAsyncContext.ev members
%typemap(in) void (*)(void*) {
    $1 = (void (*)(void*))PyLong_AsVoidPtr($input);
}

//typedef void (redisCallbackFn)(struct redisAsyncContext*, void*, void*);
%typemap(in) void (*)(struct redisAsyncContext*, void*, void*) {
    $1 = (void (*)(struct redisAsyncContext*, void*, void*))PyLong_AsVoidPtr($input);
}

typedef struct redisAsyncContext {
    redisContext c;
    int err;
    char* errstr;
    void* data;
    struct {
        void* data;
        void (*addRead)(void* privdata);
        void (*delRead)(void* privdata);
        void (*addWrite)(void* privdata);
        void (*delWrite)(void* privdata);
        void (*cleanup)(void* privdata);
    } ev;
    redisDisconnectCallback* onDisconnect;
    redisConnectCallback* onConnect;
    redisCallbackList replies;
    struct {
        redisCallbackList invalid;
        struct dict* channels;
        struct dict* patterns;
    } sub;
} redisAsyncContext;

redisAsyncContext* redisAsyncConnect(const char* ip, int port);
void redisAsyncDisconnect(redisAsyncContext* ac);
void redisAsyncFree(redisAsyncContext* ac);
int redisAsyncSetConnectCallback(redisAsyncContext* ac, void (*fn)(const struct redisAsyncContext*, int));
int redisAsyncSetDisconnectCallback(redisAsyncContext* ac, void (*fn)(const struct redisAsyncContext*, int));

%inline {
void redisAsyncCommandCBWrapper(
    struct redisAsyncContext* ac, void* reply, void* privdata
) {
    void (*cb)(redisReply*) = (void (*)(redisReply*))privdata;
    if (reply != NULL) {
        cb((redisReply*)reply);
    }
}
int redisAsyncCommandOL(
    redisAsyncContext * ac,
    const char* command,
    void (*cb)(void*)
) {
    /* void (*cb)(void*) is really void (*cb)(redisReply*), but casting makes
    using ctypes easier. This overload is to pass the callback as privdata, and
    use redisAsyncCommandCBWrapper() as the real callback.
    */
    return redisAsyncCommand(ac, redisAsyncCommandCBWrapper, cb, command);
}
redisReply* castRedisReply(unsigned long long reply_ptr) {
    /* Cast a ptr to a redisReply object.

    This is intended to be called from python inside an async command callback
    so swig does the work of constructing a redisReply object. The parameter is
    unsigned long long because ctypes gives an integer value.
    */
    return (redisReply*)reply_ptr;
}
}

void redisAsyncHandleRead(redisAsyncContext* ac);
void redisAsyncHandleWrite(redisAsyncContext* ac);
