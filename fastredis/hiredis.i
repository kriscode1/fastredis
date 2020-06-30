// SWIG mapping for hiredis version 0.14.0

%module hiredis
%{
#include <hiredis.h>
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
