// SWIG mapping for hiredis version 0.14.0

%module hiredisb

%begin %{
#define SWIG_PYTHON_STRICT_BYTE_CHAR
%}

%{
#include <hiredis.h>
#include <sys/time.h> // struct timeval
%}


%inline {

typedef struct redisReply_b {
    int type;
    long long integer;
    size_t len;
    char* str;
    size_t elements;
    struct redisReply_b** element;
} redisReply_b;

redisReply_b* replies_index_b(redisReply_b** replies, size_t index) {
    return replies[index];
}

typedef struct redisContext_b {
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
    } tcp_b;
    struct {
        char* path;
    } unix_sock_b;
} redisContext_b;


redisContext_b* redisConnect_b(const char* ip, int port) {
    return (redisContext_b*)redisConnect(ip, port);
}
redisContext_b* redisConnectWithTimeout_b(
    const char* ip,
    int port,
    const struct timeval tv
) {
    return (redisContext_b*)redisConnectWithTimeout(ip, port, tv);
}
redisReply_b* redisCommand_b(redisContext_b* c, const char* format) {
    return (redisReply_b*)redisCommand((redisContext*)c, format);
}
void freeReplyObject_b(redisReply_b* reply) {
    freeReplyObject((redisReply*)reply);
}
void redisFree_b(redisContext_b* c) {
    redisFree((redisContext*)c);
}
int redisAppendCommand_b(redisContext_b* c, const char* format) {
    return redisAppendCommand((redisContext*)c, format);
}

struct redisReplyOut_b {
    redisReply_b* reply;
    int ret;
};

void redisGetReplyOL_b(redisContext_b* c, struct redisReplyOut_b* out) {
    out->ret = redisGetReply((redisContext*)c, (void**)&out->reply);
}

}
