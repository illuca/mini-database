// bufpool.h ... buffer pool interface

#include "db.h"

#define MAXID 20

typedef struct bufPool {
    int nbufs;         // how many buffers
    char* strategy;      // LRU, MRU, Cycle
    int nrequests;     // stats counters
    int nreleases;
    int nhits;
    int nreads;
    int nwrites;
    //先进先出，freeList是一个队列
    int* freeList;     // list of completely unused bufs
    int nfree;
    int* usedList;     // implements replacement strategy
    int nused;
    struct buffer* bufs;
} bufPool;

// one buffer
typedef struct buffer {
    char id[MAXID];
    int pin;
    int usage;
    UINT oid;
    int page_index;
    UINT64 page_id;
    char* page;
} buffer;

typedef struct bufPool* BufPool;

BufPool initBufPool(int, char*);

int request_page(BufPool, UINT oid, int page_index, char* page);

void release_page(BufPool, UINT oid, int);

void showPoolUsage(BufPool);

void showPoolState(BufPool);

int pageInPool(BufPool, UINT, int);

BufPool get_bp();