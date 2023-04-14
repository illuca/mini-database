// bufpool.h ... buffer pool interface

#include "db.h"

#define MAXID 20
// one buffer
typedef struct buffer {
    char id[MAXID];
    int pin;
    int usage;
    UINT oid;
    int page_index;
    UINT64 page_id;
    Table* table;
    char* page;
} buffer;

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
    int nvb;
    buffer* bufs;
} bufPool;

typedef struct bufPool* BufPool;

BufPool initBufPool(int, char*);

int request_page_in_pool(INT oid, int page_index, char* page);

void write_to_pool(Table* t, int slot, int page_index, char* page);

buffer* request_page(FILE* fp, Table* t, int page_index);

void release_page(buffer*);

int store_page_in_pool(Table* table, int page_index, char* page);

void removeFromUsedList(int slot);

void showPoolUsage();

void showPoolState();

int pageInPool(UINT, int);

BufPool get_bp();