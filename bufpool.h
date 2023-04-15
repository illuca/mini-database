// bufpool.h ... buffer file_pool interface

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
    int nhits;
    int nreads;
    int nused;
    int nvb;
    buffer* buffers;
} bufPool;

typedef struct bufPool* BufPool;

BufPool init_buf_pool(int, char*);

void write_to_pool(Table* t, int slot, int page_index, FILE* fp);

buffer* request_page(FILE* fp, Table* t, int page_index);

void release_page(buffer* ibuf_p);

int page_in_pool(UINT oid, int page_index);

BufPool get_buffer_pool();

void free_buffer_pool();