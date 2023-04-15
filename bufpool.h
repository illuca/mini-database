// bufpool.h ... buffer file_pool interface

#include "db.h"

typedef struct buffer {
    char id[20];
    int pin;
    int usage;
    UINT oid;
    UINT page_index;
    UINT64 page_id;
    Table* table;
    char* page;
} buffer;

typedef struct bufPool {
    UINT nbufs;
    char* strategy;      // LRU, MRU, Cycle
    int nhits;
    int nreads;
    UINT nvb;
    buffer* buffers;
} bufPool;

typedef struct bufPool* BufPool;

BufPool init_buf_pool(UINT, char*);

void write_page_to_buffer_pool(Table* t, UINT slot, UINT page_index);

buffer* request_page(Table* t, int page_index);

void release_page(buffer* buffer_p);

int page_in_pool(UINT oid, int page_index);

BufPool get_buffer_pool();

void free_buffer_pool();