// bufpool.c ... buffer pool implementation

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "bufpool.h"
#include "db.h"
#include "ro.h"

// for Cycle strategy, we simply re-use the nused counter
// since we don't actually need to maintain a usedList
#define currSlot nused

BufPool bp = NULL;

BufPool get_bp() {
    return bp;
}

// Interface Functions


// initBufPool(nbufs,strategy)
// - initialise a buffer pool with nbufs
// - buffer pool uses supplied replacement strategy

BufPool initBufPool(int nbufs, char* strategy) {
    bp = NULL;
//    struct buffer *bufs;

    bp = malloc(sizeof(struct bufPool));
    assert(bp != NULL);
    bp->nbufs = nbufs;
    bp->strategy = strategy;
    bp->nhits = 0;
    bp->nreads = 0;
    bp->bufs = malloc(nbufs * sizeof(buffer));
    assert(bp->bufs != NULL);
    bp->nvb = 0;

    int i;
    for (i = 0; i < nbufs; i++) {
        bp->bufs[i].id[0] = '\0';
        bp->bufs[i].pin = 0;
        bp->bufs[i].usage = 0;
        bp->bufs[i].oid = -1;
        bp->bufs[i].page_index = -1;
        bp->bufs[i].page_id = -1;
        bp->bufs[i].page = (char*) malloc(get_conf()->page_size);
        bp->bufs[i].table = (Table*) malloc(sizeof(Table));
        //一开始全都空闲,所以应该把0-nbufs-1都放进freeList
    }
    return bp;
}

void free_bp() {
    if (bp != NULL) {
        if (bp->bufs != NULL) {
            for (int i = 0; i < bp->nbufs; i++) {
                if (bp->bufs[i].page !=NULL) {
                    free(bp->bufs[i].page);
                }
                if (bp->bufs[i].table !=NULL) {
                    free(bp->bufs[i].table);
                }
            }
            free(bp->bufs);
        }
        free(bp);
    }
}


// - check whether page from rel is already in the pool
// - returns the slot containing this page, else returns -1
int pageInPool(UINT oid, int page_index) {
    BufPool pool = get_bp();
    int slot = -1;

    for (int i = 0; i < pool->nbufs; i++) {
        buffer b = pool->bufs[i];
        if (b.oid == oid && b.page_index == page_index) {
            //返回包含这个page的buffer的index
            slot = i;
            break;
        }
    }
    return slot;
}

// write page from fp to buffer[slot]
void write_to_pool(Table* t, int slot, int page_index, FILE* fp) {
    BufPool pool = get_bp();
    UINT page_size = get_conf()->page_size;
    fseek(fp, page_index * page_size, SEEK_SET);
    fread(pool->bufs[slot].page, page_size, 1, fp);
    log_read_page(get_page_id(pool->bufs[slot].page));

    pool->nreads++;
    UINT64 page_id;
    memcpy(&page_id, pool->bufs[slot].page, sizeof(UINT64));
    sprintf(pool->bufs[slot].id, "%u-%lu", t->oid, page_id);
    //TODO 注意之后removeFromUsedList要free
    pool->bufs[slot].oid = t->oid;
    memcpy(pool->bufs[slot].table, t, sizeof(Table));
    pool->bufs[slot].page_index = page_index;
    pool->bufs[slot].page_id = page_id;
    pool->bufs[slot].usage = 1;
    pool->bufs[slot].pin = 1;
}


buffer* request_page(FILE* fp, Table* t, int page_index) {
    BufPool pool = get_bp();
    buffer* bufs = pool->bufs;
    int slot = pageInPool(t->oid, page_index);
    UINT buffer_size = get_conf()->buf_slots;
    int* nvb_p = &pool->nvb;

    if (slot >= 0) {
        bufs[slot].usage++;
        bufs[slot].pin = 1;
        pool->nhits++;
        return &bufs[slot];
    }

    while (1) {
        if (bufs[*nvb_p].pin == 0 && bufs[*nvb_p].usage == 0) {
            write_to_pool(t, *nvb_p, page_index, fp);
            int tmp = *nvb_p;
            *nvb_p = (*nvb_p + 1) % buffer_size;
            return &bufs[tmp];
        } else {
            if (bufs[*nvb_p].usage > 0) bufs[*nvb_p].usage--;
            *nvb_p = (*nvb_p + 1) % buffer_size;
        }
    }
}

void release_page(buffer* ibuf_p) {
    ibuf_p->pin = 0;
}