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
    bp->nrequests = 0;
    bp->nreleases = 0;
    bp->nhits = 0;
    bp->nreads = 0;
    bp->nwrites = 0;
    bp->nfree = nbufs;
    bp->nused = 0;
    bp->freeList = malloc(nbufs * sizeof(int));
    assert(bp->freeList != NULL);
    bp->usedList = malloc(nbufs * sizeof(int));
    assert(bp->usedList != NULL);
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
        bp->bufs[i].table = NULL;
        bp->bufs[i].page = (char*) malloc(get_conf()->page_size);
        bp->bufs[i].table = (Table*) malloc(sizeof(Table));
        //一开始全都空闲,所以应该把0-nbufs-1都放进freeList
        bp->freeList[i] = i;
        bp->usedList[i] = -1;
    }
    return bp;
}

void free_bp() {
    if (bp != NULL) {
        if (bp->bufs != NULL) {
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

// removeFirstFree(pool)
// - use the first slot on the free list
// - the slot is removed from the free list
//   by moving all later elements down
// 先把所有的free位置用完，如果还不够再用替换算法，例如LRU之类的
static
int removeFirstFree() {
    BufPool pool = get_bp();
    //先进先出，freeList是一个队列
    int v, i;
    assert(pool->nfree > 0);
    v = pool->freeList[0];
    for (i = 0; i < pool->nfree - 1; i++)
        //freeList往前移位
        pool->freeList[i] = pool->freeList[i + 1];
    pool->nfree--;
    return v;
}

// removeFromUsedList(pool,slot)
// - search for a slot in the usedList and remove it
// - depends on how usedList managed, so is strategy-dependent

void removeFromUsedList(int slot) {
    BufPool pool = get_bp();

    int i, j;
    if (strcmp(pool->strategy, "LRU") == 0
        || strcmp(pool->strategy, "MRU") == 0) {
        j = -1;
        for (i = 0; i < pool->nused; i++) {
            if (pool->usedList[i] == slot) {
                j = i;
                break;
            }
        }
        // if it's there, remove it
        if (j >= 0) {
            //移位
            for (i = j; i < pool->nused - 1; i++)
                pool->usedList[i] = pool->usedList[i + 1];
            pool->nused--;
        }
    }
    if (strcmp(pool->strategy, "Cycling") == 0) {
        printf("Error: removeFromUsedList() not implemented for strategy %s", pool->strategy);
    }
}

// getNextSlot(pool)
// - finds the "best" unused buffer pool slot
// - "best" is determined by the replacement strategy
// - if the replaced page is dirty, write it out
// - initialise the chosen slot to hold the new page
// - if there are no available slots, return -1

static
int grabNextSlot(BufPool pool) {
    int slot;
    if (strcmp(pool->strategy, "LRU") == 0) {
        // get least recently used slot from used list
        if (pool->nused == 0) return -1;
        slot = pool->usedList[0];
        int i;
        for (i = 0; i < pool->nused - 1; i++)
            pool->usedList[i] = pool->usedList[i + 1];
        pool->nused--;
    }
    if (strcmp(pool->strategy, "MRU") == 0) {
        // get most recently used slot from used list
        if (pool->nused == 0) return -1;
        slot = pool->usedList[pool->nused - 1];
        pool->nused--;
    }
    if (strcmp(pool->strategy, "Cycling") == 0) {
        slot = -1;
        for (int i = 0; i < pool->nbufs; i++) {
            int s;
            s = (pool->currSlot + i) % pool->nbufs;
            if (pool->bufs[s].pin == 0) {
                slot = s;
                break;
            }
        }
        if (slot >= 0)
            pool->currSlot = (slot + 1) % pool->nbufs;
    }

    //TODO
    if (slot >= 0) {
        pool->nwrites++;
    }
    return slot;
}

// write page from fp to buffer[slot]
void write_to_pool(Table* t, int slot, int page_index, FILE* fp) {
    UINT page_size = get_conf()->page_size;
    char* page = (char*) malloc(page_size);
    fseek(fp, page_index * page_size, SEEK_SET);
    fread(page, page_size, 1, fp);
    log_read_page(get_page_id(page));

    BufPool pool = get_bp();
    pool->nreads++;
    UINT64 page_id;
    memcpy(&page_id, page, sizeof(UINT64));
    sprintf(pool->bufs[slot].id, "%u-%llu", t->oid, page_id);
    //TODO 注意之后removeFromUsedList要free
    pool->bufs[slot].oid = t->oid;
    memcpy(pool->bufs[slot].table, t, sizeof(Table));
    pool->bufs[slot].page_index = page_index;
    pool->bufs[slot].page_id = page_id;
    pool->bufs[slot].page = page;
    pool->bufs[slot].usage = 1;
    pool->bufs[slot].pin = 1;
}


buffer* request_page(FILE* fp, Table* t, int page_index) {
    BufPool pool = get_bp();
    buffer* bufs = pool->bufs;
    int slot = pageInPool(t->oid, page_index);
    pool->nrequests++;
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