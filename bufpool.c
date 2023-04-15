#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "bufpool.h"
#include "db.h"
#include "ro.h"

BufPool buffer_pool = NULL;

BufPool get_buffer_pool() {
    return buffer_pool;
}

BufPool init_buf_pool(UINT nbufs, char* strategy) {
    buffer_pool = malloc(sizeof(struct bufPool));
    assert(buffer_pool != NULL);
    buffer_pool->nbufs = nbufs;
    buffer_pool->strategy = malloc(sizeof(strategy));
    strcpy(buffer_pool->strategy, strategy);
    buffer_pool->nhits = 0;
    buffer_pool->nreads = 0;
    buffer_pool->buffers = malloc(nbufs * sizeof(buffer));
    assert(buffer_pool->buffers != NULL);
    buffer_pool->nvb = 0;

    for (int i = 0; i < nbufs; i++) {
        buffer_pool->buffers[i].id[0] = '\0';
        buffer_pool->buffers[i].pin = 0;
        buffer_pool->buffers[i].usage = 0;
        buffer_pool->buffers[i].oid = -1;
        buffer_pool->buffers[i].page_index = -1;
        buffer_pool->buffers[i].page_id = -1;
        buffer_pool->buffers[i].page = (char*) malloc(get_conf()->page_size);
        buffer_pool->buffers[i].table = (Table*) malloc(sizeof(Table));
    }
    return buffer_pool;
}

void free_buffer_pool() {
    if (buffer_pool != NULL) {
        if (buffer_pool->buffers != NULL) {
            for (int i = 0; i < buffer_pool->nbufs; i++) {
                if (buffer_pool->buffers[i].page != NULL) {
                    free(buffer_pool->buffers[i].page);
                }
                if (buffer_pool->buffers[i].table != NULL) {
                    free(buffer_pool->buffers[i].table);
                }
            }
            free(buffer_pool->buffers);
        }
        free(buffer_pool->strategy);
        free(buffer_pool);
    }
}


// - check whether page from rel is already in the file_pool
// - returns the slot containing this page, else returns -1
int page_in_pool(UINT oid, int page_index) {
    int slot = -1;

    for (int i = 0; i < buffer_pool->nbufs; i++) {
        buffer b = buffer_pool->buffers[i];
        if (b.oid == oid && b.page_index == page_index) {
            slot = i;
            break;
        }
    }
    return slot;
}

// write page from fp to buffer[slot]
void write_page_to_buffer_pool(Table* t, UINT slot, UINT page_index, FILE* fp) {
    UINT page_size = get_conf()->page_size;
    fseek(fp, page_index * page_size, SEEK_SET);
    fread(buffer_pool->buffers[slot].page, page_size, 1, fp);
    log_read_page(get_page_id(buffer_pool->buffers[slot].page));

    buffer_pool->nreads++;
    UINT64 page_id;
    memcpy(&page_id, buffer_pool->buffers[slot].page, sizeof(UINT64));
    sprintf(buffer_pool->buffers[slot].id, "%u-%lu", t->oid, page_id);
    buffer_pool->buffers[slot].oid = t->oid;
    memcpy(buffer_pool->buffers[slot].table, t, sizeof(Table));
    buffer_pool->buffers[slot].page_index = page_index;
    buffer_pool->buffers[slot].page_id = page_id;
    buffer_pool->buffers[slot].usage = 1;
    buffer_pool->buffers[slot].pin = 1;
}


buffer* request_page(FILE* fp, Table* t, int page_index) {
    buffer* buffers = buffer_pool->buffers;
    int slot = page_in_pool(t->oid, page_index);
    UINT buffer_size = buffer_pool->nbufs;
    UINT* nvb_p = &buffer_pool->nvb;

    if (slot >= 0) {
        buffers[slot].usage++;
        buffers[slot].pin = 1;
        buffer_pool->nhits++;
        return &buffers[slot];
    }

    while (1) {
        if (buffers[*nvb_p].pin == 0 && buffers[*nvb_p].usage == 0) {
            write_page_to_buffer_pool(t, *nvb_p, page_index, fp);
            UINT tmp = *nvb_p;
            *nvb_p = (*nvb_p + 1) % buffer_size;
            return &buffers[tmp];
        } else {
            if (buffers[*nvb_p].usage > 0) buffers[*nvb_p].usage--;
            *nvb_p = (*nvb_p + 1) % buffer_size;
        }
    }
}

void release_page(buffer* buffer_p) {
    buffer_p->pin = 0;
}