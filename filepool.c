#include <stdio.h>
#include <stdlib.h>
#include "filepool.h"
#include "db.h"

FilePool* file_pool = NULL;

void init_file_pool(UINT max_opened_file_limit) {
    file_pool = (FilePool*) malloc(sizeof(FilePool));
    file_pool->buffers = (FileInfo*) malloc(max_opened_file_limit * sizeof(FileInfo));
    for (int i = 0; i < max_opened_file_limit; i++) {
        file_pool->buffers[i].file = NULL;
        file_pool->buffers[i].oid = -1;
        file_pool->buffers[i].pin = 0;
        file_pool->buffers[i].usage = 0;
    }
    file_pool->nvb = 0;
    file_pool->num_opened_files = 0;
    file_pool->nbufs = max_opened_file_limit;
}

FilePool* get_file_pool() {
    return file_pool;
}

void free_file_pool() {
    if (file_pool->buffers != NULL) {
        for (int i = 0; i < file_pool->nbufs; i++) {
            FileInfo buffer = file_pool->buffers[i];
            if(buffer.file != NULL) {
                close_file_in_pool(i);
            }

        }
        free(file_pool->buffers);
    }
    if (file_pool != NULL) {
        free(file_pool);
    }
}

void close_file_in_pool(UINT slot) {
    FileInfo buffer = file_pool->buffers[slot];
    fclose(buffer.file);
    log_close_file(buffer.oid);
    buffer.file = NULL;
    buffer.oid = -1;
    buffer.pin = 0;
    buffer.usage--;
    file_pool->num_opened_files--;
}


int file_in_pool(UINT oid) {
    int slot = -1;
    for (int i = 0; i < file_pool->nbufs; i++) {
        if (file_pool->buffers[i].oid == oid) {
            slot = i;
            break;
        }
    }
    return slot;
}


void open_file_in_pool(UINT slot, const char* filename, const char* mode, UINT oid) {
    if (file_pool->num_opened_files >= file_pool->nbufs) {
        close_file_in_pool(slot);
    }

    file_pool->buffers[slot].file = fopen(filename, mode);
    log_open_file(oid);
    file_pool->buffers[slot].oid = oid;
    file_pool->buffers[slot].pin = 1;
    file_pool->buffers[slot].usage = 1;
    file_pool->num_opened_files++;
}

FileInfo* request_file(const char* filename, const char* mode, UINT oid) {
    FileInfo* buffers = file_pool->buffers;
    int slot = file_in_pool(oid);
    UINT buffer_size = file_pool->nbufs;
    UINT* nvb_p = &file_pool->nvb;

    if (slot >= 0) {
        buffers[slot].usage++;
        buffers[slot].pin = 1;
        return &buffers[slot];
    }

    while (1) {
        if (buffers[*nvb_p].pin == 0 && buffers[*nvb_p].usage == 0) {
            open_file_in_pool(*nvb_p, filename, mode, oid);
            UINT tmp = *nvb_p;
            *nvb_p = (*nvb_p + 1) % buffer_size;
            return &buffers[tmp];
        } else {
            if (buffers[*nvb_p].usage > 0) buffers[*nvb_p].usage--;
            *nvb_p = (*nvb_p + 1) % buffer_size;
        }
    }
}


void release_file(FileInfo* file_info) {
    file_info->pin = 0;
}