#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include "filepool.h"
#include "db.h"

FilePool* file_pool = NULL;

void init_file_pool(int max_opened_file_limit) {
    file_pool = (FilePool*) malloc(sizeof(FilePool));
    file_pool->buffers = (FileInfo*) malloc(max_opened_file_limit * sizeof(FileInfo));
    for (int i = 0; i < max_opened_file_limit; i++) {
        file_pool->buffers[i].file = NULL;
        file_pool->buffers[i].oid = -1;
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
    if (file_pool->files != NULL) {
        free(file_pool->files);
    }
    if (file_pool->oids != NULL) {
        free(file_pool->oids);
    }
    if (file_pool != NULL) {
        free(file_pool);
    }
}

bool close_oldest_file() {
    for (int i = 0; i < file_pool->nbufs; i++) {
        if (file_pool->files[i] != NULL) {
            fclose(file_pool->files[i]);
            log_close_file(file_pool->oids[i]);
            file_pool->files[i] = NULL;
            file_pool->oids[i] = -1;
            file_pool->num_opened_files--;
            return true;
        }
    }
    return false;
}


int file_in_pool(UINT oid) {
    int slot = -1;
    for (int i = 0; i < file_pool->nbufs; i++) {
        if (file_pool->oids[i] == oid) {
            slot = i;
            break;
        }
    }
    return slot;
}

FILE* request_page(Table* t) {
    FILE** files = file_pool->files;
    int slot = file_in_pool(t->oid);
    UINT buffer_size = get_conf()->buf_slots;
    int* nvb_p = &buffer_pool->nvb;

    if (slot >= 0) {
        bufs[slot].usage++;
        bufs[slot].pin = 1;
        buffer_pool->nhits++;
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


FILE* request_file(const char* filename, const char* mode, UINT oid) {
    if (file_pool->num_opened_files >= file_pool->nbufs) {
        if (!close_oldest_file()) {
            return NULL;
        }
    }

    for (int i = 0; i < file_pool->nbufs; i++) {
        if (file_pool->files[i] == NULL) {
            file_pool->files[i] = fopen(filename, mode);
            log_open_file(oid);
            file_pool->oids[i] = oid;

            if (file_pool->files[i] != NULL) {
                file_pool->num_opened_files++;
            }
            return file_pool->files[i];
        }
    }
    return NULL;
}