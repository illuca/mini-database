#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "ro.h"
#include "db.h"
#include "bufpool.h"

void init() {
    // do some initialization here.

    // example to get the Conf pointer
    Conf* cf = get_conf();
    // example to get the Database pointer
    initBufPool(cf->buf_slots, cf->buf_policy);
    printf("init() is invoked.\n");
}

void release() {
    // optional
    // do some end tasks here.
    // free space to avoid memory leak
    printf("release() is invoked.\n");
}

Table* get_table(const char* name) {
    Table* p = (Table*) malloc(sizeof(Table));
    Database* db = get_db();
    for (size_t i = 0; i < db->ntables; i++) {
        Table table = db->tables[i];
        if (strcmp(name, table.name) == 0) {
            p->oid = table.oid;
            strcpy(p->name, table.name);
            p->nattrs = table.nattrs;
            p->ntuples = table.ntuples;
            return p;
        }
    }
    return NULL;
}

void print_tuple(Tuple tuple, INT nattrs) {
    for (size_t i = 0; i < nattrs; i++) {
        printf("%d ", tuple[i]);
    }
    printf("\n");
}

UINT64 get_page_id(char* page) {
    UINT64 page_id;
    memcpy(&page_id, page, sizeof(UINT64));
    return page_id;
}

INT tuple_size(INT nattrs) {
    return nattrs * sizeof(INT);
}

void write_tuples_from_page(Tuple* result, char* page, INT tuple_size, INT ntuples_per_page) {
    page += sizeof(UINT64);
    for (int i = 0; i < ntuples_per_page; i++) {
        Tuple tuple = (Tuple) malloc(tuple_size);
        memcpy(tuple, page, tuple_size);
        page += tuple_size;
//        print_tuple(tuple, tuple_size / sizeof(INT));
        result[i] = tuple;
    }
}

Tuple* get_tuples_from_page(char* page, INT tuple_size, INT ntuples_per_page) {
    page += sizeof(UINT64);
    Tuple tuples[ntuples_per_page + 1];
    for (int i = 0; i < ntuples_per_page; i++) {
        memcpy(tuples + i, page, tuple_size);
        print_tuple(tuples + i, tuple_size / sizeof(INT));
        page += tuple_size;
    }
    tuples[ntuples_per_page + 1] = NULL;
    return tuples;
}

Tuple* get_tuples(Table* t) {
    Database* db = get_db();
    Conf* cf = get_conf();
    Tuple* result = (Tuple*) malloc(sizeof(Tuple) * t->ntuples);

    char* table_path = (char*) malloc(sizeof(char) * 100);
    sprintf(table_path, "%s/%u", db->path, t->oid);
    FILE* table_fp = fopen(table_path, "r");
    if (table_fp == NULL) {
        printf("Table %s data cannot be found.\n", t->name);
        return NULL;
    }

    INT page_size = cf->page_size;
    INT ntuples_per_page = (page_size - sizeof(UINT64)) / sizeof(INT) / t->nattrs;

    int page_num = ceil(t->ntuples / (double) ntuples_per_page);
    int tuple_in_last_page = t->ntuples - (page_num - 1) * ntuples_per_page;
    int result_idx = 0;

    int page_index = 0;
    BufPool bp = get_bp();
    while (1) {
        char* page = NULL;
        int slot = pageInPool(bp, t->oid, page_index);
        if (slot >= 0) {
            page = bp->bufs[slot].page;
            page_index++;
            //read page to result
        } else {
            page = (char*) malloc(page_size);
            fread(page, page_size, 1, table_fp);
            request_page(bp, t->oid, page_index, page);
            page_index++;
            log_read_page(get_page_id(page));
        }
        UINT64 page_id = get_page_id(page);
        printf("Request %u-%llu\n", t->oid, page_id);

        if (page_index != page_num) {
            write_tuples_from_page(result + result_idx, page, tuple_size(t->nattrs), ntuples_per_page);
            result_idx += ntuples_per_page;
        } else if (page_index == page_num) {
            write_tuples_from_page(result + result_idx, page, tuple_size(t->nattrs), tuple_in_last_page);
            break;
        }
    }
    free(table_path);
    fclose(table_fp);
    return result;
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name) {
    printf("sel() is invoked.\n");
    Table* t = get_table(table_name);
    _Table* result = malloc(sizeof(_Table) + t->ntuples * sizeof(Tuple));
    if (t == NULL) {
        printf("Table %s does not exist.\n", table_name);
        return NULL;
    }

    Tuple* tuples = get_tuples(t);

    for (int i = 0; i < t->ntuples; i++) {
        print_tuple(tuples[i], t->nattrs);
    }

    int counter = 0;
    for (int i = 0; i < t->ntuples; i++) {

        if (tuples[i][idx] == cond_val) {
            result->tuples[counter++] = tuples[i];
        }
    }
    result->ntuples = counter;
    result->nattrs = t->nattrs;

    return result;
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    // testing
    // the following code constructs a synthetic _Table with 10 page and each tuple contains 4 attributes
    // examine log.txt to see the example outputs
    // replace all code with your implementation

//    UINT ntuples = 10;
//    UINT nattrs = 4;
//
//    _Table* result = malloc(sizeof(_Table) + ntuples * sizeof(Tuple));
//    result->nattrs = nattrs;
//    result->ntuples = ntuples;
//
//    INT value = 0;
//    for (UINT i = 0; i < result->ntuples; i++) {
//        Tuple t = malloc(sizeof(INT) * result->nattrs);
//        result->page[i] = t;
//        for (UINT j = 0; j < result->nattrs; j++) {
//            t[j] = value;
//            ++value;
//        }
//    }
//
//    return result;

    // return NULL;
}


_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name) {

    printf("join() is invoked.\n");
    // write your code to join two tables
    // invoke log_read_page() every time a page is read from the hard drive.
    // invoke log_release_page() every time a page is released from the memory.

    // invoke log_open_file() every time a page is read from the hard drive.
    // invoke log_close_file() every time a page is released from the memory.

    return NULL;
}