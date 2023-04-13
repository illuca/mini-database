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

void print_tuple(Tuple tuple, UINT nattrs) {
    for (size_t i = 0; i < nattrs; i++) {
        printf("%d ", tuple[i]);
    }
    printf("\n");
}

void print_table_tuple(Tuple tuple, Table* t) {
    print_tuple(tuple, t->nattrs);
}

UINT64 get_page_id(char* page) {
    UINT64 page_id;
    memcpy(&page_id, page, sizeof(UINT64));
    return page_id;
}

UINT get_tuple_size(Table* t) {
    return t->nattrs * sizeof(INT);
}

void write_tuples_from_page(Tuple* result, char* page, INT tuple_size, INT ntuples_per_page) {
    page += sizeof(UINT64);
    for (int i = 0; i < ntuples_per_page; i++) {
        Tuple tuple = (Tuple) malloc(tuple_size);
        memcpy(tuple, page, tuple_size);
        page += tuple_size;
//        print_tuple(tuple, get_tuple_size / sizeof(INT));
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



INT get_ntuples_per_page(Table* t) {
    return (get_conf()->page_size - sizeof(UINT64)) / sizeof(INT) / t->nattrs;
}

INT get_page_num(Table* t) {
    INT ntuples_per_page = get_ntuples_per_page(t);
    return ceil(t->ntuples / (double) ntuples_per_page);
}

void print_tuples(Tuple* tuples, int length, UINT nattrs) {
    for (int i = 0; i < length; i++) {
        print_tuple(tuples[i], nattrs);
    }
}

int ntuples(buffer* p) {
    Table* t = p->table;
    int ntuples_per_page = get_ntuples_per_page(t);
    int page_num = get_page_num(t);
    int tuple_in_last_page = t->ntuples - (page_num - 1) * ntuples_per_page;
    if (p->page_index < page_num - 1) {
        return ntuples_per_page;
    } else {
        return tuple_in_last_page;
    }
}

Tuple get_tuple(buffer* p, int tid) {
    Table* t = p->table;
    char* page = p->page;
    Tuple result = (Tuple) malloc(sizeof(Tuple));
    UINT tuple_size = get_tuple_size(t);
    memcpy(result, page + sizeof(UINT64) + tid * tuple_size, tuple_size);
    return result;
}

buffer* get_page_hash_join(FILE* fp, Table* t, int page_index) {
    INT page_size = get_conf()->page_size;
    char* page = (char*) malloc(page_size);
    fseek(fp, page_index * page_size, SEEK_SET);
    fread(page, page_size, 1, fp);
    log_read_page(get_page_id(page));
    BufPool bp = get_bp();
    buffer* result = (buffer*) malloc(sizeof(buffer));
    result->page = page;
    result->table = t;
    result->oid = t->oid;
    result->page_index = page_index;
    return result;
}

buffer* get_page(FILE* fp, Table* t, int page_index) {
    BufPool pool = get_bp();
    int slot = pageInPool(pool, t->oid, page_index);
    pool->nrequests++;

    if (slot >= 0) {
        pool->nhits++;
    } else {
        INT page_size = get_conf()->page_size;
        char* page = (char*) malloc(page_size);
        fseek(fp, page_index * page_size, SEEK_SET);
        fread(page, page_size, 1, fp);
        log_read_page(get_page_id(page));
        slot = store_page_in_pool(pool, t, page_index, page);
    }
    //TODO：所以pin是什么？
    pool->bufs[slot].pin++;
    removeFromUsedList(pool, slot);
    showPoolState(pool);  // for debugging

    return &pool->bufs[slot];
}

FILE* get_table_fp(char* table_name) {
    Database * db = get_db();
    Table* t = get_table(table_name);
    char* table_path = (char*) malloc(sizeof(char) * 100);
    sprintf(table_path, "%s/%u", db->path, t->oid);
    FILE* result = fopen(table_path, "r");
    return result;
}

// invoke log_open_file() every time a page is read from the hard drive.
// invoke log_close_file() every time a page is released from the memory.
// testing
// the following code constructs a synthetic _Table with 10 page and each tuple contains 4 attributes
// examine log.txt to see the example outputs
// replace all code with your implementation
_Table* sel(const UINT idx, const INT cond_val, const char* table_name) {
    printf("sel() is invoked.\n");
    Table* t = get_table(table_name);
    _Table* result = malloc(sizeof(_Table) + t->ntuples * sizeof(Tuple));
    if (t == NULL) {
        printf("Table %s does not exist.\n", table_name);
        return NULL;
    }
    FILE* input = get_table_fp(table_name);
    log_open_file(t->oid);
    if (input == NULL) {
        printf("Table %s data cannot be found.\n", t->name);
        return NULL;
    }

    INT total_page = get_page_num(t);
    int counter = 0;
    for (int page_idx = 0; page_idx < total_page; page_idx++) {
        buffer* ibuf_p = get_page(input, t, page_idx);

        for(int tid = 0; tid < ntuples(ibuf_p); tid++) {
            Tuple tuple = get_tuple(ibuf_p, tid);
            if(tuple[idx] == cond_val) {
                result->tuples[counter++] = tuple;
            }
        }
    }
    result->ntuples = counter;
    result->nattrs = t->nattrs;
    fclose(input);
    log_close_file(t->oid);
    return result;
}

char* get_pages(Table* t) {
    return NULL;
}

int hash_function(int key, int table_size) {
    return key % table_size;
}

void add_tuple(char* page, int tid, Tuple tuple, Table * t) {
    UINT tuple_size = get_tuple_size(t);
    memcpy(page + tid * tuple_size, tuple, tuple_size);
}

Tuple join_tuple(Tuple tuple1, Tuple tuple2, Table* t1, Table* t2) {
    UINT t1_size = get_tuple_size(t1), t2_size = get_tuple_size(t2);
    Tuple result = (Tuple) malloc(t1_size + t2_size);
    memcpy(result, tuple1, t1_size);
    memcpy(result + t1->nattrs, tuple2, t2_size);
    return result;
}

_Table* simple_hash_join(int idx1, int idx2, FILE* fp1, FILE* fp2, Table* t1, Table* t2) {
    _Table* result = (_Table*) malloc(sizeof(_Table) + (t1->ntuples + t2->ntuples) * sizeof(Tuple));
    buffer* buffers = (buffer*) malloc(sizeof(buffer));
    int buffer_size = get_conf()->buf_slots;
    int buffer_count[buffer_size];
    INT t1_total_page = get_page_num(t1), t2_total_page = get_page_num(t2);
    UINT t1_size = get_tuple_size(t1), t2_size = get_tuple_size(t2);

    //TODO 注意这里是指定了t1为outer，但是实际应该进行比较的，到时候就对换就行，t1和t2互换指针
    int MAX_BUFFER_SIZE = get_conf()->page_size / sizeof(INT) / t1->nattrs;

    for (int i = 0; i < buffer_size; i++) {
        buffers[i].page = (char*) malloc(MAX_BUFFER_SIZE * t1_size);
        buffer_count[i] = 0;
    }

    int counter=0;
    for(int t1_pdx=0; t1_pdx < t1_total_page; t1_pdx++) {
        buffer* t1_ibuf_p = get_page(fp1, t1, t1_pdx);
        for(int t1_tid = 0; t1_tid < ntuples(t1_ibuf_p); t1_tid++) {
            Tuple r = get_tuple(t1_ibuf_p, t1_tid);
            int index = hash_function(r[idx1], buffer_size);
            add_tuple(buffers[index].page, buffer_count[index], r, t1);
            buffer_count[index]++;

            if (buffer_count[index] == MAX_BUFFER_SIZE ||
                (t1_pdx == t1_total_page - 1 && t1_tid == ntuples(t1_ibuf_p) - 1)) {
                for (int t2_pdx = 0; t2_pdx < t2_total_page; t2_pdx++) {
                    buffer* t2_ibuf_p = get_page(fp2, t2, t2_pdx);
                    for (int t2_tid = 0; t2_tid < ntuples(t2_ibuf_p); t2_tid++) {
                        Tuple s = get_tuple(t2_ibuf_p, t2_tid);
                        int s_index = hash_function(s[idx2], buffer_size);

                        for (int k = 0; k < buffer_count[s_index]; k++) {
                            Tuple rr = (Tuple) malloc(t1_size);
                            memcpy(rr, buffers[s_index].page + k * get_tuple_size(t1), get_tuple_size(t1));
                            if (rr[idx1] == s[idx2]) {
                                Tuple new_tuple = join_tuple(rr, s, t1, t2);
                                result->tuples[counter++] = new_tuple;
                                print_tuple(new_tuple, t1->nattrs + t2->nattrs);
                            }
                        }
                    }
                }
                // clear buffer
                for (int i = 0; i < buffer_size; i++) {
                    buffer_count[i] = 0;
                }
            }
        }
//        for (int i = 0; i < buffer_size; i++) {
//            free(buffers[i].page);
//        }
    }
    result->ntuples = counter;
    result->nattrs = t1->nattrs + t2->nattrs;
    return result;
}

_Table* nested_for_loop_join(int idx1, int idx2, FILE* fp1, FILE* fp2, Table* t1, Table* t2) {
    _Table* result = (_Table*) malloc(sizeof(_Table) + (t1->ntuples + t2->ntuples) * sizeof(Tuple));
    int counter = 0;
    INT t1_size = get_tuple_size(t1), t2_size = get_tuple_size(t2);
    INT t1_total_page = get_page_num(t1), t2_total_page = get_page_num(t2);
    for(int t1_pdx=0; t1_pdx < t1_total_page; t1_pdx++) {
        buffer* t1_ibuf_p = get_page(fp1, t1, t1_pdx);
        for(int t2_pdx=0; t2_pdx < t2_total_page; t2_pdx++) {
            buffer* t2_ibuf_p = get_page(fp2, t2, t2_pdx);
            for(int t1_tid = 0; t1_tid < ntuples(t1_ibuf_p); t1_tid++) {
                for(int t2_tid = 0; t2_tid < ntuples(t2_ibuf_p); t2_tid++) {
                    Tuple r = get_tuple(t1_ibuf_p, t1_tid);
                    Tuple s = get_tuple(t2_ibuf_p, t2_tid);
                    if (r[idx1] == s[idx2]) {
                        Tuple new_tuple = join_tuple(r, s, t1, t2);
                        result->tuples[counter++] = new_tuple;
                        print_tuple(new_tuple, t1->nattrs + t2->nattrs);
                    }
                }
            }
        }
    }
    result->ntuples = counter;
    result->nattrs = t1->nattrs + t2->nattrs;
    return result;
}

// write your code to join two tables
// invoke log_read_page() every time a page is read from the hard drive.
// invoke log_release_page() every time a page is released from the memory.

// invoke log_open_file() every time a page is read from the hard drive.
// invoke log_close_file() every time a page is released from the memory.
_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name) {
    printf("join() is invoked.\n");
    Table* t1 = get_table(table1_name);
    Table* t2 = get_table(table2_name);

    _Table* result = NULL;

    FILE* fp1 = get_table_fp(table1_name);
    FILE* fp2 = get_table_fp(table2_name);

    INT t1_total_page = get_page_num(t1);
    INT t2_total_page = get_page_num(t2);

    if (get_conf()->buf_slots < t1_total_page + t2_total_page) {
        result = nested_for_loop_join(idx1, idx2, fp1, fp2, t1, t2);
    } else {
        result = simple_hash_join(idx1, idx2, fp1, fp2, t1, t2);
    }
    return result;
}