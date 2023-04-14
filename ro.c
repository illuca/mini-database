#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "ro.h"
#include "db.h"
#include "bufpool.h"

#define MIN(x, y) (((x) < (y)) ? (x) : (y))


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
    free_bp();
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

UINT get_tuple_size(UINT nattrs) {
    return nattrs * sizeof(INT);
}

INT get_ntuples_per_page(UINT nattrs) {
    UINT page_size = get_conf()->page_size;
    return (page_size - sizeof(UINT64)) / sizeof(INT) / nattrs;
}

INT get_page_num(UINT nattrs, UINT ntuples) {
    INT ntuples_per_page = get_ntuples_per_page(nattrs);
    return ceil(ntuples / (double) ntuples_per_page);
}

void print_tuples(Tuple* tuples, int length, UINT nattrs) {
    for (int i = 0; i < length; i++) {
        print_tuple(tuples[i], nattrs);
    }
}

int ntuples(buffer* p) {
    Table* t = p->table;
    int ntuples_per_page = get_ntuples_per_page(t->nattrs);
    int page_num = get_page_num(t->nattrs, t->ntuples);
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
    UINT tuple_size = get_tuple_size(t->nattrs);
    Tuple result = (Tuple) malloc(tuple_size);
    memcpy(result, page + sizeof(UINT64) + tid * tuple_size, tuple_size);
    return result;
}

FILE* get_table_fp(const char* table_name) {
    Database* db = get_db();
    Table* t = get_table(table_name);
    char* table_path = (char*) malloc(sizeof(db->path) + 5);
    sprintf(table_path, "%s/%u", db->path, t->oid);
    FILE* result = fopen(table_path, "r");
    free(table_path);
    free(t);
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
    _Table* result = (_Table*)malloc(sizeof(_Table) + t->ntuples * sizeof(Tuple));
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

    INT total_page = get_page_num(t->nattrs, t->ntuples);
    int counter = 0;
    for (int page_idx = 0; page_idx < total_page; page_idx++) {
        buffer* ibuf_p = request_page(input, t, page_idx);

        for (int tid = 0; tid < ntuples(ibuf_p); tid++) {
            Tuple tuple = get_tuple(ibuf_p, tid);
            if (tuple[idx] == cond_val) {
                result->tuples[counter++] = tuple;
            }
        }
        release_page(ibuf_p);
        log_release_page(ibuf_p->page_id);
    }
    result->ntuples = counter;
    result->nattrs = t->nattrs;
    fclose(input);
    log_close_file(t->oid);
    free(t);
    return result;
}

int hash_function(int key, int table_size) {
    return key % table_size;
}

void add_tuple(char* page, int tid, Tuple tuple, Table* t) {
    UINT tuple_size = get_tuple_size(t->nattrs);
    memcpy(page + tid * tuple_size, tuple, tuple_size);
}

Tuple join_tuple(Tuple tuple1, Tuple tuple2, Table* t1, Table* t2) {
    UINT t1_size = get_tuple_size(t1->nattrs), t2_size = get_tuple_size(t2->nattrs);
    Tuple result = (Tuple) malloc(t1_size + t2_size);
    memcpy(result, tuple1, t1_size);
    memcpy(result + t1->nattrs, tuple2, t2_size);
    return result;
}

_Table* simple_hash_join(int idx1, int idx2, FILE* fp1, FILE* fp2, Table* t1, Table* t2) {
    _Table* result = (_Table*) malloc(sizeof(_Table) + (t1->ntuples + t2->ntuples) * sizeof(Tuple));
    int buffer_size = get_bp()->nbufs;
    int buffer_count[buffer_size];
    //TODO 注意这里是指定了t1为outer，但是实际应该进行比较的，到时候就对换就行，t1和t2互换指针
    INT t1_total_page = get_page_num(t1->nattrs, t1->ntuples), t2_total_page = get_page_num(t2->nattrs, t2->ntuples);
    UINT t1_size = get_tuple_size(t1->nattrs);
    int MAX_BUFFER_SLOT_SIZE = get_conf()->page_size / sizeof(INT) / t1->nattrs;

//    buffer* buffers = (buffer*) malloc(buffer_size * (sizeof(buffer) + sizeof(Table) + get_conf()->page_size));
    buffer buffers[buffer_size];

    for (int i = 0; i < buffer_size; i++) {
        buffer_count[i] = 0;
        buffers[i].page = (char*) malloc(get_conf()->page_size);
    }

    int counter = 0;
    for (int t1_pdx = 0; t1_pdx < t1_total_page; t1_pdx++) {
        buffer* t1_ibuf_p = request_page(fp1, t1, t1_pdx);
        for (int t1_tid = 0; t1_tid < ntuples(t1_ibuf_p); t1_tid++) {
            Tuple r = get_tuple(t1_ibuf_p, t1_tid);
            int index = hash_function(r[idx1], buffer_size);
            add_tuple(buffers[index].page,buffer_count[index],r,t1);
            buffer_count[index]++;

            if (buffer_count[index] == MAX_BUFFER_SLOT_SIZE ||
                (t1_pdx == t1_total_page - 1 && t1_tid == ntuples(t1_ibuf_p) - 1)) {
                for (int t2_pdx = 0; t2_pdx < t2_total_page; t2_pdx++) {
                    buffer* t2_ibuf_p = request_page(fp2, t2, t2_pdx);
                    for (int t2_tid = 0; t2_tid < ntuples(t2_ibuf_p); t2_tid++) {
                        Tuple s = get_tuple(t2_ibuf_p, t2_tid);
                        int s_index = hash_function(s[idx2], buffer_size);

                        for (int k = 0; k < buffer_count[s_index]; k++) {
//                            Tuple rr = buffers[s_index].page + k * get_tuple_size(t1);
                            Tuple rr = (Tuple) malloc(t1_size);
                            //TODO这一步有问题
                            memcpy(rr, buffers[s_index].page + k * get_tuple_size(t1->nattrs), get_tuple_size(t1->nattrs));
                            if (rr[idx1] == s[idx2]) {
                                Tuple new_tuple = join_tuple(rr, s, t1, t2);
                                result->tuples[counter++] = new_tuple;
                            }
                        }
                    }
                    release_page(t2_ibuf_p);
                    log_release_page(t2_ibuf_p->page_id);
                }
                // clear buffer
                for (int i = 0; i < buffer_size; i++) {
                    memset(buffers[index].page, 0, MAX_BUFFER_SLOT_SIZE * t1_size);
                    buffer_count[i]=0;
                }
            }
        }
        release_page(t1_ibuf_p);
        log_release_page(t1_ibuf_p->page_id);
    }//end of for
//    free(buffers);
    for(int i = 0; i < buffer_size; i++) {
        free(buffers[i].page);
    }

    result->ntuples = counter;
    result->nattrs = t1->nattrs + t2->nattrs;
    return result;
}

_Table* block_nested_loop_join(int idx1, int idx2, FILE* fp1, FILE* fp2, Table* t1, Table* t2, bool flag) {
    _Table* result = (_Table*) malloc(sizeof(_Table) + (t1->ntuples + t2->ntuples) * sizeof(Tuple));
    int counter = 0;
    buffer* buffers = get_bp()->bufs;
    int N = get_bp()->nbufs;
    int outer_buf_size = N - 1;
    INT t1_total_page = get_page_num(t1->nattrs, t1->ntuples), t2_total_page = get_page_num(t2->nattrs, t2->ntuples);

    for (int t1_pdx = 0; t1_pdx < t1_total_page; t1_pdx += outer_buf_size) {
        // Request multiple pages for the outer relation
        int outer_pages_to_read = MIN(outer_buf_size, t1_total_page - t1_pdx);
//        buffer* outer_buffers[outer_buf_size];
        for (int i = 0; i < outer_pages_to_read; i++) {
//            outer_buffers[i] = request_page(fp1, t1, t1_pdx + i);
            write_to_pool(t1, i, t1_pdx + i, fp1);
        }

        for (int t2_pdx = 0; t2_pdx < t2_total_page; t2_pdx++) {
            // Request one page for the inner relation
            write_to_pool(t2, N-1, t2_pdx, fp2);
            buffer* t2_ibuf_p = &buffers[N - 1];

            for (int i = 0; i < outer_pages_to_read; i++) {
                buffer* t1_ibuf_p = &buffers[i];

                for (int t1_tid = 0; t1_tid < ntuples(t1_ibuf_p); t1_tid++) {
                    for (int t2_tid = 0; t2_tid < ntuples(t2_ibuf_p); t2_tid++) {
                        Tuple r = get_tuple(t1_ibuf_p, t1_tid);
                        Tuple s = get_tuple(t2_ibuf_p, t2_tid);

                        if (r[idx1] == s[idx2]) {
                            if (flag) {
                                Tuple new_tuple = join_tuple(r, s, t1, t2);
                                result->tuples[counter++] = new_tuple;
                            } else {
                                Tuple new_tuple = join_tuple(s, r, t2, t1);
                                result->tuples[counter++] = new_tuple;
                            }
                        }
                    }
                }
            }
            release_page(t2_ibuf_p);
            log_release_page(t2_ibuf_p->page_id);
        }

        // Release outer relation pages
        for (int i = 0; i < outer_pages_to_read; i++) {
            release_page(&buffers[i]);
            log_release_page(buffers[i].page_id);
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
    log_open_file(t1->oid);
    FILE* fp2 = get_table_fp(table2_name);
    log_open_file(t2->oid);

    INT t1_total_page = get_page_num(t1->nattrs, t1->ntuples);
    INT t2_total_page = get_page_num(t2->nattrs, t2->ntuples);

    if (get_conf()->buf_slots < t1_total_page + t2_total_page) {

        int N = get_bp()->nbufs;
        INT cost1 = t1_total_page + ceil(t1_total_page / (double) (N - 1)) * t2_total_page;
        INT cost2 = t2_total_page + ceil(t2_total_page / (double) (N - 1)) * t1_total_page;
        bool flag = cost1 < cost2;
        if(flag) {
            result = block_nested_loop_join(idx1, idx2, fp1, fp2, t1, t2, flag);
        } else {
            result = block_nested_loop_join(idx2, idx1, fp2, fp1, t2, t1, flag);
        }

    } else {
        result = simple_hash_join(idx1, idx2, fp1, fp2, t1, t2);
    }

    fclose(fp1);
    log_close_file(t1->oid);
    fclose(fp2);
    log_close_file(t2->oid);
//    free(t1);
//    free(t2);
    return result;
}