#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "ro.h"
#include "db.h"
#include "bufpool.h"
#include "filepool.h"

#define MIN(x, y) (((x) < (y)) ? (x) : (y))

void init() {
    printf("init() is invoked.\n");
    Conf* cf = get_conf();
    init_buf_pool(cf->buf_slots, cf->buf_policy);
    init_file_pool(cf->file_limit);
}

void release() {
    printf("release() is invoked.\n");
    free_buffer_pool();
    free_file_pool();
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

UINT get_ntuples_per_page(UINT nattrs) {
    UINT page_size = get_conf()->page_size;
    return (page_size - sizeof(UINT64)) / sizeof(INT) / nattrs;
}

INT get_page_num(UINT nattrs, UINT ntuples) {
    UINT ntuples_per_page = get_ntuples_per_page(nattrs);
    return ceil(ntuples / (double) ntuples_per_page);
}

void print_tuples(Tuple* tuples, int length, UINT nattrs) {
    for (int i = 0; i < length; i++) {
        print_tuple(tuples[i], nattrs);
    }
}

UINT get_tuples_num(buffer* p) {
    Table* t = p->table;
    UINT ntuples_per_page = get_ntuples_per_page(t->nattrs);
    int page_num = get_page_num(t->nattrs, t->ntuples);
    UINT tuple_in_last_page = t->ntuples - (page_num - 1) * ntuples_per_page;
    if (p->page_index < page_num - 1) {
        return ntuples_per_page;
    } else {
        return tuple_in_last_page;
    }
}

Tuple get_tuple_by_tuple_id(buffer* p, int tid) {
    Table* t = p->table;
    char* page = p->page;
    UINT tuple_size = get_tuple_size(t->nattrs);
    Tuple result = (Tuple) malloc(tuple_size);
    memcpy(result, page + sizeof(UINT64) + tid * tuple_size, tuple_size);
    return result;
}

FileInfo* open_file_by_table_name(const char* table_name) {
    Database* db = get_db();
    Table* t = get_table(table_name);
    char* table_path = (char*) malloc(sizeof(db->path) + 5);
    sprintf(table_path, "%s/%u", db->path, t->oid);
    FileInfo* result = request_file(table_path, "r", t->oid);
    free(table_path);
    free(t);
    return result;
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name) {
    printf("sel() is invoked.\n");
    Table* t = get_table(table_name);
    _Table* result = (_Table*) malloc(sizeof(_Table) + t->ntuples * sizeof(Tuple));
    if (t == NULL) {
        printf("Table %s does not exist.\n", table_name);
        return NULL;
    }
    FileInfo* input = open_file_by_table_name(table_name);
    if (input == NULL) {
        printf("Table %s data cannot be found.\n", t->name);
        return NULL;
    }

    INT total_page = get_page_num(t->nattrs, t->ntuples);
    int counter = 0;
    for (int page_idx = 0; page_idx < total_page; page_idx++) {
        buffer* buffer_p = request_page(input->file, t, page_idx);

        for (int tid = 0; tid < get_tuples_num(buffer_p); tid++) {
            Tuple tuple = get_tuple_by_tuple_id(buffer_p, tid);
            if (tuple[idx] == cond_val) {
                result->tuples[counter++] = tuple;
            } else {
                free(tuple);
            }
        }
        release_page(buffer_p);
        log_release_page(buffer_p->page_id);
    }
    result->ntuples = counter;
    result->nattrs = t->nattrs;
    release_file(input);
    free(t);
    return result;
}

int hash_function(UINT key, UINT tableSize) {
    return key % tableSize;
}

void add_tuple(char* page, int tid, Tuple tuple, Table* t) {
    UINT tuple_size = get_tuple_size(t->nattrs);
    memcpy(page + tid * tuple_size, tuple, tuple_size);
}

Tuple merge_tuples(Tuple tuple1, Tuple tuple2, Table* t1, Table* t2) {
    UINT t1_size = get_tuple_size(t1->nattrs), t2_size = get_tuple_size(t2->nattrs);
    Tuple result = (Tuple) malloc(t1_size + t2_size);
    memcpy(result, tuple1, t1_size);
    memcpy(result + t1->nattrs, tuple2, t2_size);
    return result;
}

_Table* simple_hash_join(UINT idx1, UINT idx2, FILE* fp1, FILE* fp2, Table* t1, Table* t2) {
    _Table* result = (_Table*) malloc(sizeof(_Table) + (t1->ntuples + t2->ntuples) * sizeof(Tuple));
    UINT buffer_size = get_buffer_pool()->nbufs;
    int buffer_count[buffer_size];
    //TODO 注意这里是指定了t1为outer，但是实际应该进行比较的，到时候就对换就行，t1和t2互换指针
    INT t1_total_page = get_page_num(t1->nattrs, t1->ntuples), t2_total_page = get_page_num(t2->nattrs, t2->ntuples);
    UINT t1_size = get_tuple_size(t1->nattrs);
    UINT MAX_BUFFER_SLOT_SIZE = get_conf()->page_size / sizeof(INT) / t1->nattrs;

    buffer buffers[buffer_size];

    for (int i = 0; i < buffer_size; i++) {
        buffer_count[i] = 0;
        buffers[i].page = (char*) malloc(get_conf()->page_size);
    }

    int counter = 0;
    for (int t1_page_index = 0; t1_page_index < t1_total_page; t1_page_index++) {
        buffer* t1_page = request_page(fp1, t1, t1_page_index);
        for (int t1_tuple_id = 0; t1_tuple_id < get_tuples_num(t1_page); t1_tuple_id++) {
            Tuple r = get_tuple_by_tuple_id(t1_page, t1_tuple_id);
            int index = hash_function(r[idx1], buffer_size);
            add_tuple(buffers[index].page, buffer_count[index], r, t1);
            free(r);
            buffer_count[index]++;

            if (buffer_count[index] == MAX_BUFFER_SLOT_SIZE ||
                (t1_page_index == t1_total_page - 1 && t1_tuple_id == get_tuples_num(t1_page) - 1)) {
                for (int t2_page_index = 0; t2_page_index < t2_total_page; t2_page_index++) {
                    buffer* t2_page = request_page(fp2, t2, t2_page_index);
                    for (int t2_tuple_id = 0; t2_tuple_id < get_tuples_num(t2_page); t2_tuple_id++) {
                        Tuple s = get_tuple_by_tuple_id(t2_page, t2_tuple_id);
                        int s_index = hash_function(s[idx2], buffer_size);

                        for (int k = 0; k < buffer_count[s_index]; k++) {
                            Tuple rr = (Tuple) malloc(t1_size);
                            memcpy(rr, buffers[s_index].page + k * get_tuple_size(t1->nattrs),
                                   get_tuple_size(t1->nattrs));
                            if (rr[idx1] == s[idx2]) {
                                Tuple new_tuple = merge_tuples(rr, s, t1, t2);
                                result->tuples[counter++] = new_tuple;
                            }
                            free(rr);
                        }
                        free(s);
                    }
                    release_page(t2_page);
                    log_release_page(t2_page->page_id);
                }
                // clear buffer
                for (int i = 0; i < buffer_size; i++) {
                    memset(buffers[index].page, 0, MAX_BUFFER_SLOT_SIZE * t1_size);
                    buffer_count[i] = 0;
                }
            }
        }
        release_page(t1_page);
        log_release_page(t1_page->page_id);
    } // end
    for (int i = 0; i < buffer_size; i++) {
        free(buffers[i].page);
    }
    result->ntuples = counter;
    result->nattrs = t1->nattrs + t2->nattrs;
    return result;
}

_Table* block_nested_loop_join(UINT idx1, UINT idx2, FILE* fp1, FILE* fp2, Table* t1, Table* t2, bool flag) {
    _Table* result = (_Table*) malloc(sizeof(_Table) + (t1->ntuples + t2->ntuples) * sizeof(Tuple));
    int counter = 0;
    buffer* buffers = get_buffer_pool()->buffers;
    UINT N = get_buffer_pool()->nbufs;
    UINT outer_buffer_size = N - 1;
    INT t1_total_page = get_page_num(t1->nattrs, t1->ntuples), t2_total_page = get_page_num(t2->nattrs, t2->ntuples);

    // outer relation
    for (UINT t1_page_index = 0; t1_page_index < t1_total_page; t1_page_index += outer_buffer_size) {
        int outer_pages_to_read = MIN(outer_buffer_size, t1_total_page - t1_page_index);
        for (int i = 0; i < outer_pages_to_read; i++) {
            write_page_to_buffer_pool(t1, i, t1_page_index + i, fp1);
        }

        // inner relation
        for (int t2_page_index = 0; t2_page_index < t2_total_page; t2_page_index++) {
            write_page_to_buffer_pool(t2, N - 1, t2_page_index, fp2);
            buffer* t2_page = &buffers[N - 1];

            for (int i = 0; i < outer_pages_to_read; i++) {
                buffer* t1_page = &buffers[i];

                for (int t1_tuple_id = 0; t1_tuple_id < get_tuples_num(t1_page); t1_tuple_id++) {
                    for (int t2_tuple_id = 0; t2_tuple_id < get_tuples_num(t2_page); t2_tuple_id++) {
                        Tuple r = get_tuple_by_tuple_id(t1_page, t1_tuple_id);
                        Tuple s = get_tuple_by_tuple_id(t2_page, t2_tuple_id);

                        if (r[idx1] == s[idx2]) {
                            if (flag) {
                                Tuple new_tuple = merge_tuples(r, s, t1, t2);
                                result->tuples[counter++] = new_tuple;
                            } else {
                                // ensure the join sequence unchanged
                                Tuple new_tuple = merge_tuples(s, r, t2, t1);
                                result->tuples[counter++] = new_tuple;
                            }
                        }
                        free(r);
                        free(s);
                    }
                }
            }
            release_page(t2_page);
            log_release_page(t2_page->page_id);
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

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name) {
    printf("join() is invoked.\n");
    Table* t1 = get_table(table1_name);
    Table* t2 = get_table(table2_name);

    _Table* result = NULL;

    FileInfo* fp1 = open_file_by_table_name(table1_name);
    FileInfo* fp2= open_file_by_table_name(table2_name);

    UINT t1_total_page = get_page_num(t1->nattrs, t1->ntuples);
    UINT t2_total_page = get_page_num(t2->nattrs, t2->ntuples);

    if (get_conf()->buf_slots < t1_total_page + t2_total_page) {

        UINT N = get_buffer_pool()->nbufs;
        UINT cost1 = t1_total_page + ceil(t1_total_page / (double) (N - 1)) * t2_total_page;
        UINT cost2 = t2_total_page + ceil(t2_total_page / (double) (N - 1)) * t1_total_page;
        // compare the cost
        bool flag = cost1 < cost2;
        if (flag) {
            result = block_nested_loop_join(idx1, idx2, fp1->file, fp2->file, t1, t2, flag);
        } else {
            // transpose outer and inner
            result = block_nested_loop_join(idx2, idx1, fp2->file, fp1->file, t2, t1, flag);
        }

    } else {
        result = simple_hash_join(idx1, idx2, fp1->file, fp2->file, t1, t2);
    }

    release_file(fp1);
    release_file(fp2);
    free(t1);
    free(t2);
    return result;
}