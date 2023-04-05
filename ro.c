#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ro.h"
#include "db.h"

void init() {
    // do some initialization here.

    // example to get the Conf pointer
    // Conf* cf = get_conf();

    // example to get the Database pointer
    // Database* db = get_db();

    printf("init() is invoked.\n");
}

void release() {
    // optional
    // do some end tasks here.
    // free space to avoid memory leak
    printf("release() is invoked.\n");
}

Table get_table(const char* table_name) {
    Database* db = get_db();
    for (size_t i = 0; i < db->ntables; i++) {
        Table table = db->tables[i];
        if (strcmp(table_name, table.name) == 0) {
            return table;
        }
    }
    return NULL;
}

_Table* sel(const UINT idx, const INT cond_val, const char* table_name) {
    Database* db = get_db();
    Table* table = NULL;
    table = &get_table(table, table_name);
    if (table == NULL) {
        printf("Table %s does not exist.\n", table_name);
        return NULL;
    }
    char* path = (char*) malloc(sizeof(char) * 100);
    sprintf(path, "%s/%u", db->path, table->oid);
    printf("path: %s.\n", path);
    printf("sel() is invoked.\n");
    return NULL;
}

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name) {

    printf("join() is invoked.\n");
    return NULL;
}