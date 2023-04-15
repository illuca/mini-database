#ifndef RO_H
#define RO_H
#include "db.h"
#include "filepool.h"

void init();
void release();

_Table* sel(const UINT idx, const INT cond_val, const char* table_name);

_Table* join(const UINT idx1, const char* table1_name, const UINT idx2, const char* table2_name);
#endif

FileInfo* open_file_by_table_name(const char* table_name);
Table* get_table(const char* name);
UINT64 get_page_id(char* page);