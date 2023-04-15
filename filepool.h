#include "db.h"

typedef struct FileInfo {
    FILE* file;
    UINT oid;
    int pin;
    int usage;
} FileInfo;

typedef struct FilePool {
    FileInfo* buffers;
    int num_opened_files;
    int nbufs;
    int nused;
    int nvb;
} FilePool;


void close_file_in_pool(int slot);
void init_file_pool(int max_opened_file_limit);
FilePool* get_file_pool();
void free_file_pool();
FileInfo* request_file(const char* filename, const char* mode, UINT oid);
void close_file();

void release_file(FileInfo* file_info);