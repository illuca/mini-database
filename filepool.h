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
    UINT nbufs;
    int nused;
    UINT nvb;
} FilePool;


void close_file_in_pool(UINT slot);
void init_file_pool(UINT max_opened_file_limit);
FilePool* get_file_pool();
void free_file_pool();
FileInfo* request_file(const char* filename, const char* mode, UINT oid);
void close_file();

void release_file(FileInfo* file_info);