# Overview
This project involves the implementation of a mini database management system as part of the coursework for a database systems course. The primary objective is to understand how data is organized within a DBMS and gain hands-on experience in implementing simple relational operators. The project specifically focuses on the `select` and `join` operations, which are fundamental to querying in any relational database system.

## Code structure

1. load input arguments.
2. `init_db()` reads an input `.txt` data file and produce all table files in a certain folder.
3. `init_db()` build the database.
4. `init()` initialize the buffer pool and file pool.
5. `run()` loads and inteprets queries from a `.txt` query file and invoke  `sel()` and `join()` to process queries. Query results will be written into a log file.
6. `release()`: Cleans up resources, freeing buffer and file pools.

## Features

- **Select Operation**: Implementation of the select operator which allows filtering records based on specified conditions.
- **Join Operation**: Implementation of a nested loop join and a choice of either a hash join or a sort-merge join based on available memory buffer slots.
- **Buffer Management**: Custom implementation of a memory buffer to manage data pages using a clock-sweep algorithm, emulating buffer replacement strategies used in real-world databases like PostgreSQL.

## Design

Table File structure:

![img](https://p.ipic.vip/d7x6c9.png)

Each page and tuple(record) is constant and then we can locate a tuple by page id and tuple id. The calculation of page num and tuple num is written clearly in codes.

## HOW TO RUN

Project Structure

```shell
$project
├── Makefile
├── README.md
├── bufpool.c
├── bufpool.h
├── data/
├── db.c
├── db.h
├── filepool.c
├── filepool.h
├── main.c
├── ro.c
├── ro.h
├── run-vxdb.sh
├── run.sh
├── test/
```



```shell
cd $project
make
./main [page_size] [buffer_slots] [max_opened_files] [buffer_replacement_policy] [database_folder] [input_data] [queries] [output_log]

# For example:
cd $project
make && ./main 64 6 1 CLS ./data ./test/test1/data_1.txt ./test/test1/query_1.txt ./test/test1/log_1.txt
```



```shell
# Run all test
./run.sh

# Run all test and check memory leak by valgrind
./run-vxdb.sh
```

If you run with `data1`, then you can find the output in `$project/test/test1/log.txt`. By using `diff log.txt expected_log_1.txt`, you can check if the result is correct.
