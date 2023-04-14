rm ./data/*
rm ./*.o
make
valgrind ./main 50 14 2 CLS ./data ./test/test3/data_3.txt ./test/test3/query_3.txt ./test/test3/log_3.txt