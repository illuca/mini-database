rm ./data/*
rm ./*.o
make
valgrind ./main 40 3 3 CLS ./data ./test/test5/data_5.txt ./test/test5/query_5.txt ./test/test5/log_5.txt