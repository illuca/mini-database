rm ./data/*
rm ./*.o
make
valgrind --leak-check=full ./main 64 6 3 CLS ./data ./test/test1/data_1.txt ./test/test1/query_1.txt ./test/test1/log_1.txt