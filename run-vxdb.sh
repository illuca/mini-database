rm ./*.o
rm ./data/*
make
# binary_file page_size buffer_slots max_opened_files buffer_replacement_policy database_folder input_data queries output_log

test_folder='test'

valgrind --leak-check=full ./main 64 6 1 CLS ./data ./test/test1/data_1.txt ./test/test1/query_1.txt ./test/test1/log_1.txt

# selection only test
valgrind ./main 50 3 1 CLS ./data ./test/test2/data_2.txt ./test/test2/query_2.txt ./test/test2/log_2.txt

# join only test （for enough buffers）
valgrind ./main 50 14 1 CLS ./data ./test/test3/data_3.txt ./test/test3/query_3.txt ./test/test3/log_3.txt

# join only test  
valgrind ./main 50 5 1 CLS ./data ./test/test4/data_4.txt ./test/test4/query_4.txt ./test/test4/log_4.txt

# mixed test 
valgrind ./main 40 3 1 CLS ./data ./test/test5/data_5.txt ./test/test5/query_5.txt ./test/test5/log_5.txt