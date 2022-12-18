echo "Range Selectivity" > ../profiling/results/select_server_output.txt
./server >> ../profiling/results/select_server_output.txt &
sleep 1
./client < ../profiling/Select/selectivity.dsl >> ../profiling/results/select_client_output.txt
echo range selectivity_test done!!
./client < ../profiling/shutdown
echo "Group Size Selectivity" >> ../profiling/results/select_server_output.txt
./server >> ../profiling/results/select_server_output.txt &
sleep 1
./client < ../profiling/Select/grouped_select_test.dsl >> ../profiling/results/select_client_output.txt
echo group size selectivity_test done!!
./client < ../profiling/shutdown
echo "Table Size Selectivity" >> ../profiling/results/select_server_output.txt
./server >> ../profiling/results/select_server_output.txt &
sleep 1
./client < ../profiling/Select/size_select_test.dsl >> ../profiling/results/select_client_output.txt
echo table size selectivity_test done!!
./client < ../profiling/shutdown
echo "Fixed selectivity test" >> ../profiling/results/select_server_output.txt
./server >> ../profiling/results/select_server_output.txt &
sleep 1
./client < ../profiling/Select/batch_select_fixed_selectivity_test.dsl >> ../profiling/results/select_client_output.txt
echo Fixed selectivity test done!!
./client < ../profiling/shutdown
echo "Random selectivity test" >> ../profiling/results/select_server_output.txt
./server >> ../profiling/results/select_server_output.txt &
sleep 1
./client < ../profiling/Select/batch_select_random_selectivity_test.dsl >> ../profiling/results/select_client_output.txt
echo random selectivity test done!!
./client < ../profiling/shutdown
echo "Different size batch test" >> ../profiling/results/select_server_output.txt
./server >> ../profiling/results/select_server_output.txt &
sleep 1
./client < ../profiling/Select/batch_queries_different_size.dsl >> ../profiling/results/select_client_output.txt
echo different size batch test done!!
./client < ../profiling/shutdown
