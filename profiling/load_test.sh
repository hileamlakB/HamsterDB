pwd
echo "ROW LOAD TEST" > ../profiling/results/load_server_output.txt
perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_very_small.dsl >> ../profiling/results/load_client_output.txt
echo row_test_very_small done!!


perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_very_small1.dsl >> ../profiling/results/load_client_output.txt
echo row_test_very_small1 done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_small.dsl >> ../profiling/results/load_client_output.txt
echo row_test_small done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_small2.dsl >> ../profiling/results/load_client_output.txt
echo row_test_small2 done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_medium.dsl >> ../profiling/results/load_client_output.txt
echo row_test_medium done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_medium2.dsl >> ../profiling/results/load_client_output.txt
echo row_test_medium2 done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_large.dsl >> ../profiling/results/load_client_output.txt
echo row_test_large done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_large2.dsl >> ../profiling/results/load_client_output.txt
echo row_test_large2 done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_large3.dsl >> ../profiling/results/load_client_output.txt
echo row_test_large3 done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_large4.dsl >> ../profiling/results/load_client_output.txt
echo row_test_large4 done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_very_large.dsl >> ../profiling/results/load_client_output.txt
echo row_test_very_large done!!

perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
 >> ../profiling/results/load_server_output.txt &
sleep 1
./client < ../profiling/Load/row_test_very_large2.dsl >> ../profiling/results/load_client_output.txt
echo row_test_very_large2 done!!
# perf stat -B -e cache-references,cache-misses,cycles,instructions,branches,faults,migrations ./server
#  >> ../profiling/results/load_server_output.txt &
# sleep 1
# ./client < ../profiling/shutdown
# echo "COL LOAD TEST" >> ../profiling/results/load_server_output.txt
# ./server >> ../profiling/results/load_server_output.txt &
# sleep 1
# ./client < ../profiling/Load/col_test_one.dsl >> ../profiling/results/load_client_output.txt
# echo col_test_one done!!
# ./client < ../profiling/Load/col_test_three.dsl >> ../profiling/results/load_client_output.txt
# echo col_test_three done!!
# ./client < ../profiling/Load/col_test_five.dsl >> ../profiling/results/load_client_output.txt
# echo col_test_five done!!
# ./client < ../profiling/Load/col_test_ten.dsl >> ../profiling/results/load_client_output.txt
# echo col_test_ten done!!
# ./client < ../profiling/Load/col_test_fifteen.dsl >> ../profiling/results/load_client_output.txt
# echo col_test_fifteen done!!
# ./client < ../profiling/Load/col_test_twenty.dsl >> ../profiling/results/load_client_output.txt
# echo col_test_twenty done!!
# ./client < ../profiling/shutdown
