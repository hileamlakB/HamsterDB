echo "Index load test" > ../profiling/results/index_server_out.txt
./server >> ../profiling/results/index_server_out.txt &
sleep 1
echo "Index load test very_small" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_very_small.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading very_small done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test very_small1" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_very_small1.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading very_small1 done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test small" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_small.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading small done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test small2" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_small2.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading small2 done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test medium" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_medium.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading medium done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test medium2" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_medium2.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading medium2 done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test large" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_large.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading large done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test large2" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_large2.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading large2 done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test large3" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_large3.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading large3 done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test large4" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_large4.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading large4 done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test very_large" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_very_large.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading very_large done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test very_large2" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_very_large2.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading very_large2 done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
echo "Index load test exteremly_large" >> ../profiling/results/index_server_out.txt
./client < ../profiling/Index/row_test_exteremly_large.dsl >> ../profiling/results/index_client_output.txt
./client < ../profiling/shutdown
echo loading exteremly_large done
 ./server >> ../profiling/results/index_server_out.txt &

            sleep 1
