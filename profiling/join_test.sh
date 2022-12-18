echo "Join test ranges" > ../profiling/results/join_server_out.txt
./server >> ../profiling/results/join_server_out.txt &
sleep 1
./client < ../profiling/Join/range_join.dsl >> ../profiling/results/join_client_output.txt
./client < ../profiling/shutdown
echo "Join test Done!!"

            echo "Join test overlap" >> ../profiling/results/join_server_out.txt
            ./server >> ../profiling/results/join_server_out.txt &
sleep 1
./client < ../profiling/Join/overlap_join.dsl >> ../profiling/results/join_client_output.txt
./client < ../profiling/shutdown
echo "Join test overlap!!"

            echo "Join test row" > ../profiling/results/join_server_out.txt
            ./server >> ../profiling/results/join_server_out.txt &
sleep 1
./client < ../profiling/Join/row_join.dsl >> ../profiling/results/join_client_output.txt
./client < ../profiling/shutdown
echo "Join test row Done!!"
            