tests=("../profiling/load_test.sh" "../profiling/select_test.sh")
# run each test
for i in "${tests[@]}"
do
   $i
done


# "../profiling/join_test.sh" "../profiling/index_test.sh"