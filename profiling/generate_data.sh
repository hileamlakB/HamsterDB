generate_data=("python ../profiling/test_load.py" "python ../profiling/test_select.py")
# Generate data
for i in "${generate_data[@]}"
do
   $i
done

# "python ../profiling/test_index.py" "../profiling/test_join.py"
