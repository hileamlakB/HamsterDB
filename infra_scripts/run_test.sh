# Test Convenience Script
# CS 165
# Contact: Wilson Qin

# note this should be run from base project folder as `./infra_scripts/run_test 01`
# note this should be run inside the docker container

# If a container is already successfully running after `make startcontainer outputdir=<ABSOLUTE_PATH1> testdir=<ABSOLUTE_PATH2>`
# This endpoint takes a `test_id` argument, from 01 up to 43,
#     runs the corresponding generated test DSLs
#    and checks the output against corresponding EXP file.

test_id=$1
output_dir=${2:-'/cs165/infra_outputs'}

echo "Running test # $test_id"

cd /cs165/src
# collect the client output for this test case by test_id
./client < /cs165/staff_test/test${test_id}gen.dsl 2> ${output_dir}/test${test_id}gen.out.err 1> ${output_dir}/test${test_id}gen.out
cd /cs165
# run the "comparison" script for comparing against expected output for test_id
./infra_scripts/verify_output_standalone.sh $test_id ${output_dir}/test${test_id}gen.out /cs165/staff_test/test${test_id}gen.exp ${output_dir}/test${test_id}gen.cleaned.out ${output_dir}/test${test_id}gen.cleaned.sorted.out
