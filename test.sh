# the test directory, containing generated data dl, exp, and csvs
# expressed, relative to the base project directory
REL_TEST_DIR=generated_data
# absolute with the docker mount points
ABS_TEST_DIR=/cs165/$REL_TEST_DIR

DATA_SIZE=10000
RAND_SEED=42

# create milestone 1 data
cd project_tests/data_generation_scripts
python milestone1.py $DATA_SIZE $RAND_SEED $ABS_TEST_DIR

# setup code
cd ../../src
make clean
make all

# record results of tests
echo ""
echo "running test 1"
./server > test01gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test01gen.dsl
echo ""
echo "running test 2"
./server > test02gen.server.debug.out &
sleep 1
./client < ../$REL_TEST_DIR/test02gen.dsl