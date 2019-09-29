# note this should be run from base project folder as `./infra_scripts/run_test 01`

# If a container is already successfully running after `make startcontainer outputdir=<ABSOLUTE_PATH1> testdir=<ABSOLUTE_PATH2>`
# This endpoint takes a `test_id` argument, from 01 up to 43,
#     runs the corresponding generated test DSLs
#    and checks the output against corresponding EXP file.

test_id=$1
DOCKER_CMD=docker

echo "Running test # $test_id"
DOCKER_CONT_ID=`cat status.current_container_id | awk '{print $1}'`
# check if there is server running already
SERVER_NUM_RUNNING=`$DOCKER_CMD exec $DOCKER_CONT_ID bash -c "ps aux | grep server | wc -l"`

if [ $(($SERVER_NUM_RUNNING)) -ne 1 ]; then
    if [ $(($SERVER_NUM_RUNNING)) -ne 0 ]; then
		# kill any servers existing
		$DOCKER_CMD exec $DOCKER_CONT_ID bash -c "if pgrep server; then pkill server; fi"
	fi


    # start the one server that should be serving test clients
    # invariant: at this point there should only be NO servers running
    $DOCKER_CMD exec -d $DOCKER_CONT_ID bash -c 'cd /cs165/src; ./server > last_server.out &'
    #$(DOCKER_CMD) exec -d $(DOCKER_CONT_ID) bash -c "cd /cs165/src; ps aux | grep ./server | tr -s ' ' | cut -f 2 -d ' ' | head -n 1 > status.current_server_pid"
    sleep 1;
fi

# invariant: at this point there should always be one server running
# Now do Testing Procedures
$DOCKER_CMD exec $DOCKER_CONT_ID bash -c "cd /cs165/src; ./client < /cs165/staff_test/test${test_id}gen.dsl > last_output.out; ../infra_scripts/verify_output_standalone.sh $test_id last_output.out /cs165/staff_test/test${test_id}gen.exp last_output_cleaned.out last_output_cleaned_sorted.out"
