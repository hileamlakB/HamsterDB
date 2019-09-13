# CS165 Staff Note: Generally students do not need to modify this build file
# This contains Makefile endpoints/commands for deploying related to docker prep/setup/running
# 
# NOTE: If you need to make your own targets, add them to the customized section at the end
# 	We will periodically update the targets related to staff automated testing as the semester
# 	progresses. In which case you will need to pull upstream from distribution code


###################### Begin STAFF ONLY ########################

BASE_DIR := $(shell pwd)

DOCKER_CMD := docker

all: prep run

# builds a docker Image called tag `cs165`, from the dockerfile in this current directory
# (`make build`), this target needs to be re-run anytime the base dockerfile image is changed
# you shouldn't need to re-run this much
build:
	$(DOCKER_CMD) build --tag=cs165 .

# runs a docker container, based off the `cs165` image that was last built and registered
# kicks off a test bash script and then stops the container
run:
	$(eval DOCKER_CONT_ID := $(shell docker container run \
		-v $(BASE_DIR)/src:/cs165/src \
		-v $(BASE_DIR)/project_tests:/cs165/project_tests \
		-v $(BASE_DIR)/infra_scripts:/cs165/infra_scripts \
		-v $(BASE_DIR)/generated_data:/cs165/generated_data \
		-v $(BASE_DIR)/student_outputs:/cs165/student_outputs \
		-d --rm -t -i cs165 bash))
	$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash /cs165/infra_scripts/test.sh
	$(DOCKER_CMD) stop $(DOCKER_CONT_ID)

prep_build:
	$(eval DOCKER_CONT_ID := $(shell cat status.current_container_id | awk '{print $1}'))
	$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash -c "cd /cs165/src; make distclean; make; exit"

# This endpoint runs a milestone on an already running docker container, 
# based off the `cs165` image that was last built and registered.
# this target is used to kick off a test by optionally a milestone #. 
# Note FS binding, is one-way. read-only into the docker.
#
# provide `mile_id` on the make commandline
#run_mile: prep_build
#	@infra_scripts/test_milestone.sh $(mile_id)

# If a container is already successfully running after `make startcontainer outputdir=<ABSOLUTE_PATH1> testdir=<ABSOLUTE_PATH2>`
# This endpoint takes a `test_id` argument, from 01 up to 43, 
# 	runs the corresponding generated test DSLs 
#	and checks the output against corresponding EXP file.
# 
#run_test:
#	@# echo "Running test # $(test_id)"
#	@$(eval DOCKER_CONT_ID := $(shell cat status.current_container_id | awk '{print $1}'))
#	@# check if there is server running already
#	@# $(eval SERVER_NUM_RUNNING := $(shell docker exec $(DOCKER_CONT_ID) ps aux | grep ./server | wc -l))
#	@$(DOCKER_CMD) exec -d $(DOCKER_CONT_ID) bash -c "if pgrep myServer; then pkill myServer; fi"
#	@#sleep 1;
#	@# Now do Testing Procedures
#	@$(DOCKER_CMD) exec -d $(DOCKER_CONT_ID) bash -c 'cd /cs165/src; ./server > last_server.out &'
#	@$(DOCKER_CMD) exec -d $(DOCKER_CONT_ID) bash -c "cd /cs165/src; ps aux | grep ./server | tr -s ' ' | cut -f 2 -d ' ' | head -n 1 > status.current_server_pid"
#	@#sleep 1;
#	@$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash -c "cd /cs165/src; ./client < /cs165/staff_test/test$(test_id)gen.dsl > last_output.out; ../infra_scripts/verify_output_standalone.sh $(test_id) last_output.out /cs165/staff_test/test$(test_id)gen.exp last_output_cleaned.out; exit"

# usage `make startcontainer outputdir=<ABSOLUTE_PATH1> testdir=<ABSOLUTE_PATH2>`
#	where ABSOLUTE_PATH1 is the place to output runtime records
#	where ABSOLUTE_PATH2 is the place to for reading test cases CSVs, DSLs and EXPs
# starts a docker container, based off the `cs165` image that was last built and registered.
# this target is used to kick off automated testing on staff grading server
# note that staff_test mount point is one-way. read-only into the docker.
startcontainer:
	$(eval DOCKER_CONT_ID := $(shell docker container run \
		-v $(BASE_DIR)/src:/cs165/src \
		-v $(testdir):/cs165/staff_test:ro \
		-v $(BASE_DIR)/infra_scripts:/cs165/infra_scripts \
		-v $(BASE_DIR)/student_outputs:/cs165/student_outputs \
		-v $(outputdir):/cs165/infra_outputs \
		-d --rm -t --privileged -i cs165 bash))
	echo $(DOCKER_CONT_ID) > status.current_container_id

# stops a docker container, the based off the last stored current_container_id
# if startcontainer was run earlier in a session, it will be stopped by this command
stopcontainer:
	$(eval DOCKER_CONT_ID := $(shell cat status.current_container_id | awk '{print $1}'))
	$(DOCKER_CMD) stop $(DOCKER_CONT_ID)
	rm status.current_container_id

prep:
	[ -d student_outputs ] || mkdir student_outputs


clean:
	rm -r student_outputs

###################### End STAFF ONLY ##########################


###################### Begin Customization ########################

# make your own target for your own testing / dev

###################### End Customization ##########################
