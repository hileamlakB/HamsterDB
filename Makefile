# CS165 Staff Note: Generally students do not need to modify this build file
# This contains Makefile endpoints/commands for deploying related to docker prep/setup/running
# 
# NOTE: If you need to make your own targets, add them to the customized section at the end
# 	We will periodically update the targets related to staff automated testing as the semester
# 	progresses. In which case you will need to pull upstream from distribution code


# This is where Docker will find your project directory path, 
#	only used for Docker volume binding purposes
BASE_DIR := $(shell pwd)

# NOTE: IF YOU ARE USING Windows 10 WSL then you need something else in the first part of the `pwd` 
#	in order to docker bind your Windows (c drive) contents to the Windows Docker
# uncomment the following, changing what is necessary to match your Windows path to your project:
# BASE_DIR := "//c/Users/MY_WINDOWS_USERNAME/REST_OF_PATH/cs165-2019-base"


ifdef CS165_PROD
BASE_DIR := $(shell pwd)
endif

###################### Begin STAFF ONLY ########################

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
	$(eval DOCKER_CONT_ID := $(shell $(DOCKER_CMD) container run \
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
	$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash /cs165/infra_scripts/prep_build.sh

# This endpoint runs a milestone on an already running docker container, 
# based off the `cs165` image that was last built and registered.
# this target is used to kick off a test by optionally a milestone #. 
# Note FS binding, is one-way. read-only into the docker.
#
# provide `mile_id` and `restart_server_wait` on the make commandline
# e.g. run milestone1 and wait 5s between each server restart: 
#	`make run_mile mile_id=1 server_wait=5`
run_mile: prep_build
	@$(eval DOCKER_CONT_ID := $(shell cat status.current_container_id | awk '{print $1}'))
	@$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash /cs165/infra_scripts/test_milestone.sh $(mile_id) $(server_wait)

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
