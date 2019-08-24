# CS165 Staff Note: Students do not need to modify this build file
# This is for command for deploying related to docker prep/setup/running

BASE_DIR := $(shell pwd)

DOCKER_CMD := docker

# CS165_DOCKER_USE_SUDO := $(CS165_DOCKER_USE_SUDO)

# ifeq ($(strip $(CS165_DOCKER_USE_SUDO)),)
# 	DOCKER_CMD := docker
# else
# 	DOCKER_CMD := sudo docker
# endif

# $(info $$DOCKER_CMD is [${DOCKER_CMD}])

all: prep run

# builds a docker Image called tag `cs165`, from the dockerfile in this current directory
# (`make build`), this target needs to be re-run anytime the base dockerfile image is changed
# you shouldn't need to re-run this much
build:
	$(DOCKER_CMD) build --tag=cs165 .

# runs a docker container, based off the `cs165` image that was last built and registered
run:
	$(eval DOCKER_CONT_ID := $(shell docker container run \
		-v $(BASE_DIR)/src:/cs165/src \
		-v $(BASE_DIR)/project_tests:/cs165/project_tests \
		-v $(BASE_DIR)/generated_data:/cs165/generated_data \
		-v $(BASE_DIR)/test.sh:/cs165/test.sh \
		-v $(BASE_DIR)/student_outputs:/cs165/student_outputs \
		-d --rm -t -i cs165 bash))
	$(DOCKER_CMD) exec $(DOCKER_CONT_ID) bash /cs165/test.sh
	$(DOCKER_CMD) stop $(DOCKER_CONT_ID)

prep:
	[ -d generated_data ] || mkdir generated_data
	[ -d student_outputs ] || mkdir student_outputs

# m1:
# 	$(DOCKER_CMD) container run -v $(BASE_DIR)/src:/cs165/src -v $(BASE_DIR)/project_tests:/cs165/project_tests -t -i cs165 bash

clean:
	rm -r generated_data
	rm -r student_outputs