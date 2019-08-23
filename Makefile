# CS165 Staff Note: Students do not need to modify this build file
# This is for command for deploying related to docker prep/setup/running

BASE_DIR := $(shell pwd)

all: prep run

# builds a docker Image called tag `cs165`, from the dockerfile in this current directory
# (`make build`), this target needs to be re-run anytime the base dockerfile image is changed
# you shouldn't need to re-run this much
build:
	docker build --tag=cs165 .

# runs a docker container, based off the `cs165` image that was last built and registered
run:
	$(eval DOCKER_CONT_ID := $(shell docker container run \
		-v $(BASE_DIR)/src:/cs165/src \
		-v $(BASE_DIR)/project_tests:/cs165/project_tests \
		-v $(BASE_DIR)/generated_data:/cs165/generated_data \
		-v $(BASE_DIR)/test.sh:/cs165/test.sh \
		-d --rm -t -i cs165 bash))
	docker exec $(DOCKER_CONT_ID) bash /cs165/test.sh
	docker stop $(DOCKER_CONT_ID)

prep:
	[ -d generated_data ] || generated_data
	[ -d student_outputs ] || student_outputs

# m1:
# 	docker container run -v $(BASE_DIR)/src:/cs165/src -v $(BASE_DIR)/project_tests:/cs165/project_tests -t -i cs165 bash

clean:
	rm -r generated_data
	rm -r student_outputs