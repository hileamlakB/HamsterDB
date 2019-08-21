# CS165 Staff Note: Students do not need to modify this build file
# This is for command for deploying related to docker prep/setup/running

all: run

# builds a docker Image called tag `cs165`, from the dockerfile in this current directory
# (`make build`), this target needs to be re-run anytime the base dockerfile image is changed
# you shouldn't need to re-run this much
build:
	docker build --tag=cs165 .

# runs a docker container, based off the `cs165` image that was last built and registered
run:
	docker container run -t -i cs165
