### Welcome to CS 165 Docker Procedure

# declare parent image
# start from ubuntu 18.04 base docker image (specified by Docker hub itself)
FROM ubuntu:18.04

# specify working directory inside the new docker container, creating it, if does not exist
WORKDIR /cs165


# copy everything from this dir to the docker container
# NOTE: this is now DEPRECATED and replaced by host FS mounting via the container run cmd
# COPY ./src /cs165

# install any pre-reqs and dependencies needed?
# CMD sudo apt-get install build-essential

# define any environmental variables you need?
ENV MYLABEL helloworld

# keep these commands together for "cache busting" within docker layers
# build-essential is needed for Make
# gcc is needed for compilation of C
# tmux you can use for switching between multiple windows 
#   within your interactive docker shell
#   for more on tmux here's a helpful cheatsheet: https://tmuxcheatsheet.com/
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    python \
    python-pip \
    tmux \
    valgrind \
    strace \
    mutt \
    && \
    pip install scipy pandas

# start by cleaning and making, and then starting a shell
CMD cd src
CMD make clean && make all && /bin/bash && tmux
