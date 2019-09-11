### Welcome to CS 165 Docker Procedure
#   Most people should not have to change this too much.
#   Should you need customization for development reasons, 
#     see the Customization section of this Dockerfile near the bottom of the file.

# declare parent image
# start from ubuntu 18.04 base docker image (specified by Docker hub itself)
FROM ubuntu:18.04

# specify working directory inside the new docker container, creating it, if does not exist
WORKDIR /cs165

# NOTE: This Run Statement is Staff Only for Setup. Students should not modify this part.
#   We keep these commands together for "caching" within docker layers.
# What are all these dependencies?
#   build-essential is needed for Make
#   gcc is needed for compilation of C
#   linux tools are for utilities such as `perf` for counters / performance measurement
#   psmisc convenience utilities for managing processes (e.g. killall processes by name)
#   valgrind for memory issue debugging
#   tmux for multiplexing between multiple windows within your interactive docker shell
#   python 2.7.x
#   python stat packages: scipy, pandas. dependencies for test generation
#   strace for stack profiling & debugging
#   mutt for email formatting, this is required on our staff automated tests
#       emailing you summaries when your trial runs are done
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    linux-tools-$(uname -r) linux-tools-generic \
    psmisc \
    python \
    python-pip \
    tmux \
    valgrind \
    strace \
    mutt \
    && \
    pip install scipy pandas

###################### Begin Customization ########################

####  NOTE: IF NEEDED, ADD ADDITIONAL IMAGE SETUP NEEDS FOR YOUR DEV REASONS HERE ####
# e.g. install your favorite dev tools:
# for more on Docker 'layering' for space amplification control:
#   https://stackoverflow.com/questions/39223249/

# RUN apt-get install emacs


# define any environmental variables you need?
# ENV MYLABEL helloworld

###################### End Customization ##########################

# start by cleaning and making, and then starting a shell with tmux
# tmux is a nice window multiplexer / manager within a single terminal
# for a quick rundown of tmux window management shortcuts:
#   https://tmuxcheatsheet.com/
CMD cd src
CMD make clean && make all && /bin/bash && tmux
