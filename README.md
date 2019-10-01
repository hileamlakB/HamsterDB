# CS165 Fall 2019

## Introduction

This repository contains the distribution code for CS165 Fall 2019.
More details about the project: http://daslab.seas.harvard.edu/classes/cs165/project.html

We suggest you fork this project, and create a git remote so you can pull patches to
the starter code periodically should the course staff need to release any during the semester.

Please make sure that your project/repo are set to private.
You'll still need to share read access with cs165 staff.

We recommend that throughout the semester, you make git tags at each of
the checkpoints so that it's easier to manage the progress of your project.

## Creating a git remote for starter code distribution patches
To first create a git remote for the distribution code, run
`git remote add distro git@code.harvard.edu:wwq724/cs165-2019-starter-code.git`

To receive patches you can pull from this new remote point:
`git pull distro master`

We will announce major patches during points in the semester.
We anticipate that patches would mostly be if any updates, issues or ambiguities arise in test generation, 
or infrastructure integration or changes to accomodate the automated testing of your code base in 
the docker environment. Should you run into a git merge conflict, due to progress commits you have made, you may have to manually triage yourself. 
For more on git merge conflict handling, see: https://help.github.com/en/articles/resolving-a-merge-conflict-using-the-command-line

## Understanding How the Docker Environment Works

Please read `docker-quickstart.txt`, `Dockerfile` and `Makefile` in this directory, to get a better understanding of how
the pieces of your C source and skeleton code integrate with Docker containers as a location for runtime.

Keep in mind that you are welcome to develop and debug your source code locally on your host machine.
For integration and testing purposes the environment which you are evaluated on is a ubuntu docker container
running on the following metal machine: 
a large state of the art server with sufficient main memory to hold the datasets you deal with.

## Client-Server code (inside `src/`)
Special Note: 
The following is regarding the starter C code. The commands described below are relative to the `src`
folder and its Makefile system, note this is not the top-level Makefile used for integration testing purposes.
If you want to run this in your integration docker environment, 
you will need to start a docker container and attach an interactive shell into it, before you can do these commands.
Of course, you are always welcome to do this in your host system during development time.
If you are not sure what this means, please read the three files described in the section above about using Docker.

We have included a simple unix socket implementation of an interactive
client-server database implementation. You are recommended to use it
as a foundation for your own database implementation. We have also
provided a sample makefile that should be compatible with most machines.
You are free to use your own makefile as well.

Inside `src` folder, you can build both the client and server using:

> `make all`

You should spin up the server first before trying to connect the client.

> `./server`
> `./client`

A high-level explanation of what happens is:

1. The server creates a socket to listen for an incoming connection.

2. The client attempts to connect to the server.

3. When the client has successfully connected, it waits for input from stdin.
Once received, it will create a message struct containing the input and
then send it to the server.  It immediately waits for a response to determine
if the server is willing to process the command.

4. When the server notices that a client has connected, it waits for a message
from the client.  When it receives the message from the client, it parses the
input and decides whether it is a valid/invalid query.
It immediately returns the response indicating whether it was valid or not.

5. Once the client receives the response, three things are possible:
1) if the query was invalid, then just go back to waiting for input from stdin.
2) if the query was valid and the server indicates that it will send back the
result, then wait to receive another message from the server.
3) if the query was valid but the server indicates that it will not send back
the result, then go back to waiting for input on stdin.

6. Back on the server side, if the query is a valid query then it should
process it, and then send back the result if it was asked to.

## Logging (inside `src/`)


We have included a couple useful logging functions in utils.c.
These logging functions depend on #ifdef located within the code.
There are multiple ways to enable logging. One way is by adding your own
definition at the top of the file:

> `#define LOG 1`

The other way is to add it during the compilation process. Instead of running
just `make`, you can run:

> `make CFLAGS+="-DLOG -DLOG_ERR -DLOG_INFO"

## Generating your own tests (inside `project_tests/data_generation_scripts`)
iIn the `project_tests/data_generation_scripts` directory we 
have several python scripts that allow generation
of test templates for base data CSVs, DSL query files, and EXP expected output files for a battery of tests
separated by milestone number (1-5). 
Note that these tests are cumulative by nature within each milestone. 
So for instance, M1 covers tests 01-09 and builds on each other 
(a test may modify data loaded/inserted by an earlier test within the same milestone number).

Read `project_tests/data_generation_scripts/README.md` for more details on how to generate and store a copy of tests locally.
We suggest you keep your generated tests in a directory which you can access between the docker container and your host file system.
See the root `Dockerfile` and `Makefile` for more about where you can mount a tests folder between container and host.

## Running a smoke test for Milestone x [NEW]
We provide you lite convenience scripts for quick diffing of your client outputs.
You can run the following make target `run_mile`, which runs a
battery of test cases up to the end of a designated milestone.
The following command runs all milestone 1 tests in order, and suppresses extra make printouts of moving directories etc:
`make run_mile mile_id=1 --no-print-directory`
