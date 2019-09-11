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


## Client-Server code
We have included a simple unix socket implementation of an interactive
client-server database implementation. You are recommended to use it
as a foundation for your own database implementation. We have also
provided a sample makefile that should be compatible with most machines.
You are free to use your own makefile as well.

You can build both the client and server using:

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

## Logging

We have included a couple useful logging functions in utils.c.
These logging functions depend on #ifdef located within the code.
There are multiple ways to enable logging. One way is by adding your own
definition at the top of the file:

> `#define LOG 1`

The other way is to add it during the compilation process. Instead of running
just `make`, you can run:

> `make CFLAGS+="-DLOG -DLOG_ERR -DLOG_INFO"

## Creating a git remote for starter code distribution patches
To first create a git remote for the distribution code, run
`git remote add distro git@code.harvard.edu:wwq724/cs165-2019-starter-code.git`

To receive patches you can pull from this new remote point:
`git pull origin distro`

We will announce major patches during points in the semester.
We anticipate that patches would mostly be if any updates, issues or ambiguities arise in test generation, 
or infrastructure integration or changes to accomodate the automated testing of your code base in 
the docker environment. Should you run into a git merge conflict, due to progress commits you have made, you may have to manually triage yourself. 
For more on git merge conflict handling, see: https://help.github.com/en/articles/resolving-a-merge-conflict-using-the-command-line

## Understanding How the Docker Environment Works

Please read `docker-quickstart.txt`, `Dockerfile` and `Makefile` in this directory, to get a better understanding of how
the pieces of your C source and skeleton code integrate with Docker containers as a location for runtime.
