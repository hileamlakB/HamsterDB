# CS165 MakeFile
# This file includes automatic dependency tracking
# Please see example of "utils" to see how to add additional file
# to your project

all: client server

# C-compiler settings
CC = gcc -std=c99 -g -ggdb3

# Default optimization level
O ?= 0

#  -fsanitize=memory 

# Flags and other libraries
override CFLAGS += -Wall -Wextra -pedantic -pthread -O$(O) -I$(INCLUDES) 
LDFLAGS = -lpthread  -lm
LIBS =
INCLUDES = include


# this is the DB server and client's unix socket path
# Note: the extra backslash is for escaping the quotes for makefile format
SOCK_PATH=\"/tmp/cs165_unix_socket\"

####### Automatic dependency magic #######
# Set-up dependency directory
DEPSDIR := .deps
BUILDSTAMP := $(DEPSDIR)/rebuildstamp
DEPFILES := $(wildcard $(DEPSDIR)/*.d)
ifneq ($(DEPFILES),)
include $(DEPFILES)
endif
DEPCFLAGS = -MD -MF $(DEPSDIR)/$*.d -MP -DSOCK_PATH=$(SOCK_PATH)

# Dependency compilation
ifneq ($(DEP_CC),$(CC) $(CFLAGS) $(DEPCFLAGS) $(O))
DEP_CC := $(shell mkdir -p $(DEPSDIR); echo >$(BUILDSTAMP); echo "DEP_CC:=$(CC) $(CFLAGS) $(DEPCFLAGS) $(O)" >$(DEPSDIR)/_cc.d)
endif

# Make sure dependency directories are generated
$(DEPSDIR)/stamp $(BUILDSTAMP):
	mkdir -p $(@D)
	touch $@

####### Automatic dependency magic #######

%.o : %.c $(BUILDSTAMP)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -O$(O) -o $@ -c $<

##
# To include additional non-executable files (e.g. selects.c, utils.c, etc),
# you'll need to add an additional build dependency to the file that requires
# the new file.  For example, see that client and server both require utils.o
#
# If you create a new file such as selects.c, then you will need a "selects.o"
# dependency on the right side of whichever one requires the file.
##

COMMON_OBJ = utils.o\
thread_pool.o 
CLIENT_OBJ = 
SERVER_OBJ = server.o\
	parse.o\
	var_pool.o \
	tasks.o \
	hash_table.o\
	queue.o \
	sort.o \
	join.o \
	other_serializers.o \
	insert.o \
	column_serializer.o\
	parse_batch_execute.o\
	parse_create.o\
	parse_fetch.o\
	parse_insert.o\
	parse_join.o\
	parse_operations.o\
	parse_print.o\
	instance_methods.o\
	fetch.o\
	select.o\
	create_col.o\
	create_table.o\
	create_db.o\
	create_index.o\
	btree.o\
	btree_index.o\
	parse_timer.o\
	execute.o\
	print.o\
	math_operations.o\


VPATH := Insert:Utils:Serializer:Parser:Insert:Join:Others:Parallelization:Select:Varpool:Datastructures:Create:Indexing:Engine:Printer

client: client.o utils.o thread_pool.o 
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

server: $(SERVER_OBJ) $(COMMON_OBJ)
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

btree_test: btree_test.o btree.o
	$(CC) $(CFLAGS) $(DEPCFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

clean:
	rm -rf dbdir
	rm -f client server *.o *~ *.bak core *.core $(SOCK_PATH)
	rm -rf .deps
	rm -f *.out

distclean: clean
	rm -rf $(DEPSDIR)

.PHONY: all clean distclean
