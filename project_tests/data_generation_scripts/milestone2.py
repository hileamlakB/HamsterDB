#!/usr/bin/python
import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd

import data_gen_utils

# note this is the base path where we store the data files we generate
TEST_BASE_DIR = "/cs165/generated_data"

# note this is the base path that _POINTS_ to the data files we generate
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"

#
# Example usage: 
#   python milestone2.py 10000 42 ~/repo/cs165-docker-test-runner/test_data /cs165/staff_test
#

############################################################################
# Notes: You can generate your own scripts for generating data fairly easily by modifying this script.
#
# To test functionality and speed, run your tests first on small data. Then when you are reasonably confident that your code works, move to bigger data sizes for speed.
# 
############################################################################

def generateDataMilestone2(dataSize):
    outputFile = TEST_BASE_DIR + '/data3_batch.csv'
    header_line = data_gen_utils.generateHeaderLine('db1', 'tbl3_batch', 4)
    outputTable = pd.DataFrame(np.random.randint(0, dataSize/5, size=(dataSize, 4)), columns =['col1', 'col2', 'col3', 'col4'])
    # This is going to have many, many duplicates for large tables!!!!
    outputTable['col1'] = np.random.randint(0,1000, size = (dataSize))
    outputTable['col4'] = np.random.randint(0,10000, size = (dataSize))
    outputTable['col4'] = outputTable['col4'] + outputTable['col1']
    outputTable.to_csv(outputFile, sep=',', index=False, header=header_line, line_terminator='\n')
    return outputTable

def createTestTen():
    # prelude
    output_file, exp_output_file = data_gen_utils.openFileHandles(10, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Load Test Data 2\n')
    output_file.write('-- Create a table to run batch queries on\n')
    output_file.write('--\n')
    # query
    output_file.write('-- Loads data from: data3_batch.csv\n')
    output_file.write('--\n')
    output_file.write('-- Create Table\n')
    output_file.write('create(tbl,"tbl3_batch",db1,4)\n')
    output_file.write('create(col,"col1",db1.tbl3_batch)\n')
    output_file.write('create(col,"col2",db1.tbl3_batch)\n')
    output_file.write('create(col,"col3",db1.tbl3_batch)\n')
    output_file.write('create(col,"col4",db1.tbl3_batch)\n')
    output_file.write('--\n')
    output_file.write('-- Load data immediately\n')
    output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/data3_batch.csv\")\n')
    output_file.write('--\n')
    output_file.write('-- Testing that the data is durable on disk.\n')
    output_file.write('shutdown\n')
    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTestEleven(dataTable):
    # prelude and query
    output_file, exp_output_file = data_gen_utils.openFileHandles(11, TEST_DIR=TEST_BASE_DIR)
    output_file.write('--\n')
    output_file.write('-- Testing for batching queries\n')
    output_file.write('-- 2 queries with NO overlap\n')
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT col4 FROM tbl3_batch WHERE col1 >= 10 AND col1 < 20;\n')
    output_file.write('-- SELECT col4 FROM tbl3_batch WHERE col1 >= 800 AND col1 < 830;\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('batch_queries()\n')
    output_file.write('s1=select(db1.tbl3_batch.col1,10,20)\n')
    output_file.write('s2=select(db1.tbl3_batch.col1,800,830)\n')
    output_file.write('batch_execute()\n')
    output_file.write('f1=fetch(db1.tbl3_batch.col4,s1)\n')
    output_file.write('f2=fetch(db1.tbl3_batch.col4,s2)\n')
    output_file.write('print(f1)\n')
    output_file.write('print(f2)\n')
    # generate expected restuls. 
    dfSelectMask1 = (dataTable['col1'] >= 10) & (dataTable['col1'] < 20)
    dfSelectMask2 = (dataTable['col1'] >= 800) & (dataTable['col1'] < 830)
    output1 = dataTable[dfSelectMask1]['col4']
    output2 = dataTable[dfSelectMask2]['col4']
    exp_output_file.write(data_gen_utils.outputPrint(output1))
    exp_output_file.write('\n\n')
    exp_output_file.write(data_gen_utils.outputPrint(output2))
    exp_output_file.write('\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)


def createTestTwelve(dataTable):
    # prelude and query
    output_file, exp_output_file = data_gen_utils.openFileHandles(12, TEST_DIR=TEST_BASE_DIR)
    output_file.write('--\n')
    output_file.write('-- Testing for batching queries\n')
    output_file.write('-- 2 queries with partial overlap\n')
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT col4 FROM tbl3_batch WHERE col1 >= 600 AND col1 < 820;\n')
    output_file.write('-- SELECT col4 FROM tbl3_batch WHERE col1 >= 800 AND col1 < 830;\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('batch_queries()\n')
    output_file.write('s1=select(db1.tbl3_batch.col1,600,820)\n')
    output_file.write('s2=select(db1.tbl3_batch.col1,800,830)\n')
    output_file.write('batch_execute()\n')
    output_file.write('f1=fetch(db1.tbl3_batch.col4,s1)\n')
    output_file.write('f2=fetch(db1.tbl3_batch.col4,s2)\n')
    output_file.write('print(f1)\n')
    output_file.write('print(f2)\n')
    # generate expected restuls. 
    dfSelectMask1 = (dataTable['col1'] >= 600) & (dataTable['col1'] < 820)
    dfSelectMask2 = (dataTable['col1'] >= 800) & (dataTable['col1'] < 830)
    output1 = dataTable[dfSelectMask1]['col4']
    output2 = dataTable[dfSelectMask2]['col4']
    exp_output_file.write(data_gen_utils.outputPrint(output1))
    exp_output_file.write('\n\n')
    exp_output_file.write(data_gen_utils.outputPrint(output2))
    exp_output_file.write('\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTestThirteen(dataTable):
    # prelude and query
    output_file, exp_output_file = data_gen_utils.openFileHandles(13, TEST_DIR=TEST_BASE_DIR)
    output_file.write('--\n')
    output_file.write('-- Testing for batching queries\n')
    output_file.write('-- 2 queries with full overlap (subsumption)\n')
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT col4 FROM tbl3_batch WHERE col1 >= 810 AND col1 < 820;\n')
    output_file.write('-- SELECT col4 FROM tbl3_batch WHERE col1 >= 800 AND col1 < 830;\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('batch_queries()\n')
    output_file.write('s1=select(db1.tbl3_batch.col1,810,820)\n')
    output_file.write('s2=select(db1.tbl3_batch.col1,800,830)\n')
    output_file.write('batch_execute()\n')
    output_file.write('f1=fetch(db1.tbl3_batch.col4,s1)\n')
    output_file.write('f2=fetch(db1.tbl3_batch.col4,s2)\n')
    output_file.write('print(f1)\n')
    output_file.write('print(f2)\n')
     # generate expected restuls. 
    dfSelectMask1 = (dataTable['col1'] >= 810) & (dataTable['col1'] < 820)
    dfSelectMask2 = (dataTable['col1'] >= 800) & (dataTable['col1'] < 830)
    output1 = dataTable[dfSelectMask1]['col4']
    output2 = dataTable[dfSelectMask2]['col4']
    exp_output_file.write(data_gen_utils.outputPrint(output1))
    exp_output_file.write('\n\n')
    exp_output_file.write(data_gen_utils.outputPrint(output2))
    exp_output_file.write('\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTestFourteen(dataTable):
    # prelude and query
    output_file, exp_output_file = data_gen_utils.openFileHandles(14, TEST_DIR=TEST_BASE_DIR)
    output_file.write('--\n')
    output_file.write('-- Testing for batching queries\n')
    output_file.write('-- Queries with no overlap\n')
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- 10 Queries of the type:\n')
    output_file.write('-- SELECT col1 FROM tbl3_batch WHERE col4 >= _ AND col4 < _;\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('batch_queries()\n')
    for i in range(10):
        output_file.write('s{}=select(db1.tbl3_batch.col4,{},{})\n'.format(i, (1000 * i), (1000 * i) + 30))
    output_file.write('batch_execute()\n')
    for i in range(10):
        output_file.write('f{}=fetch(db1.tbl3_batch.col1,s{})\n'.format(i,i))
    for i in range(10):
        output_file.write('print(f{})\n'.format(i))
    #generate expected results
    for i in range(10):
        dfSelectMask = (dataTable['col4'] >= (1000 * i)) & (dataTable['col4'] < ((1000 * i) + 30))
        output = dataTable[dfSelectMask]['col1']
        exp_output_file.write(data_gen_utils.outputPrint(output))
        exp_output_file.write('\n\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)


def createTestFifteen(dataTable):
    # prelude and queryDOCKER_TEST_BASE_DIR
    output_file, exp_output_file = data_gen_utils.openFileHandles(15, TEST_DIR=TEST_BASE_DIR)
    output_file.write('--\n')
    output_file.write('-- Testing for batching queries\n')
    output_file.write('-- Queries with full overlap (subsumption)\n')
    output_file.write('--\n')
    randomVal = np.random.randint(1000,9900)
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- 10 Queries of the type:\n')
    output_file.write('-- SELECT col1 FROM tbl3_batch WHERE col4 >= _ AND col4 < _;\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('batch_queries()\n')
    for i in range(10):
        output_file.write('s{}=select(db1.tbl3_batch.col4,{},{})\n'.format(i, randomVal + (2 * i), randomVal + 60 - (2 * i)))
    output_file.write('batch_execute()\n')
    for i in range(10):
        output_file.write('f{}=fetch(db1.tbl3_batch.col1,s{})\n'.format(i,i))
    for i in range(10):
        output_file.write('print(f{})\n'.format(i))
    #generate expected results
    for i in range(10):
        dfSelectMask = (dataTable['col4'] >= (randomVal + (2 * i))) & (dataTable['col4'] < (randomVal + 60 - (2 * i)))
        output = dataTable[dfSelectMask]['col1']
        exp_output_file.write(data_gen_utils.outputPrint(output))
        exp_output_file.write('\n\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTests16And17(dataTable, dataSize):
    # 1 / 1000 tuples should qualify on average. This is so that most time is spent on scans & not fetches or prints
    offset = np.max([1, int(dataSize/5000)])
    query_starts = np.random.randint(0,(dataSize/8), size = (100))
    output_file16, exp_output_file16 = data_gen_utils.openFileHandles(16, TEST_DIR=TEST_BASE_DIR)
    output_file17, exp_output_file17 = data_gen_utils.openFileHandles(17, TEST_DIR=TEST_BASE_DIR)
    output_file16.write('--\n')
    output_file16.write('-- Control timing for without batching\n')
    output_file16.write('-- Queries for 16 and 17 are identical.\n')
    output_file16.write('-- Query in SQL:\n')
    output_file16.write('-- 100 Queries of the type:\n')
    output_file16.write('-- SELECT col3 FROM tbl3_batch WHERE col2 >= _ AND col2 < _;\n')
    output_file16.write('--\n')
    output_file17.write('--\n')
    output_file17.write('-- Same queries with batching\n')
    output_file17.write('-- Queries for 16 and 17 are identical.\n')
    output_file17.write('--\n')
    output_file17.write('batch_queries()\n')
    for i in range(100):
        output_file16.write('s{}=select(db1.tbl3_batch.col2,{},{})\n'.format(i, query_starts[i], query_starts[i] + offset))
        output_file17.write('s{}=select(db1.tbl3_batch.col2,{},{})\n'.format(i, query_starts[i], query_starts[i] + offset))
    output_file17.write('batch_execute()\n')
    for i in range(100):
        output_file16.write('f{}=fetch(db1.tbl3_batch.col3,s{})\n'.format(i,i))
        output_file17.write('f{}=fetch(db1.tbl3_batch.col3,s{})\n'.format(i,i))
    for i in range(100):
        output_file16.write('print(f{})\n'.format(i))
        output_file17.write('print(f{})\n'.format(i))
    # generate expected results
    for i in range(100):
        dfSelectMask = (dataTable['col2'] >= query_starts[i]) & ((dataTable['col2'] < (query_starts[i] + offset)))
        output = dataTable[dfSelectMask]['col3']
        exp_output_file16.write(data_gen_utils.outputPrint(output))
        exp_output_file16.write('\n\n')
        exp_output_file17.write(data_gen_utils.outputPrint(output))
        exp_output_file17.write('\n\n')
    data_gen_utils.closeFileHandles(output_file16, exp_output_file16)
    data_gen_utils.closeFileHandles(output_file17, exp_output_file17)


def generateMilestoneTwoFiles(dataSize, randomSeed):
    np.random.seed(randomSeed)
    dataTable = generateDataMilestone2(dataSize)   
    createTestTen()
    createTestEleven(dataTable)
    createTestTwelve(dataTable)
    createTestThirteen(dataTable)
    createTestFourteen(dataTable)
    createTestFifteen(dataTable)
    createTests16And17(dataTable, dataSize)

def main(argv):
    global TEST_BASE_DIR
    global DOCKER_TEST_BASE_DIR

    dataSize = int(argv[0])
    if len(argv) > 1:
        randomSeed = int(argv[1])
    else:
        randomSeed = 47

    # override the base directory for where to output test related files
    if len(argv) > 2:
        TEST_BASE_DIR = argv[2]
        if len(argv) > 3:
            DOCKER_TEST_BASE_DIR = argv[3]

    generateMilestoneTwoFiles(dataSize, randomSeed)

if __name__ == "__main__":
    main(sys.argv[1:])

