#!/usr/bin/python
import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd
import math

import data_gen_utils

# note this is the base path to the data files we generate
TEST_BASE_DIR = "/cs165/generated_data"

# note this is the base path that _POINTS_ to the data files we generate
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"

############################################################################
# Notes: You can generate your own scripts for generating data fairly easily by modifying this script.
# 
############################################################################
def generateDataMilestone5(dataSize):
    outputFile = TEST_BASE_DIR + '/data5.csv'
    header_line = data_gen_utils.generateHeaderLine('db1', 'tbl5', 4)
    outputTable = pd.DataFrame(np.random.randint(0, dataSize/5, size=(dataSize, 4)), columns =['col1', 'col2', 'col3', 'col4'])
    # This is going to have many, many duplicates for large tables!!!!
    outputTable['col1'] = np.random.randint(0,1000, size = (dataSize))
    outputTable['col2'] = np.random.randint(0,1000, size = (dataSize))
    outputTable['col3'] = np.random.randint(0,10000, size = (dataSize))
    outputTable['col4'] = np.random.randint(0,10000, size = (dataSize))
    outputTable.to_csv(outputFile, sep=',', index=False, header=header_line, line_terminator='\n')
    return outputTable
    

def createTest38(dataTable):
    # prelude
    output_file, exp_output_file = data_gen_utils.openFileHandles(38, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Correctness test: Do inserts in tbl5.\n')
    output_file.write('--\n')
    output_file.write('-- Let table tbl5 have a secondary index (col2) and a clustered index (col3), so, all should be maintained when we insert new data.\n')
    output_file.write('-- This means that the table should be always sorted on col3 and the secondary indexes on col2 should be updated\n')
    output_file.write('--\n')
    output_file.write('-- Create Table\n')
    output_file.write('create(tbl,"tbl5",db1,4)\n')
    output_file.write('create(col,"col1",db1.tbl5)\n')
    output_file.write('create(col,"col2",db1.tbl5)\n')
    output_file.write('create(col,"col3",db1.tbl5)\n')
    output_file.write('create(col,"col4",db1.tbl5)\n')
    output_file.write('-- Create a clustered index on col1\n')
    output_file.write('create(idx,db1.tbl5.col1,sorted,clustered)\n')
    output_file.write('-- Create an unclustered btree index on col2\n')
    output_file.write('create(idx,db1.tbl5.col2,btree,unclustered)\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('-- Load data immediately in the form of a clustered index\n')
    output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/data5.csv\")\n')
    output_file.write('--\n')

    output_file.write('-- INSERT INTO tbl5 VALUES (-1,-11,-111,-1111);\n')
    output_file.write('-- INSERT INTO tbl5 VALUES (-2,-22,-222,-2222);\n')
    output_file.write('-- INSERT INTO tbl5 VALUES (-3,-33,-333,-2222);\n')
    output_file.write('-- INSERT INTO tbl5 VALUES (-4,-44,-444,-2222);\n')
    output_file.write('-- INSERT INTO tbl5 VALUES (-5,-55,-555,-2222);\n')
    output_file.write('--\n')
    output_file.write('relational_insert(db1.tbl5,-1,-11,-111,-1111)\n')
    output_file.write('relational_insert(db1.tbl5,-2,-22,-222,-2222)\n')
    output_file.write('relational_insert(db1.tbl5,-3,-33,-333,-2222)\n')
    output_file.write('relational_insert(db1.tbl5,-4,-44,-444,-2222)\n')
    output_file.write('relational_insert(db1.tbl5,-5,-55,-555,-2222)\n')
    #output_file.write('shutdown\n')
    # update dataTable
    dataTable = dataTable.append({"col1":-1, "col2":-11, "col3": -111, "col4": -1111}, ignore_index = True)
    dataTable = dataTable.append({"col1":-2, "col2":-22, "col3": -222, "col4": -2222}, ignore_index = True)
    dataTable = dataTable.append({"col1":-3, "col2":-33, "col3": -333, "col4": -2222}, ignore_index = True)
    dataTable = dataTable.append({"col1":-4, "col2":-44, "col3": -444, "col4": -2222}, ignore_index = True)
    dataTable = dataTable.append({"col1":-5, "col2":-55, "col3": -555, "col4": -2222}, ignore_index = True)
    
    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)
    return dataTable

def createTest39(dataTable, approxSelectivity):
    output_file, exp_output_file = data_gen_utils.openFileHandles(39, TEST_DIR=TEST_BASE_DIR)
    dataSize = len(dataTable)
    offset = int(approxSelectivity * dataSize)
    highestHighVal = int((dataSize/2) - offset)
    selectValLess = np.random.randint(-55, -11)
    selectValGreater = selectValLess + offset
    selectValLess2 = np.random.randint(-10, 0)
    selectValGreater2 = selectValLess2 + offset
    output_file.write('-- Correctness test: Test for updates on columns with index\n')
    output_file.write('--\n')
    output_file.write('-- SELECT col1 FROM tbl5 WHERE col2 >= {} AND col2 < {};\n'.format(selectValLess, selectValGreater))
    output_file.write('--\n')
    output_file.write('s1=select(db1.tbl5.col2,{},{})\n'.format(selectValLess, selectValGreater))
    output_file.write('f1=fetch(db1.tbl5.col1,s1)\n')
    output_file.write('print(f1)\n')
    output_file.write('--\n')
    output_file.write('-- SELECT col3 FROM tbl5 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess2, selectValGreater2))
    output_file.write('--\n')
    output_file.write('s2=select(db1.tbl5.col1,{},{})\n'.format(selectValLess2, selectValGreater2))
    output_file.write('f2=fetch(db1.tbl5.col3,s2)\n')
    output_file.write('print(f2)\n')
    # generate expected results
    dfSelectMaskGT = dataTable['col2'] >= selectValLess
    dfSelectMaskLT = dataTable['col2'] < selectValGreater
    output = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col1']
    if len(output) > 0:
        exp_output_file.write(output.to_string(header=False,index=False))
        exp_output_file.write('\n\n')

    dfSelectMaskGT2 = dataTable['col1'] >= selectValLess2
    dfSelectMaskLT2 = dataTable['col1'] < selectValGreater2
    output = dataTable[dfSelectMaskGT2 & dfSelectMaskLT2]['col3']
    if len(output) > 0:
        exp_output_file.write(output.to_string(header=False,index=False))
        exp_output_file.write('\n')
    data_gen_utils.closeFileHandles(output_file, exp_output_file)


def createTests40(dataTable):
    output_file, exp_output_file = data_gen_utils.openFileHandles(40, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Correctness test: Update values\n')
    output_file.write('--\n')
    output_file.write('-- UPDATE tbl5 SET col1 = -10 WHERE col1 = -1;\n')
    output_file.write('-- UPDATE tbl5 SET col1 = -20 WHERE col2 = -22;\n')
    output_file.write('-- UPDATE tbl5 SET col1 = -30 WHERE col1 = -3;\n')
    output_file.write('-- UPDATE tbl5 SET col1 = -40 WHERE col3 = -444;\n')
    output_file.write('-- UPDATE tbl5 SET col1 = -50 WHERE col1 = -5;\n')
    output_file.write('--\n')
    output_file.write('u1=select(db1.tbl5.col1,-1,0)\n')
    output_file.write('relational_update(db1.tbl5.col1,u1,-10)\n')
    output_file.write('u2=select(db1.tbl5.col2,-22,-21)\n')
    output_file.write('relational_update(db1.tbl5.col1,u2,-20)\n')
    output_file.write('u3=select(db1.tbl5.col1,-3,-2)\n')
    output_file.write('relational_update(db1.tbl5.col1,u3,-30)\n')
    output_file.write('u4=select(db1.tbl5.col3,-444,-443)\n')
    output_file.write('relational_update(db1.tbl5.col1,u4,-40)\n')
    output_file.write('u5=select(db1.tbl5.col1,-5,-4)\n')
    output_file.write('relational_update(db1.tbl5.col1,u5,-50)\n')
    output_file.write('shutdown\n')
   # update dataTable
    dfSelectMaskEq = dataTable['col1'] == -1
    dataTable.loc[dfSelectMaskEq,'col1']=-10

    dfSelectMaskEq = dataTable['col2'] == -22
    dataTable.loc[dfSelectMaskEq,'col1']=-20
    
    dfSelectMaskEq = dataTable['col1'] == -3
    dataTable.loc[dfSelectMaskEq,'col1']=-30
    
    dfSelectMaskEq = dataTable['col3'] == -444
    dataTable.loc[dfSelectMaskEq,'col1']=-40
    
    dfSelectMaskEq = dataTable['col1'] == -5
    dataTable.loc[dfSelectMaskEq,'col1']=-50

    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)
    return dataTable

def createTest41(dataTable):
    output_file, exp_output_file = data_gen_utils.openFileHandles(41, TEST_DIR=TEST_BASE_DIR)
    selectValLess = np.random.randint(-200, -100)
    selectValGreater = np.random.randint(10, 100)
    output_file.write('-- Correctness test: Run query after inserts and updates\n')
    output_file.write('--\n')
    output_file.write('-- SELECT col1 FROM tbl5 WHERE col2 >= {} AND col2 < {};\n'.format(selectValLess, selectValGreater))
    output_file.write('--\n')
    output_file.write('s1=select(db1.tbl5.col2,{},{})\n'.format(selectValLess, selectValGreater))
    output_file.write('f1=fetch(db1.tbl5.col1,s1)\n')
    output_file.write('print(f1)\n')
    # generate expected results
    dfSelectMask = (dataTable['col2'] >= selectValLess) & (dataTable['col2'] < selectValGreater)
    output = dataTable[dfSelectMask]['col1']
    exp_output_file.write(output.to_string(header=False,index=False))
    data_gen_utils.closeFileHandles(output_file, exp_output_file)


def createTest42(dataTable):
    output_file, exp_output_file = data_gen_utils.openFileHandles(42, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Correctness test: Delete values and run queries after inserts, updates, and deletes\n')
    output_file.write('--\n')
    output_file.write('-- DELETE FROM tbl5 WHERE col1 = -10;\n')
    output_file.write('-- DELETE FROM tbl5 WHERE col2 = -22;\n')
    output_file.write('-- DELETE FROM tbl5 WHERE col1 = -30;\n')
    output_file.write('-- DELETE FROM tbl5 WHERE col3 = -444;\n')
    output_file.write('-- DELETE FROM tbl5 WHERE col1 = -50;\n')
    output_file.write('-- SELECT col1 FROM tbl5 WHERE col2 >= -100 AND col2 < 20;\n')
    output_file.write('--\n')
    output_file.write('d1=select(db1.tbl5.col1,-10,-9)\n')
    output_file.write('relational_delete(db1.tbl5,d1)\n')
    output_file.write('d2=select(db1.tbl5.col2,-22,-21)\n')
    output_file.write('relational_delete(db1.tbl5,d2)\n')
    output_file.write('d3=select(db1.tbl5.col1,-30,-29)\n')
    output_file.write('relational_delete(db1.tbl5,d3)\n')
    output_file.write('d4=select(db1.tbl5.col3,-444,-443)\n')
    output_file.write('relational_delete(db1.tbl5,d4)\n')
    output_file.write('d5=select(db1.tbl5.col1,-50,-49)\n')
    output_file.write('relational_delete(db1.tbl5,d5)\n')
    output_file.write('s1=select(db1.tbl5.col2,-100,20)\n')
    output_file.write('f1=fetch(db1.tbl5.col1,s1)\n')
    output_file.write('print(f1)\n')


    # update dataTable
    dataTable = dataTable[dataTable.col1!=-10]
    dataTable = dataTable[dataTable.col2!=-22]
    dataTable = dataTable[dataTable.col1!=-30]
    dataTable = dataTable[dataTable.col3!=-444]
    dataTable = dataTable[dataTable.col1!=-50]
    
    dfSelectMask1=dataTable['col2']>=-100 
    dfSelectMask2=dataTable['col2']<20
    output = dataTable[dfSelectMask1 & dfSelectMask2]['col1']

    if len(output) > 0:
        exp_output_file.write(output.to_string(header=False,index=False))
        exp_output_file.write('\n')

    data_gen_utils.closeFileHandles(output_file, exp_output_file)
    return dataTable

def createRandomUpdates(dataTable, numberOfUpdates, output_file):
    dataSize = len(dataTable)
    for i in range(numberOfUpdates):
        updatePos = np.random.randint(1, dataSize-1)
        col2Val = dataTable.values[updatePos][1]
        col1Val = dataTable.values[updatePos][0]
        output_file.write('-- UPDATE tbl5 SET col1 = {} WHERE col2 = {};\n'.format(col1Val+1, col2Val))
        output_file.write('u1=select(db1.tbl5.col2,{},{})\n'.format(col2Val, col2Val+1))
        output_file.write('relational_update(db1.tbl5.col1,u1,{})\n'.format(col1Val+1))
        output_file.write('--\n')
        dfSelectMaskEq = dataTable['col2'] == col2Val
        dataTable.loc[dfSelectMaskEq,'col1']=col1Val+1
    return dataTable

def createRandomDeletes(dataTable, numberOfUpdates, output_file):
    for i in range(numberOfUpdates):
        dataSize = len(dataTable)
        updatePos = np.random.randint(1, dataSize-1)
        col1Val = dataTable.values[updatePos][0]
        output_file.write('-- DELETE FROM tbl5 WHERE col1 = {};\n'.format(col1Val))
        output_file.write('d1=select(db1.tbl5.col1,{},{})\n'.format(col1Val, col1Val+1))
        output_file.write('relational_delete(db1.tbl5,d1)\n')
        output_file.write('--\n')
        dataTable = dataTable[dataTable.col1!=col1Val]
    return dataTable

def createRandomInserts(dataTable, numberOfInserts, output_file):
    for i in range(numberOfInserts):
        col1Val = np.random.randint(0,1000)
        col2Val = np.random.randint(0,1000)
        col3Val = np.random.randint(0,10000)
        col4Val = np.random.randint(0,10000)
        output_file.write('-- INSERT INTO tbl5 VALUES ({},{},{},{});\n'.format(col1Val, col2Val, col3Val, col4Val))
        output_file.write('relational_insert(db1.tbl5,{},{},{},{})\n'.format(col1Val, col2Val, col3Val, col4Val))
        dataTable = dataTable.append({"col1":col1Val, "col2":col2Val, "col3": col3Val, "col4": col4Val}, ignore_index = True)
        output_file.write('--\n')
    return dataTable

def createRandomSelects(dataTable, numberOfQueries, output_file, exp_output_file):
    lowestVal = dataTable['col2'].min()
    highestVal = dataTable['col2'].max()
    dataSize = len(dataTable)
    for i in range(numberOfQueries):
        selectValLess = np.random.randint(lowestVal-1, highestVal-1)
        selectValGreater = np.random.randint(selectValLess, highestVal)
        output_file.write('-- SELECT col1 FROM tbl5 WHERE col2 >= {} AND col2 < {};\n'.format(selectValLess, selectValGreater))
        output_file.write('s1=select(db1.tbl5.col2,{},{})\n'.format(selectValLess, selectValGreater))
        output_file.write('f1=fetch(db1.tbl5.col1,s1)\n')
        output_file.write('print(f1)\n')
        dfSelectMaskGT = dataTable['col2'] >= selectValLess
        dfSelectMaskLT = dataTable['col2'] < selectValGreater
        output = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col1']
        if len(output) > 0:
            exp_output_file.write(output.to_string(header=False,index=False))
            exp_output_file.write('\n')
        

def createTest43(dataTable):
    output_file, exp_output_file = data_gen_utils.openFileHandles(43, TEST_DIR=TEST_BASE_DIR)
    output_file.write('-- Scalability test: A large number of inserts, deletes and updates, followed by a number of queries\n')
    output_file.write('--\n')
    dataTable = createRandomInserts(dataTable, 100, output_file)
    dataTable = createRandomUpdates(dataTable, 100, output_file)
    dataTable = createRandomDeletes(dataTable, 100, output_file)
    createRandomSelects(dataTable, 5, output_file, exp_output_file)
    data_gen_utils.closeFileHandles(output_file, exp_output_file)



def generateMilestoneFiveFiles(dataSize,randomSeed=47):
    np.random.seed(randomSeed)
    dataTable = generateDataMilestone5(dataSize)
    dataTable = createTest38(dataTable)
    createTest39(dataTable, 0.1)
    dataTable = createTests40(dataTable)
    createTest41(dataTable)
    dataTable = createTest42(dataTable)
    createTest43(dataTable)

def main(argv):
    global TEST_BASE_DIR
    global DOCKER_TEST_BASE_DIR
    dataSize = int(argv[0])
    if len(argv) > 1:
        randomSeed = int(argv[1])
    else:
        randomSeed = 47
    
    if len(argv) > 2:
        TEST_BASE_DIR = argv[2]
        if len(argv) > 3:
            DOCKER_TEST_BASE_DIR = argv[3]

    generateMilestoneFiveFiles(dataSize, randomSeed=randomSeed)


if __name__ == "__main__":
    main(sys.argv[1:])

