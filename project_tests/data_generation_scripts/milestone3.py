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


############################################################################
# Notes: You can generate your own scripts for generating data fairly easily by modifying this script.
# 
############################################################################

def generateDataMilestone3(dataSize):
    outputFile_ctrl = 'data4_ctrl.csv'
    outputFile_btree = 'data4_btree.csv'
    header_line_ctrl = data_gen_utils.generateHeaderLine('db1', 'tbl4_ctrl', 4)
    header_line_btree = data_gen_utils.generateHeaderLine('db1', 'tbl4_btree', 4)
    outputTable = pd.DataFrame(np.random.randint(0, dataSize/5, size=(dataSize, 4)), columns =['col1', 'col2', 'col3', 'col4'])
    # This is going to have many, many duplicates for large tables!!!!
    outputTable['col1'] = np.random.randint(0,1000, size = (dataSize))
    outputTable['col4'] = np.random.randint(0,10000, size = (dataSize))
    ### make ~5\% of values a single value! 
    maskStart = np.random.uniform(0.0,1.0, dataSize)   
    mask1 = maskStart < 0.05
    ### make ~2% of values a different value
    maskStart = np.random.uniform(0.0,1.0, dataSize)
    mask2 = maskStart < 0.02
    outputTable['col2'] = np.random.randint(0,10000, size = (dataSize))
    frequentVal1 = np.random.randint(0,int(dataSize/5))
    frequentVal2 = np.random.randint(0,int(dataSize/5))
    outputTable.loc[mask1, 'col2'] = frequentVal1
    outputTable.loc[mask2, 'col2'] = frequentVal2
    outputTable['col4'] = outputTable['col4'] + outputTable['col1']
    outputTable.to_csv(outputFile_ctrl, sep=',', index=False, header=header_line_ctrl, line_terminator='\n')
    outputTable.to_csv(outputFile_btree, sep=',', index=False, header=header_line_btree, line_terminator='\n')
    return frequentVal1, frequentVal2, outputTable

def createTest18():
    # prelude
    output_file, exp_output_file = data_gen_utils.openFileHandles(18)
    output_file.write('-- Create a control table that is identical to the one in test19.dsl, but\n')
    output_file.write('-- without any indexes\n')
    output_file.write('--\n')
    output_file.write('-- Loads data from: data4_ctrl.csv\n')
    output_file.write('--\n')
    output_file.write('-- Create Table\n')
    output_file.write('create(tbl,"tbl4_ctrl",db1,4)\n')
    output_file.write('create(col,"col1",db1.tbl4_ctrl)\n')
    output_file.write('create(col,"col2",db1.tbl4_ctrl)\n')
    output_file.write('create(col,"col3",db1.tbl4_ctrl)\n')
    output_file.write('create(col,"col4",db1.tbl4_ctrl)\n')
    output_file.write('--\n')
    output_file.write('-- Load data immediately\n')
    output_file.write('load("/home/cs165/cs165-management-scripts/project_tests_2017/data4_ctrl.csv")\n')
    output_file.write('--\n')
    output_file.write('-- Testing that the data and their indexes are durable on disk.\n')
    output_file.write('shutdown\n')
    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTest19():
    output_file, exp_output_file = data_gen_utils.openFileHandles(19)
    output_file.write('-- Test for creating table with indexes\n')
    output_file.write('--\n')
    output_file.write('-- Table tbl3 has a clustered index with col3 being the leading column.\n')
    output_file.write('-- The clustered index has the form of a sorted column.\n')
    output_file.write('-- The table also has a secondary btree index.\n')
    output_file.write('--\n')
    output_file.write('-- Loads data from: data4_btree.csv\n')
    output_file.write('--\n')
    output_file.write('-- Create Table\n')
    output_file.write('create(tbl,"tbl4",db1,4)\n')
    output_file.write('create(col,"col1",db1.tbl4)\n')
    output_file.write('create(col,"col2",db1.tbl4)\n')
    output_file.write('create(col,"col3",db1.tbl4)\n')
    output_file.write('create(col,"col4",db1.tbl4)\n')
    output_file.write('-- Create a clustered index on col1\n')
    output_file.write('create(idx,db1.tbl4.col3,sorted,clustered)\n')
    output_file.write('-- Create an unclustered btree index on col2\n')
    output_file.write('create(idx,db1.tbl4.col2,btree,unclustered)\n')
    output_file.write('--\n')
    output_file.write('--\n')
    output_file.write('-- Load data immediately in the form of a clustered index\n')
    output_file.write('load("/home/cs165/cs165-management-scripts/project_tests_2017/data4_btree.csv")\n')
    output_file.write('--\n')
    output_file.write('-- Testing that the data and their indexes are durable on disk.\n')
    output_file.write('shutdown\n')
    # no expected results
    data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTests20And21(dataTable, dataSize):
    output_file20, exp_output_file20 = data_gen_utils.openFileHandles(20)
    output_file21, exp_output_file21 = data_gen_utils.openFileHandles(21)
    output_file20.write('--\n')
    output_file20.write('-- Query in SQL:\n')
    # selectivity = 
    offset = np.max([1, int(dataSize/5000)])
    offset2 = np.max([2, int(dataSize/2500)])
    val1 = np.random.randint(0, int((dataSize/5) - offset))
    val2 = np.random.randint(0, int((dataSize/5) - offset2))
    # generate test 20
    output_file20.write('-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= {} and col3 < {};\n'.format(val1, val1+offset))
    output_file20.write('-- SELECT col1 FROM tbl4_ctrl WHERE col3 >= {} and col3 < {};\n'.format(val2, val2+offset2))
    output_file20.write('--\n')
    output_file20.write('-- Control test for test13.dsl\n')
    output_file20.write('s1=select(db1.tbl4_ctrl.col3,{},{})\n'.format(val1, val1 + offset))
    output_file20.write('f1=fetch(db1.tbl4_ctrl.col1,s1)\n')
    output_file20.write('print(f1)\n')
    output_file20.write('s2=select(db1.tbl4_ctrl.col3,{},{})\n'.format(val2, val2 + offset2))
    output_file20.write('f2=fetch(db1.tbl4_ctrl.col1,s1)\n')
    output_file20.write('print(f2)\n')
    # generate test 21
    output_file21.write('--\n')
    output_file21.write('-- tbl3 has a secondary b-tree tree index on col2, and a clustered index on col1 with the form of a sorted column\n')
    output_file21.write('-- testing for correctness\n')
    output_file21.write('--\n')
    output_file21.write('-- Query in SQL:\n')
    output_file21.write('-- SELECT col1 FROM tbl3_ WHERE col3 >= {} and col3 < {};\n'.format(val1, val1+offset))
    output_file21.write('-- SELECT col1 FROM tbl3_ctrl WHERE col3 >= {} and col3 < {};\n'.format(val2, val2+offset2))
    output_file21.write('--\n')
    output_file21.write('-- since col1 has a clustered index, the index is expected to be used by the select operator\n')
    output_file21.write('s1=select(db1.tbl4.col3,{},{})\n'.format(val1, val1 + offset))
    output_file21.write('f1=fetch(db1.tbl4.col1,s1)\n')
    output_file21.write('print(f1)\n')
    output_file21.write('s2=select(db1.tbl4.col3,{},{})\n'.format(val2, val2 + offset2))
    output_file21.write('f2=fetch(db1.tbl4.col1,s1)\n')
    output_file21.write('print(f2)\n')
    # generate expected results
    dfSelectMask1 = (dataTable['col3'] >= val1) & (dataTable['col3'] < (val1 + offset))
    dfSelectMask2 = (dataTable['col3'] >= val2) & (dataTable['col3'] < (val2 + offset2))
    output1 = dataTable[dfSelectMask1]['col1']
    output2 = dataTable[dfSelectMask2]['col1']
    for exp_output_file in [exp_output_file20, exp_output_file21]:
        exp_output_file.write(data_gen_utils.outputPrint(output1))
        exp_output_file.write('\n\n')
        exp_output_file.write(data_gen_utils.outputPrint(output2))
        exp_output_file.write('\n')
    data_gen_utils.closeFileHandles(output_file20, exp_output_file20)
    data_gen_utils.closeFileHandles(output_file21, exp_output_file21)

def createTest22(dataTable, dataSize):
    output_file, exp_output_file = data_gen_utils.openFileHandles(22)
    offset = np.max([1, int(dataSize/10)])
    offset2 = 2000
    val1 = np.random.randint(0, int((dataSize/5) - offset))
    val2 = np.random.randint(0, 8000)
    output_file.write('-- Test for a clustered index select followed by a second predicate\n')
    output_file.write('--\n')
    output_file.write('-- Query in SQL:\n')
    output_file.write('-- SELECT sum(col1) FROM tbl3_ WHERE (col3 >= {} and col3 < {}) AND (col2 >= {} and col2 < {});\n'.format(val1, val1+offset, val2, val2+offset))
    output_file.write('--\n')
    output_file.write('s1=select(db1.tbl4.col3,{},{})\n'.format(val1, val1 + offset))
    output_file.write('f1=fetch(db1.tbl4.col2,s1)\n')
    output_file.write('s2=select(f1,{},{})\n'.format(val2, val2 + offset2))
    output_file.write('f1=fetch(db1.tbl4.col1,s2)\n')
    output_file.write('a1=sum(f1)\n')
    output_file.write('print(a1)\n')
    # generate expected results
    dfSelectMask1 = (dataTable['col3'] >= val1) & (dataTable['col3'] < (val1 + offset))
    dfSelectMask2 = (dataTable['col2'] >= val2) & (dataTable['col2'] < (val2 + offset2))
    values = dataTable[dfSelectMask1 & dfSelectMask2]['col1']
    exp_output_file.write(str(values.sum()))
    data_gen_utils.closeFileHandles(output_file, exp_output_file)



def generateMilestoneThreeFiles(dataSize):
    randomSeed = 47
    np.random.seed(randomSeed)
    frequentVal1, frequentVal2, dataTable = generateDataMilestone3(dataSize)  
    createTest18()
    createTest19()
    createTests20And21(dataTable, dataSize)
    createTest22(dataTable, dataSize)

def main(argv):
    dataSize = int(argv[0])
    if len(argv) > 1:
        randomSeed = argv[1]
    else:
        randomSeed = 47
    generateMilestoneThreeFiles(dataSize)

if __name__ == "__main__":
    main(sys.argv[1:])

