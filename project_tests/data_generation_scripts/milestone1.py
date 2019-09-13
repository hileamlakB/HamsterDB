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
import math

# note this is the base path where we store the data files we generate
TEST_BASE_DIR = "/cs165/generated_data"

# note this is the base path that _POINTS_ to the data files we generate
DOCKER_TEST_BASE_DIR = "/cs165/staff_test"

#
# Example usage: 
#   python milestone1.py 10000 42 ~/repo/cs165-docker-test-runner/test_data /cs165/staff_test
#

# PRECISION FOR AVG OPERATION
PLACES_TO_ROUND = 2

############################################################################
# Notes: You can generate your own scripts for generating data fairly easily by modifying this script.
############################################################################

def generateDataFileMidwayCheckin():
	outputFile = TEST_BASE_DIR + '/data1_generated.csv'
	header_line = data_gen_utils.generateHeaderLine('db1', 'tbl1', 2)
	column1 = list(range(0,1000))
	column2 = list(range(10,1010))
	#### For these 3 tests, the seed is exactly the same on the server.
	np.random.seed(47)
	np.random.shuffle(column2)
	#outputTable = np.column_stack((column1, column2)).astype(int)
	outputTable = pd.DataFrame(list(zip(column1, column2)), columns =['col1', 'col2'])
	outputTable.to_csv(outputFile, sep=',', index=False, header=header_line, line_terminator='\n')
	return outputTable

def createTestOne():
	# write out test
	output_file, exp_output_file = data_gen_utils.openFileHandles(1, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Load+create Data and shut down of tbl1 which has 1 attribute only\n')
	output_file.write('create(db,\"db1\")\n')
	output_file.write('create(tbl,\"tbl1\",db1,2)\n')
	output_file.write('create(col,\"col1\",db1.tbl1)\n')
	output_file.write('create(col,\"col2\",db1.tbl1)\n')
	output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/data1_generated.csv\")\n')
	output_file.write('shutdown\n')
	# generate expected results
	data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTestTwo(dataTable):
	# write out test
	output_file, exp_output_file = data_gen_utils.openFileHandles(2, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Test Select + Fetch\n')
	output_file.write('--\n')
	### Part 1
	# write query out
	selectValLess = 20
	output_file.write('-- SELECT col1 FROM tbl1 WHERE col1 < {};\n'.format(selectValLess))
	output_file.write('s1=select(db1.tbl1.col1,null,{})\n'.format(selectValLess))
	output_file.write('f1=fetch(db1.tbl1.col1,s1)\n')
	output_file.write('print(f1)\n')
	output_file.write('--\n')
	# generate expected results
	dfSelectMaskLT = dataTable['col1'] < selectValLess
	output = dataTable[dfSelectMaskLT]['col1']
	exp_output_file.write(output.to_string(header=False,index=False))
	exp_output_file.write('\n\n')
	#write query 2 out
	selectValGreater = 987
	output_file.write('-- SELECT col2 FROM tbl1 WHERE col1 >= {};\n'.format(selectValGreater))
	output_file.write('s2=select(db1.tbl1.col1,{},null)\n'.format(selectValGreater))
	output_file.write('f2=fetch(db1.tbl1.col2,s2)\n')
	output_file.write('print(f2)\n')
	dfSelectMaskGT = dataTable['col1'] >= selectValGreater
	output = dataTable[dfSelectMaskGT]['col2']
	exp_output_file.write(output.to_string(header=False,index=False))
	exp_output_file.write('\n\n')
	data_gen_utils.closeFileHandles(output_file, exp_output_file)


def createTestThree(dataTable):
	# prelude
	output_file, exp_output_file = data_gen_utils.openFileHandles(3, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Test Multiple Selects + Average\n')
	output_file.write('--\n')
	# query
	selectValLess = 956
	selectValGreater = 972
	output_file.write('-- SELECT avg(col2) FROM tbl1 WHERE col1 >= {} and col1 < {};\n'.format(selectValLess, selectValGreater))
	output_file.write('s1=select(db1.tbl1.col1,956,972)\n')
	output_file.write('f1=fetch(db1.tbl1.col2,s1)\n')
	output_file.write('a1=avg(f1)\n')
	output_file.write('print(a1)\n')
	# generate expected result
	dfSelectMaskGT = dataTable['col1'] >= selectValLess
	dfSelectMaskLT = dataTable['col1'] < selectValGreater
	output = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col2']
	exp_output_file.write(str(np.round(output.mean(), PLACES_TO_ROUND)))
	exp_output_file.write('\n')
	data_gen_utils.closeFileHandles(output_file, exp_output_file)

def generateDataFile2(dataSizeTableTwo):
	outputFile = TEST_BASE_DIR + '/' + 'data2_generated.csv'
	header_line = data_gen_utils.generateHeaderLine('db1', 'tbl2', 4)
	outputTable = pd.DataFrame(np.random.randint(-1 * dataSizeTableTwo/2, dataSizeTableTwo/2, size=(dataSizeTableTwo, 4)), columns =['col1', 'col2', 'col3', 'col4'])
	outputTable['col2'] = outputTable['col2'] + outputTable['col1']
	# This is going to have many, many duplicates!!!!
	outputTable['col3'] = np.random.randint(0,100, size = (dataSizeTableTwo))
	outputTable['col4'] = np.random.randint(2**31 - 10000, 2**31, size = (dataSizeTableTwo))
	outputTable.to_csv(outputFile, sep=',', index=False, header=header_line, line_terminator='\n')
	return outputTable

def createTestFour(dataTable):
	# prelude
	output_file, exp_output_file = data_gen_utils.openFileHandles(4, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Load Test Data 2\n')
	output_file.write('--\n')
	output_file.write('-- Load+create+insert Data and shut down of tbl2 which has 4 attributes\n')
	output_file.write('create(tbl,\"tbl2\",db1,4)\n')
	output_file.write('create(col,\"col1\",db1.tbl2)\n')
	output_file.write('create(col,\"col2\",db1.tbl2)\n')
	output_file.write('create(col,\"col3\",db1.tbl2)\n')
	output_file.write('create(col,\"col4\",db1.tbl2)\n')
	output_file.write('load(\"'+DOCKER_TEST_BASE_DIR+'/data2_generated.csv\")\n')
	output_file.write('relational_insert(db1.tbl2,-1,-11,-111,-1111)\n')
	output_file.write('relational_insert(db1.tbl2,-2,-22,-222,-2222)\n')
	output_file.write('relational_insert(db1.tbl2,-3,-33,-333,-2222)\n')
	output_file.write('relational_insert(db1.tbl2,-4,-44,-444,-2222)\n')
	output_file.write('relational_insert(db1.tbl2,-5,-55,-555,-2222)\n')
	output_file.write('relational_insert(db1.tbl2,-6,-66,-666,-2222)\n')
	output_file.write('relational_insert(db1.tbl2,-7,-77,-777,-2222)\n')
	output_file.write('relational_insert(db1.tbl2,-8,-88,-888,-2222)\n')
	output_file.write('relational_insert(db1.tbl2,-9,-99,-999,-2222)\n')
	output_file.write('relational_insert(db1.tbl2,-10,-11,0,-34)\n')
	output_file.write('shutdown\n')

	# columns need to align for append to work
	deltaTable = pd.DataFrame([[-1, -11, -111, -1111],
		[-2, -22, -222, -2222],
		[-3, -33, -333, -2222],
		[-4, -44, -444, -2222],
		[-5, -55, -555, -2222],
		[-6, -66, -666, -2222],
		[-7, -77, -777, -2222],
		[-8, -88, -888, -2222],
		[-9, -99, -999, -2222],
		[-10, -11, 0, -34]], columns=['col1', 'col2', 'col3', 'col4'])
	
	dataTable = dataTable.append(deltaTable)
	data_gen_utils.closeFileHandles(output_file, exp_output_file)
	return dataTable

## NOTE: approxSelectivity should be between 0 and 1
def createTestFive(dataTable, dataSizeTableTwo, approxSelectivity):
	# prelude
	output_file, exp_output_file = data_gen_utils.openFileHandles(5, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Summation\n')
	output_file.write('--\n')
	# query
	offset = int(approxSelectivity * dataSizeTableTwo)
	highestHighVal = int((dataSizeTableTwo/2) - offset)
	selectValLess = np.random.randint(int(-1 * (dataSizeTableTwo/2)), highestHighVal)
	selectValGreater = selectValLess + offset
	output_file.write('-- SELECT SUM(col3) FROM tbl2 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess, selectValGreater))
	output_file.write('s1=select(db1.tbl2.col1,{},{})\n'.format(selectValLess, selectValGreater))
	output_file.write('f1=fetch(db1.tbl2.col3,s1)\n')
	output_file.write('a1=sum(f1)\n')
	output_file.write('print(a1)\n')
	output_file.write('--\n')
	output_file.write('-- SELECT SUM(col1) FROM tbl2;\n')
	output_file.write('a2=sum(db1.tbl2.col1)\n')
	output_file.write('print(a2)\n')
	# generate expected results
	dfSelectMaskGT = dataTable['col1'] >= selectValLess
	dfSelectMaskLT = dataTable['col1'] < selectValGreater
	output = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col3']
	exp_output_file.write(str(int(output.sum())))
	exp_output_file.write('\n')
	exp_output_file.write(str(int(dataTable['col1'].sum())))
	exp_output_file.write('\n')
	data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTestSix(dataTable, dataSizeTableTwo, approxNumOutputTuples):
	# prelude
	output_file, exp_output_file = data_gen_utils.openFileHandles(6, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Addition\n')
	output_file.write('--\n')
	# query
	offset = approxNumOutputTuples
	highestHighVal = int((dataSizeTableTwo/2) - offset)
	selectValLess = np.random.randint(int(-1 * (dataSizeTableTwo/2)), highestHighVal)
	selectValGreater = selectValLess + offset
	output_file.write('-- SELECT col2+col3 FROM tbl2 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess, selectValGreater))
	output_file.write('s11=select(db1.tbl2.col1,{},{})\n'.format(selectValLess, selectValGreater))
	output_file.write('f11=fetch(db1.tbl2.col2,s11)\n')
	output_file.write('f12=fetch(db1.tbl2.col3,s11)\n')
	output_file.write('a11=add(f11,f12)\n')
	output_file.write('print(a11)\n')
	# generate expected results
	dfSelectMaskGT = dataTable['col1'] >= selectValLess
	dfSelectMaskLT = dataTable['col1'] < selectValGreater
	output = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col3'] + dataTable[dfSelectMaskGT & dfSelectMaskLT]['col2']
	exp_output_file.write(output.to_string(header=False,index=False))
	exp_output_file.write('\n')
	data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTestSeven(dataTable, dataSizeTableTwo, approxNumOutputTuples):
	output_file, exp_output_file = data_gen_utils.openFileHandles(7, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Subtraction\n')
	output_file.write('--\n')
	offset = approxNumOutputTuples
	highestHighVal = int((dataSizeTableTwo/2) - offset)
	selectValLess = np.random.randint(int(-1 * (dataSizeTableTwo/2)), highestHighVal)
	selectValGreater = selectValLess + offset
	output_file.write('-- SELECT col3-col2 FROM tbl2 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess, selectValGreater))
	output_file.write('s21=select(db1.tbl2.col1,{},{})\n'.format(selectValLess, selectValGreater))
	output_file.write('f21=fetch(db1.tbl2.col2,s21)\n')
	output_file.write('f22=fetch(db1.tbl2.col3,s21)\n')
	output_file.write('s21=sub(f22,f21)\n')
	output_file.write('print(s21)\n')
	# generate expected results
	dfSelectMaskGT = dataTable['col1'] >= selectValLess
	dfSelectMaskLT = dataTable['col1'] < selectValGreater
	output = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col3'] - dataTable[dfSelectMaskGT & dfSelectMaskLT]['col2']
	exp_output_file.write(output.to_string(header=False,index=False))
	exp_output_file.write('\n')
	data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTestEight(dataTable, dataSizeTableTwo, approxSelectivity):
	output_file, exp_output_file = data_gen_utils.openFileHandles(8, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Min,Max\n')
	output_file.write('--\n')
	offset = int(approxSelectivity * dataSizeTableTwo)
	highestHighVal = int((dataSizeTableTwo/2) - offset)
	selectValLess = np.random.randint(int(-1 * (dataSizeTableTwo/2)), highestHighVal)
	selectValGreater = selectValLess + offset
	output_file.write('-- Min\n')
	output_file.write('-- SELECT min(col1) FROM tbl2 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess, selectValGreater))
	output_file.write('s1=select(db1.tbl2.col1,{},{})\n'.format(selectValLess, selectValGreater))
	output_file.write('f1=fetch(db1.tbl2.col1,s1)\n')
	output_file.write('m1=min(f1)\n')
	output_file.write('print(m1)\n')
	output_file.write('--\n')
	output_file.write('-- SELECT min(col2) FROM tbl2 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess, selectValGreater))
	output_file.write('f2=fetch(db1.tbl2.col2,s1)\n')
	output_file.write('m2=min(f2)\n')
	output_file.write('print(m2)\n')
	output_file.write('--\n')
	output_file.write('--\n')
	output_file.write('-- Max\n')
	output_file.write('-- SELECT max(col1) FROM tbl2 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess, selectValGreater))
	output_file.write('s21=select(db1.tbl2.col1,{},{})\n'.format(selectValLess, selectValGreater))
	output_file.write('f21=fetch(db1.tbl2.col1,s21)\n')
	output_file.write('m21=max(f21)\n')
	output_file.write('print(m21)\n')
	output_file.write('--\n')
	output_file.write('-- SELECT max(col2) FROM tbl2 WHERE col1 >= {} AND col1 < {};\n'.format(selectValLess, selectValGreater))
	output_file.write('f22=fetch(db1.tbl2.col2,s21)\n')
	output_file.write('m22=max(f22)\n')
	output_file.write('print(m22)\n')
	# generate expected results
	dfSelectMaskGT = dataTable['col1'] >= selectValLess
	dfSelectMaskLT = dataTable['col1'] < selectValGreater
	output1 = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col1'].min()
	output2 = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col2'].min()
	output3 = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col1'].max()
	output4 = dataTable[dfSelectMaskGT & dfSelectMaskLT]['col2'].max()
	exp_output_file.write(str(output1) + '\n')
	exp_output_file.write(str(output2) + '\n')
	exp_output_file.write(str(output3) + '\n')
	exp_output_file.write(str(output4) + '\n')
	data_gen_utils.closeFileHandles(output_file, exp_output_file)

def createTestNine(dataTable, dataSizeTableTwo, approxSelectivity):
	output_file, exp_output_file = data_gen_utils.openFileHandles(9, TEST_DIR=TEST_BASE_DIR)
	output_file.write('-- Big Bad Boss Test! Milestone 1\n')
	output_file.write('-- It\'s basically just the previous tests put together\n')
	output_file.write('-- But also, its.... Boss test!\n')
	output_file.write('\n')
	approxSelectivityEachSubclause = np.sqrt(approxSelectivity)
	offset = int(approxSelectivityEachSubclause * dataSizeTableTwo)
	highestHighVal = int((dataSizeTableTwo/2) - offset)
	selectValLess1 = np.random.randint(int(-1 * (dataSizeTableTwo/2)), highestHighVal)
	selectValLess2 = np.random.randint(int(-1 * (dataSizeTableTwo/2)), highestHighVal)
	selectValGreater1 = selectValLess1 + offset
	selectValGreater2 = selectValLess2 + offset
	output_file.write('-- SELECT avg(col1+col2), min(col2), max(col3), avg(col3-col2), sum(col3-col2) FROM tbl2 WHERE (col1 >= {} AND col1 < {}) AND (col2 >= {} AND col2 < {});\n'.format(selectValLess1, selectValGreater1, selectValLess2, selectValGreater2))
	output_file.write('s1=select(db1.tbl2.col1,{},{})\n'.format(selectValLess1, selectValGreater1))
	output_file.write('sf1=fetch(db1.tbl2.col2,s1)\n')
	output_file.write('s2=select(s1,sf1,{},{})\n'.format(selectValLess2, selectValGreater2))
	output_file.write('f1=fetch(db1.tbl2.col1,s2)\n')
	output_file.write('f2=fetch(db1.tbl2.col2,s2)\n')
	output_file.write('f3=fetch(db1.tbl2.col3,s2)\n')
	output_file.write('add12=add(f1,f2)\n')
	output_file.write('out1=avg(add12)\n')
	output_file.write('out2=min(f2)\n')
	output_file.write('out3=max(f3)\n')
	output_file.write('sub32=sub(f3,f2)\n')
	output_file.write('out4=avg(sub32)\n')
	output_file.write('out5=sum(sub32)\n')
	output_file.write('print(out1,out2,out3,out4,out5)\n')
	#expected results
	# generate expected results
	dfSelectMaskGT1 = dataTable['col1'] >= selectValLess1
	dfSelectMaskLT1 = dataTable['col1'] < selectValGreater1
	dfSelectMaskGT2 = dataTable['col2'] >= selectValLess2
	dfSelectMaskLT2 = dataTable['col2'] < selectValGreater2
	totalMask = dfSelectMaskGT1 & dfSelectMaskLT1 & dfSelectMaskGT2 & dfSelectMaskLT2
	col1pluscol2 = dataTable[totalMask]['col1'] + dataTable[totalMask]['col2']
	col3minuscol2 = dataTable[totalMask]['col3'] - dataTable[totalMask]['col2']
	# round any mean
	output1 = np.round(col1pluscol2.mean(), PLACES_TO_ROUND)
	if (math.isnan(output1)):
		exp_output_file.write('0.00,')
	else:
		exp_output_file.write('{:0.2f},'.format(output1))

	output2 = dataTable[totalMask]['col2'].min()
	output3 = dataTable[totalMask]['col3'].max()
	# round any mean
	output4 = np.round(col3minuscol2.mean(), PLACES_TO_ROUND)
	output5 = col3minuscol2.sum()
	exp_output_file.write(str(output2) + ',')
	exp_output_file.write(str(output3) + ',')
	if (math.isnan(output4)):
		exp_output_file.write('0.00,')
	else:
		exp_output_file.write('{:0.2f},'.format(output4))
	if (math.isnan(output5)):
		exp_output_file.write('0,')
	else:
		exp_output_file.write('{}\n'.format(output5))
	data_gen_utils.closeFileHandles(output_file, exp_output_file)

def generateTestsMidwayCheckin(dataTable):
	createTestOne()
	createTestTwo(dataTable)
	createTestThree(dataTable)

def generateOtherMilestoneOneTests(dataTable2, dataSizeTableTwo):
	dataTable2 = createTestFour(dataTable2)
	createTestFive(dataTable2, dataSizeTableTwo, 0.8)
	createTestSix(dataTable2, dataSizeTableTwo, 20)
	createTestSeven(dataTable2, dataSizeTableTwo, 20)
	createTestEight(dataTable2, dataSizeTableTwo, 0.1)
	createTestNine(dataTable2, dataSizeTableTwo, 0.1)

def generateMilestoneOneFiles(dataSizeTableTwo, randomSeed):
	dataTable = generateDataFileMidwayCheckin()
	generateTestsMidwayCheckin(dataTable)
	#### The seed is now a different number on the server! Data size is also different.
	np.random.seed(randomSeed)
	dataTable2 = generateDataFile2(dataSizeTableTwo)
	generateOtherMilestoneOneTests(dataTable2, dataSizeTableTwo)

def main(argv):
	global TEST_BASE_DIR
	global DOCKER_TEST_BASE_DIR

	dataSizeTableTwo = int(argv[0])

	if len(argv) > 1:
		randomSeed = int(argv[1])
	else:
		randomSeed = 47

	# override the base directory for where to output test related files
	if len(argv) > 2:
		TEST_BASE_DIR = argv[2]
		if len(argv) > 3:
			DOCKER_TEST_BASE_DIR = argv[3]


	generateMilestoneOneFiles(dataSizeTableTwo, randomSeed)

if __name__ == "__main__":
	main(sys.argv[1:])
