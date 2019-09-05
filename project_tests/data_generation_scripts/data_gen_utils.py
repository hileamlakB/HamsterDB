#!/usr/bin/python
import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd

def openFileHandles(testNum, TEST_DIR=""):
	# if a directory base specified, we want to add the trailing separator `/`
	if TEST_DIR != "":
		TEST_DIR += "/"
	if testNum < 10:
		output_file = open(TEST_DIR + "test0{}gen.dsl".format(testNum),"w")
		exp_output_file = open(TEST_DIR + "test0{}gen.exp".format(testNum),"w")
	else:
		output_file = open(TEST_DIR + "test{}gen.dsl".format(testNum),"w")
		exp_output_file = open(TEST_DIR + "test{}gen.exp".format(testNum),"w")
	return output_file, exp_output_file

def closeFileHandles(output_file, exp_output_file):
	output_file.flush()
	exp_output_file.flush()
	output_file.close()
	exp_output_file.close()

def generateHeaderLine(dbName, tableName, numColumns):
	outputString = []
	for i in range(1, numColumns+1):
		outputString.append('{}.{}.col{}'.format(dbName, tableName, i))
	#outputString.append('{}.{}.col{}'.format(dbName, tableName, numColumns))
	return outputString

def outputPrint(pandasArray):
	if pandasArray.shape[0] == 0:
		return ''
	else:
		return pandasArray.to_string(header=False,index=False)

