#!/usr/bin/python
import sys, string
from random import choice
import random
from string import ascii_lowercase
from scipy.stats import beta, uniform
import numpy as np
import struct
import pandas as pd

def openFileHandles(testNum):
	if testNum < 10:
		output_file = open("test0{}gen.dsl".format(testNum),"w")
		exp_output_file = open("test0{}gen.exp".format(testNum),"w")
	else:
		output_file = open("test{}gen.dsl".format(testNum),"w")
		exp_output_file = open("test{}gen.exp".format(testNum),"w")
	return output_file, exp_output_file

def closeFileHandles(output_file, exp_output_file):
	output_file.close()
	exp_output_file.close()

def generateHeaderLine(dbName, tableName, numColumns):
	outputString = ''
	for i in range(1, numColumns):
		outputString += '{}.{}.col{},'.format(dbName, tableName, i)
	outputString += '{}.{}.col{}'.format(dbName, tableName, numColumns)
	return outputString

def outputPrint(pandasArray):
	if pandasArray.shape[0] == 0:
		return ''
	else:
		return pandasArray.to_string(header=False,index=False)

