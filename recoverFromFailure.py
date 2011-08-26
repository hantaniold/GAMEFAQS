#recoverFromFailure.py
#For dealing with those times where you download 90% of links and you get rate-limited at the end and it gets shot to hell
#Takes output of hadoop fs -ls msgdata, compares against original board links, returns a file of links left to explore. SANITY!

import sys
import os

if len(sys.argv) != 4 :
	print "Usage: <hadoop fs -ls msgdata output file> <original mapreduce input> <desired output name>"
	exit(1)

lsInput = open(sys.argv[1],"r")
mapredInput = open(sys.argv[2],"r")
output = open(sys.argv[3],"a")

fileIDList = []
for line in lsInput:
	fileID = line.split("/")[4].rstrip("\n") #e.g. "1007-10"
	fileIDList = fileIDList+[fileID]

for line in mapredInput:
	mrID = line.split()[0]
	if mrID not in fileIDList:
		output.write(line)

lsInput.close()
mapredInput.close()
output.close()

