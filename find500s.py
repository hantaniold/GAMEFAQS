import os
import sys
#Sean Hogan, 2011
#CHANGE THIS!!!
OUTPUTDIR = "/home/seanhogan/GAMEFAQS/output/"
HDFSINPUTDIR = "wii-msg-*/"

#Grabs the 500-post threads from the message data
#Make sure you modifiy "hadoop fs -cat <yourdirwithmessages>" accordingly
infile = open(sys.argv[1],"r")
splits = sys.argv[1].split("/")
outfile = splits[len(splits)-1]
for line in infile:
	os.system("hadoop fs -cat "+HDFSINPUTDIR+line.strip("\n")+" | grep $'\t'500$'\t' -B 499 >> "+OUTPUTDIR+outfile)
	
