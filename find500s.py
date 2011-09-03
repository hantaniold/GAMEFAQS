import os
import sys

#Grabs the 500-post threads from the message data
#Make sure you modifiy "hadoop fs -cat <yourdirwithmessages>" accordingly
infile = open(sys.argv[1],"r")
splits = sys.argv[1].split("/")
print splits
outfile = splits[len(splits)-1]
for line in infile:
	os.system("hadoop fs -cat wii-msg-*/"+line.strip("\n")+" | grep $'\t'500$'\t' -B 499 >> /home/seanhogan/GAMEFAQS/output/"+outfile)
	print "did "+line
	
