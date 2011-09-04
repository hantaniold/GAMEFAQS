#boardLinkGen.py
#Takes a system and produces an output file of the links to every game of a system
#sean hogan, some point in august 2011

import os
import sys

if (len(sys.argv)) != 3:
	print("Usage: python boardLinkGen.py <console> <PAGES>") 
	exit(0)

PAGES_OF_GAMES = sys.argv[2]
console = sys.argv[1]
#I have this feeling I'm going to forget so this will make sure the number of pages are right 
if console == "wii":
	PAGES_OF_GAMES = 26
if console == "xbox360":
	PAGES_OF_GAMES = 45
root = "www.gamefaqs.com/"+console+"/list-999"
child = "" #
pageNo = 0 #0 indexed
prevLink = "" #This ensures that we only write a link if we don't have a duplicate.
counter = 1
console_file = console+"boards.txt"

outFile = open(console_file,"a")
while(1):
	if pageNo == PAGES_OF_GAMES:
		break
	os.system("wget -q "+root+child)
	os.system("grep /boards/ list-999"+child+" | tail --lines=+2 > temp")	
	f = open("temp","r")
	for line in f:
		#Somehow the below 5 lines are an ad hoc way of dealing with the raw html to pull out the links to boards.
		if line.split()[0] == "<td><a":
			link = line.split("\"")[1]
			outFile.write(link+" "+console+"\n")
			print str(counter)
			counter = counter + 1
			
			
	pageNo = pageNo + 1
	child = "?page="+str(pageNo)
	print "Got"+str(pageNo)
	os.system("rm list-999*")
######
outFile.close()

#SORT AND REMOVE DUPLICATES
os.system("cp "+console+"boards.txt  ohhi.txt")
os.system("sort "+console+"boards.txt > tmpsorted")
f = open("tmpsorted","r")
g = open("finalout","w")

prevLine = ""
i = 1

for line in f:
	if line != prevLine:
		g.write(str(i)+" "+line)
		i = i + 1
		prevLine = line
		
os.system("rm "+console+"boards.txt")
os.system("mv finalout "+console+"boards.txt")
os.system("rm tmpsorted finalout temp")
f.close()
g.close()


