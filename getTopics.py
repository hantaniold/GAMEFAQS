#I screwed up with the wii titles.
#This takes a list of BOARDS and returns the titles without visiting every topic.
#This is 20 times fastersince we don't go into the topoics

import time
import urllib2
from BeautifulSoup import BeautifulSoup
import os
import sys
import random

outputdir = "/home/seanhogan/GAMEFAQS/output/"

f = open(outputdir+sys.argv[1],"w") #Open dummy "99-3" for example, so hadoop doesnt' blow up trying to write 99-3 and 99-3topicsData to HDFS
f.close()
outfile = open(outputdir+sys.argv[1]+"topicsData","a")
link = sys.argv[2]
link_prefix = link.split("=")[0]
cur_page = int(link.split("=")[1])
inlink = link_prefix+"="+str(cur_page)
MAXPAGES = 2

ub = random.randint(5,15) #To make waits between HTTP requests and avoid being rate-limited
ubt = random.randint(1,13)

pagesvisited = 0
while(1):
	page = urllib2.urlopen("http://www.gamefaqs.com"+inlink)
	time.sleep(random.randint(0,ub) % random.randint(1,ubt))
	soup = BeautifulSoup(page)
	boardtitle = str((soup.h1).renderContents())
	bob = soup.findAll("tbody")
	rawstuffwewant = ""
	for thing in bob:
		rawstuffwewant = rawstuffwewant + thing.prettify()

	rawstuffwewant = rawstuffwewant.split("\n")

	TITLESTATE = 0
	NAMESTATE = 1
	COUNTSTATE = 2
	topictitle,name,count = "","",""
	state = -1
	for line in rawstuffwewant:

		if state == COUNTSTATE:
			count = line
			outfile.write(sys.argv[3]+"\t"+boardtitle+"\t"+name+"\t"+topictitle+"\t"+count+"\n")
			topictitle,name,count="","",""
			state = -1
		if state == NAMESTATE:
			name = line
			state = -1
		if state == TITLESTATE:
			topictitle = line
			state = -1

		if line.split()[0] == "<a":
			state = TITLESTATE
		if line.split()[0] == "<span>":
			state = NAMESTATE
		if line.split()[0] == "<td>" and name != "":
			state = COUNTSTATE
	pagesvisited = pagesvisited + 1
	cur_page = cur_page + 1
	inlink = link_prefix+"="+str(cur_page)
	print "Visited "+inlink
	if pagesvisited >= MAXPAGES:
		break
outfile.close()
#print str(names)



