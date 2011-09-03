#Generates data for the 500 threads. Its input is a bunch of 500 topics, messages in sequential orders. Just point this script
#to a file that has line-delimited filenames of the input files
#I use awk of of ls -l output to generate mine

import os
import sys
import time
if (len(sys.argv) != 3):
	print "Usage: python gen500data.py <input file> <output file>"
	exit()
infile = open(sys.argv[1],"r")
nrtopicsexplored = 0

timelist, i = [] , 0

userpostsdict = {}
gametopicsdict = {}
firstposterposts = 0 #Number of posts made by the topic creator, for a moderately interesting stat
gpostslist = []

while (i < 500):
	timelist  += [0]
	gpostslist +=[0]
	i += 1

postindex = 0 #0-indexed

for filename in infile:
	text = open(filename.strip("\n"),"r")

	for message in text:
		try:
			datestamp,timestamp = message.split("\t")[5],message.split("\t")[6]
			AMorPM = timestamp.split()[1]
			l = timestamp.split(":")
			theHour = int(l[0])
			if (AMorPM == "PM" and theHour != 12) or (AMorPM == "AM" and theHour == 12):
				theHour = (theHour + 12 ) % 24
				timestamp = str(theHour) + ":" + l[1] + ":" + l[2]
		
			timetuple = time.strptime(datestamp+" "+timestamp,"%m/%d/%Y %H:%M:%S %p")
			if (postindex == 0):
				firstposttime = time.mktime(timetuple)
			timedelta = time.mktime(timetuple) - firstposttime
			timelist[postindex] += timedelta
			########
			user = message.split("\t")[3]
			if postindex == 0:
				game = message.split("\t")[1]
				if gametopicsdict.has_key(game) == False:
					gametopicsdict[game] = 0
				else:
					gametopicsdict[game] += 1
				firstuser = user
			if user == firstuser:
				firstposterposts += 1	

			if userpostsdict.has_key(user) == False:
				userpostsdict[user] = 1
			else:
				userpostsdict[user] += 1

			

			postindex = (postindex + 1) % 500
			if postindex == 0:
				postslist = []
				for k, v in userpostsdict.iteritems():
					postslist += [v]
				postslist.sort()
				postslist.reverse()
				i = 0
				for item in postslist:
					gpostslist[i] += item
					i += 1
				userpostsdict = {}
				
				nrtopicsexplored += 1


		except IndexError:
			foo = 0	

		

	text.close()
infile.close()

i = 0
outfiletime = open(sys.argv[2]+"time.dat","w")
outfileposts = open(sys.argv[2]+"posts.dat","w")

while (i < 500):
	timepoint =  str(i)+"\t"+str(timelist[i]/float(nrtopicsexplored))
	postpoint = str(i) + "\t" + str(gpostslist[i]/float(nrtopicsexplored))
	i += 1
	outfiletime.write(timepoint+"\n")
	outfileposts.write(postpoint+"\n")

print "First posts per topic: "+str(firstposterposts/float(nrtopicsexplored))

for k, v in gametopicsdict.iteritems():
	print k+"\t"+str(v)	

outfiletime.close()
outfileposts.close()


