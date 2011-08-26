
#Chunkify.py
#Takes as an input the list of boards from boardgen.py or whatever
#What it does it it looks at all of these boards, sees how many pages there are and "chunks" them into 2 page splits. Which will work with gfcrawlmr.py as long as the settings for max boards is set to 2.
#sean hogan, 8/19/11
import sys
import os
import time

print "Make sure the input is the system and your system+boards.txt file exists"
console_filename = sys.argv[1]+"boards.txt"
console_file = open(console_filename,"r")
output_file = open("split"+console_filename,"a")
text = ""
for line in console_file:
	proceed = 0 #Fault-tolerance with the lynx

	[fid,link,console] = line.split() #file prefix, link to board, game console...
	while(1):
		os.system("lynx --dump www.gamefaqs.com"+link+" > chunkify.tmp")
		current_board = open("chunkify.tmp","r")
		for another_line in current_board:
			if "Page 1 of" in another_line: #if so we can get the # of pages in the board
				proceed = 1 #We got it so we can go on to the next line of input
				numpages = int(another_line.split()[3])
				if numpages == 0: #Don't bother writing a link to an empty board.
					break
				i = 0 #i just counts every other page so we write that many as links to visit
				while i < numpages:
					text = text+fid+"-"+str(i)+" "+link+"?page="+str(i)+" "+console+"\n" #example is 23-4 /boards/4-game?page=4 console
					i = i + 1
				break
		if proceed == 1: #Stuff to do before getting next line
			os.system("rm chunkify.tmp")
			print("finished "+fid)
			output_file.write(text)
			text = ""
			proceed = 0
			break
		else:
			print("Oops, didn't get "+link+". Trying again...")
			time.sleep(7)

output_file.close()
console_file.close()
