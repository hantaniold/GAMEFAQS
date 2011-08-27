from BeautifulSoup import BeautifulSoup 
import random
import getMsgMeta
import time
import urllib2
import subprocess
import sys, os

#Ad-hoc crawler for gamefaqs message boards.
#Sean Hogan, 2011
#gfCrawlMR.py Is a version of gfCrawl.py that will work with a mapreduce application I will make. An independent script takes in a system, and returns a list of ALL boards for that game system.
#This takes in -ONE- of those boards, and explores it completely.
#It does not include split boards other than the main boards. Maybe that will change later
#So mapreduce will just run this as many times as needed to get all the data. Yep...

#Things you can modify:
#In explore_topic(), you can change MAX_PAGES to change how many pages per topic are crawled.
#In explore_topic(), you can comment out the write to the topic data file, or msg data file.
#In explore_board(), you can change MAX_BOARD_PAGES to change how many pages per board are crawled from the starting link.
#In explore_board(), you can change the offset of the start link (cur_link).
#Near the bottom you can change the random numbers used in determining time between HTTP requests.
#Of course you will need to change the output filepath. Running this script locally you don't need the full paths. Running them through Hadoop you do.


#10 boards * MAX_board pages/board * 20 Topics/board page * MAX_PAGES = Total pages. 

TOPIC_CNT = 0
PAGE_CNT = 0

#This explores topics.
#In any case, we (1) download the page, (2) there is no 2 
#(3) find the number of pages in the topic, (4) get the topic and board title
#(5) parse out the messages, (6) parse out the names, (7) format the output file
#(8) check if we need to go through another page and if so, change the link and iterate
#(9) Finally, write separate topic data to another file, and exit.
def exploreTopic(link):
	MAX_PAGES = 50 #Only want 50 pages per topic, max
	inMsgBody, text, pages, curpage, pagelink = 0,"",0,0,""
	postNo = 1 #Keep track of post number
	postCounter = 0
	creator = "" #Name of topic creator
	boardTitle = ""
	topicTitle = "" 
	createDate,createTime,lastPostDate,lastPostTime = "","","",""
	while(1):
		global PAGE_CNT
		PAGE_CNT = PAGE_CNT + 1
		state = 0 #Have we gotten past all the links that wouclassld match "]" yet and gotten to the text
		try:
			page = urllib2.urlopen(link+pagelink)
			time.sleep(random.randint(0,ub) % random.randint(1,ubt))
		except urllib2.URLError:
			print "Couldn't get "+link+pagelink+"\n"+"Skipping this topic."
			return #Sacrifice this topic...	

		soup = BeautifulSoup(page)
	

		if postNo == 1:
			words = soup.findAll(attrs={"class" : "u_pagenav"})
			lines = str(BeautifulSoup(str(words)).prettify()).split('\n')
			for line in lines:
				if line != '' and line.split()[0] == "Page":
					try:
						pages = int(line.split()[3])
					except ValueError:
						print "ValueError!!! here: "+line
		try:
			topicTitle = str((soup.h2).renderContents())

			boardTitle = str((soup.h1).renderContents())
		except AttributeError:
			os.system("rm "+timestamp_filename)
			os.system("rm "+out_file_board)
			exit(0)

#Look for the attribute "class" with the value "msg_body" or "name"
#since that respectively indicates the message text, and the user post.
#Parsing that is SIGNIFICANTLY EASIER than before, and will also give me the name of the user...

		msg_list = [] #Holds messages in order they were posted
		msgs = soup.findAll(attrs={"class" : "msg_body"})
		msgSoup = BeautifulSoup(str(msgs))
		msgs = str(msgSoup.prettify())
		msg = ""
		f = open(out_file_topic,"w")
		f.write(msgs)
		f.close()
		f = open(out_file_topic,"r")
#Below: Iterate through prettified messages. Change state based on if we're in a tag...later, check for included html things, throw them out. Also, make this into a thing with names as well
		
		for line in f:
		
			if state == 1:
				if "</div>" in line:
					msg_list = msg_list + [msg]
					state = 0
					msg = ""
				else:
					msg = msg + " " + line.rstrip('\n')

			if state == 0:
				if """<div class="msg_body">""" in line:
					state = 1
		f.close()
		state = 0 #Reset state just in case.

		#contains a list of however many posts this page had - containing NAME POST_ID DATE TIME
		metaInfo = getMsgMeta.getMsgMeta(soup,0)

		

		counter = 0
		lastInfo = ""
		for info in metaInfo:
			lastInfo = info
			postCounter = postCounter + 1
			if postCounter == 1:
				createDate,createTime = info.split('\t')[2],info.split('\t')[3]
				creator = info.split("\t")[0] 
#.....so we should make output that is MapReduce friendly, to be fed to a tab-delimited scanner. Eventually, "text" will be written to the file the user specifies, so each line is the board title, the topic title, the current poster, the post number, and the message. We also can check who the topic creator is, and we increment things as necessary.
			text = text+ sys.argv[3] + "\t" + boardTitle+"\t"+topicTitle+"\t" +info + "\t" + msg_list[counter] + "\n"
			counter = counter + 1

		os.system("rm "+out_file_topic)
		
		print "Page "+str(curpage)+" of "+link
		curpage = curpage + 1
		
#Just check if we can stop looking through the topic.
		if curpage >= MAX_PAGES or curpage >= pages:
			out = open(out_file,"a")
			out.write(text)
			out.close()
			f.close()
			break
		pagelink = "?page="+str(curpage)
	
#After exiting the main while loop that iterates through the topic, we can use some variables we assigned to write to a separate data that will tell us info about creators, etc
	lastPostDate,lastPostTime = lastInfo.split('\t')[2],lastInfo.split('\t')[3]
	topicData = open(out_file+"topicsData","a")
	topicData.write(sys.argv[3]+"\t"+boardTitle+"\t"+creator+"\t"+topicTitle+"\t"+str(postCounter)+"\t"+createDate+"\t"+createTime+"\t"+lastPostDate+"\t"+lastPostTime+"\n")
	topicData.close()
#######



#Input: the extension /path/to/whatever for off of gamefaqs.
#Output: Well, it outputs the full path to a topic for exploreTopic to use.

#Here's how we can think of this:
#We use some ugly ad-hoc parsing to deal with the structure we get from
#lynx --dump. Since every board consists of pages of 20 topics, we keep a list of 20 topics. We call exploreTopic on all of these.
#Then depending on MAX_BOARD_PAGES, we get a new set of 20 topics from the next page (if it exists), then do it all again.

def exploreBoard(link):
	page_count, state, first_topic_id,num_got  = 0,0,"",0
	MAX_BOARD_PAGES = 1#Important. How many pages you visit.
	link_prefix = link.split("=")[0] #...?page
	cur_page = int(link.split("=")[1])
	cur_link = link_prefix+"="+str(cur_page) #Where you start in reference to input.
	boards_visited = 0
	startpg = "1" 
	global TOPIC_CNT

	while(1):
		os.system("lynx --dump www.gamefaqs.com"+cur_link+" > "+out_file_board)
		time.sleep(random.randint(0,ub) % random.randint(1,ubt))

		f = open(out_file_board,"r")
		print "Exploring: Topics in page "+str(cur_page+1)+" of "+cur_link
		topic_links = []
		for line in f:
#Hardcoded till further notice! Each board displays 20 topics. Bad things happen if we keep going.

			if num_got == 20: 
				break
			if "Page 1 of" in line: #Find # pages in board
				words = line.split()
				page_count = int(words[3])
				if page_count == 0: #Empty board
					os.system("rm "+out_file_board)
					os.system("rm "+timestamp_filename)
					exit(0)
				print "Pages of topics: "+words[3]

			#Sometimes the lynx output wraps a line cause the topic title is too long. this helps to ignore that case.
			if state == 3:
				num_got = num_got + 1
				words = line.split()
				topic_links = topic_links+[words[1]]
			if state == 2:
				if first_topic_link in line:
					state = 3
					words = line.split()
					topic_links = topic_links+[words[1]]
					num_got = num_got + 1
			if state == 1:
#Ugly way to know when we can safely look for links to topics.
				first_topic_link = line.split()[0][1:3]+". http"
				state = 2
			if "Created By" in line:
				print "hi"
				state = 1
				
		print str(len(topic_links))
		for topic_link in topic_links:
			print "Exploring the topic "+topic_link
			exploreTopic(topic_link)
			TOPIC_CNT = TOPIC_CNT + 1
		f.close()
		if topic_links != []: #Sometimes the call to lynx fails and topic_links is empty. This results in skipping an entire page of topics.
			cur_page = cur_page + 1
		else:
			time.sleep(30)
		num_got, topic_links, state = 0,[],0
		os.system("rm "+out_file_board)
		cur_link = link_prefix+"="+str(cur_page)
		boards_visited = boards_visited + 1
		#annoyingly enough, gamefaqs 0-indexes their pages, so ?page=8 is page 9, ?page = 0 is page 1..
		if boards_visited  >= MAX_BOARD_PAGES:
			os.system("rm "+out_file_board)
			break

##########
## MAIN ##
##########


ub = random.randint(5,15) #To make waits between HTTP requests and avoid being rate-limited
ubt = random.randint(1,13)

if (len(sys.argv) < 2):
	print "Usage: python gfCrawlMR.py <Output file> <board link> <system>"
	exit(0)
out_file = "/home/seanhogan/GAMEFAQS/output/"+sys.argv[1]
out_file_topic = out_file+"topic"
out_file_board = out_file+"board"


#Timestamp files help me to debug problems related to this crawler.
#So I can know when something started, if it failed, what link - was it a network thing or a crawler thing?

timestamp_filename = out_file+"-"+time.strftime("%b%d-%H:%M:%S",time.localtime())
timestamp = open(timestamp_filename,"w")
timestamp.write(sys.argv[2])
timestamp.close()



exploreBoard(sys.argv[2])
os.system("rm "+timestamp_filename)

print "Looked at "+str(TOPIC_CNT)+" topics."
print "Looked through "+str(PAGE_CNT)+" pages among those topics."
print "That's an average of "+str(float(PAGE_CNT/TOPIC_CNT))+" pages per topic."
os.system("rm "+out_file_topic)

##END##

