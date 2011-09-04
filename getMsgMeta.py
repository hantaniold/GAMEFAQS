from BeautifulSoup import BeautifulSoup
import urllib2
import sys

#Sean Hogan, 2011
#link = sys.argv[1]

#page = urllib2.urlopen(link)

#getMsgMeta
#Input: int isArchived: 0 for no, 1 for yes
#	string text: the result of the call to soup.findAll.blahblahblah
#Output: List of lists for: Post date, post time, message ID, poster name


def getMsgMeta(soup,isArchived):
	outtext = [] 
	
	for message in soup.findAll(attrs={"class":"msg_stats_left"}):
		message = message.prettify()
		i = 0
		for chunk in str(message).split('\n'):
			if i == 1:
				id = chunk.split("\"")[1]
			if i == 4:
				name = chunk.split()[0]
			if i == 7 and chunk.split()[0] == "Posted": #Edge case: Sometimes this will be (Moderator) or (Admin) 
				date = chunk.split()[1]
				time = chunk.split()[2]+" "+chunk.split()[3]
				outtext = outtext + [name + "\t" + id + "\t" + date + "\t" + time]
				break
			elif i == 7:
				i = i - 1
			i = i + 1	

	return outtext	


