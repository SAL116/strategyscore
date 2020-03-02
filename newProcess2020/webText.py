from bs4 import BeautifulSoup,SoupStrainer
import requests
import sys 
from bs4.element import Comment
from dateutil.parser import parse
from trash import isTrash
import datetime
import time

def target_date(website,year):
	"""
	Will reuturn the last timestamp from 
	the specified year
	"""
	try:
		r = requests.get("https://web.archive.org/__wb/calendarcaptures?url="+website+"&selected_year="+str(year),timeout=5).json()
		for m in range(12):
			for w in range(4):
				value = r[-1-m][-1-w][0]
				if 'ts' in value.keys():
					timestamp = value['ts'][0]
					if timestamp is not None:
						return timestamp
				else:
					continue
		# except:
		# 	return None
		return None
	except:
		return None
		
def tag_visible(element):
	if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
		return False
	if isinstance(element, Comment):
		return False
	return True

def text_from_html(soup): # take in a bs object, not just the link name, need process before hand 
	
	texts = soup.findAll(text=True)
	visible_texts = filter(tag_visible, texts)  
	raw = u" ".join(t.strip() for t in visible_texts)
	return str(raw.replace('\t',' ').replace('\\n',' ').replace('\n',' ').strip().encode('ascii', 'ignore'))

def page2text(soup):
	##Function for scrape text from html	
	#try:
	content = text_from_html(soup)
	if len(content)<1000 or content is None or isTrash(content):
		return " "
	else:
		return content
	# except:
	# 	return None
def getSoup(url):
	try:
		resp = requests.get(url,timeout=3)
		return BeautifulSoup(resp.content, 'html.parser')
	except:
		return None
def homePage(url,timestamp):
	if timestamp is None:
		return None
	url = "https://web.archive.org/web/"+str(timestamp)+"/"+url+"/"
	soup = getSoup(url)
	return soup

def findPages(soup,timestamp):
	toFetch = list()
	About=["ABOUT","COMPANY","PRODUCTS","OVERVIEW","SERVICES","FEATURES","SOLUTIONS","WHAT WE DO","WHO WE ARE"]
	for link in soup.find_all('a', href=True):
		up = link.text.upper()
		if up is None or len(up)<2:
			continue
		for a in About:
			if (a == up or a in up.split(' ')) and checkDate(link,str(timestamp)):
				toFetch.append(getFullLink(link))
				break
	return toFetch

def checkDate(href,timestamp):
	href = href['href']
	try:
		linkTime = href[href.find('/web/')+5:href.find('/web/')+5+14]
		ogTime = datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S")
		linkTime = datetime.datetime.strptime(linkTime, "%Y%m%d%H%M%S")
		if abs((linkTime-ogTime).days) < 365:
			return True
		else:
			return False
	except:
		return False

def getFullLink(href):
	href = href['href']
	if href.startswith('http') or href.startswith('https'):
		return href
	else:
		return "https://web.archive.org"+href

def getText(website,year):
	#this is the homepage object
	timestamp = target_date(website,2008)
	soup = homePage(website,timestamp)
	if soup is None:
		return None
	#this is the list of links form the homepage 
	toFetch = findPages(soup,timestamp)
	#this is the text form the homepage
	#at least we will have information form the homepage
	text = page2text(soup)
	#looping links and get texts
	for page in toFetch:
		soup = getSoup(page)
		if soup is None:
			continue
		text += ("  " + page2text(soup))
	if len(text.strip()) > 1000:
		return text
	else:
		return None
