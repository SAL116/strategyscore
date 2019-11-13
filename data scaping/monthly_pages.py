import os
import re
from bs4 import BeautifulSoup,SoupStrainer
import requests
import pymysql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sys 
from bs4.element import Comment

def timestamp_monthly(website,year):
	timestamp = list()
	try:
		r0 = requests.get("https://web.archive.org/__wb/calendarcaptures?url="+website+"&selected_year="+str(year),timeout=10)
		r = r0.json()
	except:
		return timestamp
	# get list 
	for month in range(len(r)):
		flag = False
		for week in range(len(r[month])):
			if flag == True:
				break
			for day in range(len(r[month][week])):
				try:
					if r[month][week][day]:
						timestamp.append(r[month][week][day]['ts'][0])
						flag = True
						break
				except:
					continue
									
	return timestamp

######GET PAGE CONTENT #############
def tag_visible(element):
	if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
		return False
	if isinstance(element, Comment):
		return False
	return True

def text_from_html(link):
	try:
		resp = requests.get(link,timeout=10)
	except:
		return ''
	try:
		soup = BeautifulSoup(resp.content, 'html.parser')
		texts = soup.findAll(text=True)
		visible_texts = filter(tag_visible, texts)  
		raw = u" ".join(t.strip() for t in visible_texts)
	except:
		return '' 
	
	return str(raw.replace('\t',' ').replace('\\n',' ').replace('\n',' ').strip().encode('ascii', 'ignore'))

### find all pages ###
def webname(a):
	if a.find('.') == -1:
		return None
	if '//www.' in a:
	    r = (a[a.index('//www.')+6:a.rfind('.')])
	elif 'https://' in a:
	    r = (a[a.index('https://')+8:a.rfind('.')])
	elif 'http://' in a:
	    r = (a[a.index('http://')+7:a.rfind('.')])
	else:
		return None
	return r 

def find_links(soup):
	links = list()
	for link in soup.find_all('a', href=True):
		links.append(link['href'])
	return links

def check_valid(sub,companyname):
	sub = sub.lower()
	if ('http' in sub or 'https' in sub or sub.startswith('/web/')) and (companyname not in sub):
		return False
	else:
		return True

def if_discovered(link,discovered):
	try:
		resp2 = requests.get(link,timeout=10)
	except:
		return True,None
	if resp2.url in discovered:
		return True,None
	else:
		return False,resp2.url

def get_all_links(soup,link,companyname):
	newlinks = list()
	for path in find_links(soup):
		if check_valid(path,companyname) == True:
			if path.startswith('/web'):
				newlinks.append('https://web.archive.org'+path)
			elif path.startswith('https://web.archive.org/web/'):
				newlinks.append(path)
			else:
				newlinks.append(link+path)
	return newlinks

def traverse(newlinks,discovered,f):
	for path in newlinks:
		disc = if_discovered(path,discovered)
		if disc[0] == False:
			content = text_from_html(path)
			f.write(content)
			discovered.append(disc[1])
	return None

def year_texts(url,year,uuid):
	timestamp = timestamp_monthly(url,year)
	if len(timestamp) == 0:
		return None
	companyname = webname(url)
	if companyname is None:
		return None
	for ts in timestamp:
		link = "https://web.archive.org/web/"+str(ts)+"/"+url+"/"
		filename = '/home/ubuntu/SIMItxt/startup_may_2019/{}.txt'.format(uuid+'_'+str(ts))
		if os.path.isfile(filename):
			continue 
		f = open(filename, "a+")
		# get all pages 
		try:
			resp = requests.get(link,timeout=10)
		except:
			continue	
		if resp.status_code == 404:
			continue 
		soup = BeautifulSoup(resp.content, 'html.parser')
		# text from home page 
		f.write(text_from_html(link))

		discovered = [link]

		newlinks = get_all_links(soup,link,companyname)
		
		traverse(newlinks,discovered,f)

		f.close()
	return None

engine=create_engine('mysql+pymysql://******@startup.ctbs9z8dwcln.us-east-2.rds.amazonaws.com/strategy')
query = "select * from years_for_web WHERE uuid is not NULL AND founded_on = wayback_on "
business = pd.read_sql(query, engine)
business = business[int(sys.argv[1]):int(sys.argv[2])]

for index,row in business.iterrows():
	url = row['homepage_url']
	uuid = row['uuid']
	fyear = int(row['founded_on'])

	for year in range(fyear,2020):
		year_texts(url,year,uuid)

	print('finished ',uuid)






