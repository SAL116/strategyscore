#!/user/user1/al3836/.conda/envs/al3836/bin/python
import os
import re
from bs4 import BeautifulSoup,SoupStrainer
import requests
import pandas as pd
import numpy as np
import sys 
from bs4.element import Comment
from dateutil.parser import parse
import pymysql
from sqlalchemy import create_engine
from math import ceil
from multiprocessing import Process

# change the "save" -- each company is a folder 
# record the url names


##Function for scrape text from html
def tag_visible(element):
	if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
		return False
	if isinstance(element, Comment):
		return False
	return True

def text_from_html(soup): # take in a bs object, not just the link name, need process before hand 
	try:
		texts = soup.findAll(text=True)
		visible_texts = filter(tag_visible, texts)  
		raw = u" ".join(t.strip() for t in visible_texts)
	except:
		return None
	return str(raw.replace('\t',' ').replace('\\n',' ').replace('\n',' ').strip().encode('ascii', 'ignore'))

trash = list()
trash.append("This page is available on the web!  Help make the Wayback Machine more complete!  Save this url in the Wayback Machine")
trash.append("The Wayback Machine requires your browser to support JavaScript, please email info@archive.org if you have any questions about this.	The Wayback Machine is an initiative of the Internet Archive ,		   a 501(c)(3) non-profit, building a digital library of		   Internet sites and other cultural artifacts in digital form. Other projects include Open Library & archive-it.org .  Your use of the Wayback Machine is subject to the Internet Archive\'s Terms of Use")
trash.append("Hrm.  The Wayback Machine has not archived that URL.")
trash.append("This page is not available on the web  because access is forbidden")
trash.append("This page is provided courtesy of GoDaddy.com, LLC. Copyright  1999-2016 GoDaddy.com, LLC. All rights reserved. Privacy Policy")
trash.append("Search the history of over 362 billion web pages on the Internet.		 search  Search the Wayback Machine			Featured  texts All Texts  latest This Just In  Smithsonian Libraries  FEDLINK (US)  Genealogy  Lincoln Collection  Additional Collections		  Books to Borrow	   Top  American Libraries  Canadian Libraries  Universal Library  Community Texts  Project Gutenberg  Biodiversity Heritage Library  Children's Library		  Open Library	   Books by Language  permanentlegacy  uslprototype		Featured  movies All Video  latest This Just In  Prelinger Archives  Democracy Now!  Occupy Wall Street  TV NSA Clip Library		  TV News	  Top  Animation & Cartoons  Arts & Music  Community Video  Computers & Technology  Cultural & Academic Films  Ephemeral Films  Movies		  Understanding 9/11	  News & Public Affairs  Spirituality & Religion  Sports Videos  Television  Videogame Videos  Vlogs  Youth Media		Featured  audio All Audio  latest This Just In  Grateful Dead  Netlabels  Old Time Radio  78 RPMs and Cylinder Recordings		  Live Music Archive	  Top  Audio Books & Poetry  Community Audio  Computers & Technology  Music, Arts & Culture  News & Public Affairs  Non-English Audio  Radio Programs		  Librivox Free Audiobook	  Spirituality & Religion  Chris & Spigs' Rock & Roll Circus  Podcasts		Featured  software All Software  latest This Just In  Old School Emulation  MS-DOS Games  Historical Software  Classic PC Games  Software Library		  Internet Arcade	  Top  Community Software  MS-DOS  Kodi Archive and Support File  CD-ROM Software  APK  CD-ROM Software Library  Vintage Software		  Console Living Room	  Software Sites  Tucows Software Library  Shareware CD-ROMs  ZX Spectrum  DOOM Level CD  ZX Spectrum Library: Games  CD-ROM Images		Featured  image All Image  latest This Just In  Flickr Commons  Occupy Wall Street Flickr  Cover Art  USGS Maps		  Metropolitan Museum	  Top  NASA Images  Solar System Collection  Ames Research Center		  Brooklyn Museum			  web	texts	movies	audio	software	image   logo	  Toggle navigation		ABOUT  CONTACT  BLOG  PROJECTS  HELP  DONATE  JOBS  VOLUNTEER  PEOPLE		search		 Search metadata	 Search text contents	 Search TV news captions	 Search archived web sites	Advanced Search		 upload UPLOAD	person SIGN IN	 ABOUT  CONTACT  BLOG  PROJECTS  HELP  DONATE  JOBS  VOLUNTEER  PEOPLE		   DONATE	  Internet Archive's Wayback Machine		  Latest  Show All")
trash.append("Click here to search for all archived pages under")
trash.append("This page is provided courtesy of GoDaddy.com, LLC. Copyright  1999-2017 GoDaddy.com, LLC. All rights reserved. Privacy Policy")
trash.append("Sorry, there are no results for your search.Search again")
trash.append("The Wayback Machine requires your browser to support JavaScript, please email info@archive.org if you have any questions about this.	 The Wayback Machine is an initiative of the Internet Archive ,		   a 501(c)(3) non-profit, building a digital library of		   Internet sites and other cultural artifacts in digital form. Other projects include Open Library & archive-it.org .  Your use of the Wayback Machine is subject to the Internet Archive's Terms of Use")
trash.append("This page is not available on the web  because page does not exist")
trash.append("Featured  texts All Texts  latest This Just In  Smithsonian Libraries  FEDLINK (US)  Genealogy  Lincoln Collection  Additional Collections")
trash.append("Featured  movies All Video  latest This Just In  Prelinger Archives  Democracy Now!  Occupy Wall Street  TV NSA Clip Library")
trash.append("The Wayback Machine is an initiative of the Internet Archive , a 501(c)(3) non-profit, building a digital library of Internet sites and other cultural artifacts in digital form. Other projects include Open Library & archive-it.org .  Your use of the Wayback Machine is subject to the Internet Archive's Terms of Use")
trash.append("ABOUT  CONTACT  BLOG  PROJECTS  HELP  DONATE  JOBS  VOLUNTEER  PEOPLE		search		 Search metadata	 Search text contents	 Search TV news captions	 Search archived web sites	Advanced Search		 upload UPLOAD	person SIGN IN	 ABOUT  CONTACT  BLOG  PROJECTS  HELP  DONATE  JOBS  VOLUNTEER  PEOPLE		   DONATE	  Internet Archive's Wayback Machine")
trash.append("Featured  software All Software  latest This Just In  Old School Emulation  MS-DOS Games  Historical Software  Classic PC Games  Software Library		  Internet Arcade	  Top  Community Software  MS-DOS  Kodi Archive and Support File  CD-ROM Software  APK  CD-ROM Software Library  Vintage Software		  Console Living Room	  Software Sites  Tucows Software Library  Shareware CD-ROMs  ZX Spectrum  DOOM Level CD  ZX Spectrum Library: Games  CD-ROM Images ")
trash.append("Featured  audio All Audio  latest This Just In  Grateful Dead  Netlabels  Old Time Radio  78 RPMs and Cylinder Recordings")
trash.append("Featured  image All Image  latest This Just In  Flickr Commons  Occupy Wall Street Flickr  Cover Art  USGS Maps		  Metropolitan Museum	  Top  NASA Images  Solar System Collection  Ames Research Center		  Brooklyn Museum			  web	texts	movies	audio	software	image   logo	  Toggle navigation")
trash.append("Live Music Archive	  Top  Audio Books & Poetry  Community Audio  Computers & Technology  Music, Arts & Culture  News & Public Affairs  Non-English Audio  Radio Programs		  Librivox Free Audiobook	  Spirituality & Religion  Podcasts")
trash.append("Search the history of over 362 billion web pages on the Internet.		 search  Search the Wayback Machine")
trash.append("Top  American Libraries  Canadian Libraries  Universal Library  Community Texts  Project Gutenberg  Biodiversity Heritage Library  Children's Library")
trash.append("Sorry.  This URL has been excluded from the Wayback Machine")
trash.append("Chris & Spigs' Rock & Roll Circus")
###get way back timestamp 
def timestamp_monthly(website,year):
	timestamp = list()
	try:
		r0 = requests.get("https://web.archive.org/__wb/calendarcaptures?url="+website+"&selected_year="+str(year),timeout=5)
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

#these functions are used to traverse all links in the webpage and get'em all
def webname(a):
	if pd.isna(a):
		return None
	if a.find('.') == -1:
		return None
	if a == 'http://bit.ly' or a =='https://bit.ly':
		return None
	
	try:
		l = a.split('.')
		if l[0] in ['uk','home','m','diversity','corporate','global','homepage','plc','media','us','hr']:
			return l[1]
		else:
			return l[0]
	except:
		return None

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

# this function decide if we want to continue with the link
# to be valid, it must not visted and with in 10 day range of the homepage snapshot timstamp

def if_discovered(link,discovered,ts): 
	if 'https://web.archive.org/web' in link:
		ts2 = link[link.find('/web/')+5:link.find('/web/')+19]
		diff = abs((parse(str(ts)) - parse(ts2)).days)
		if diff>30:
			return None,None
	if link in discovered:
		return True,None
	try:
		resp2 = requests.get(link,timeout=3)
	except:
		return True,None
	
	if resp2.url in discovered:
		return True,None
	else:
		return False,resp2

## this function will check each link for the page, and all links in all sub-pages
def traverse(newlinks,discovered,companyname,ts,cid,link,year,urlindex):
	for path in newlinks:
		#print('trav.  1 ')
		try:
			disc = if_discovered(path,discovered,ts)
		except:
			return urlindex
		if disc[0] == False:
			try:
				soup = BeautifulSoup(disc[1].content, 'html.parser')
			except:
				continue
			content = text_from_html(soup)
			
			if content is None:
				continue
			for tr in trash:
				content = content.replace(tr,'')
			if len(content)<1000:
				pass
			else:
				urlindex = record(disc[1].url,content,cid,year,urlindex)
				discovered.append(disc[1].url)
			# recursion
			nl2 = get_all_links(soup,link,companyname)
			urlindex = traverse2(nl2,discovered,companyname,ts,cid,year,urlindex)			
	return urlindex
# since there are so many pages and they are all inner connected to each other,
# the scraping would take forever, so, I decided to only check for 1 round after the homepage, 
# That's being said, we are not checking for further than 2 levels of subpages.
# This will not bring much loss (if any) but save subtantial times. 
# traverse2 is exactly the same as traverse, just not doinng the revursion to it's subpages.
def traverse2(newlinks,discovered,companyname,ts,cid,year,urlindex):
	for path in newlinks:
		#print('trav.  2 ')
		try:
			disc = if_discovered(path,discovered,ts)
		except:
			return urlindex
		if disc[0] == False:
			try:
				soup = BeautifulSoup(disc[1].content, 'html.parser')
			except:
				continue
			content = text_from_html(soup)
			
			if content is None:
				continue
			for tr in trash:
				content = content.replace(tr,'')
			if len(content)<1000:
				pass
			else:
				urlindex = record(disc[1].url,content,cid,year,urlindex)
				discovered.append(disc[1].url)		 
	return urlindex

def record(url,content,cid,year,urlindex):
	idx = str(len(urlindex))
	filename = '/NOBACKUP/scratch/al3836/startup_wb/'+str(cid)+'/'+str(idx)+'.txt'
	save(filename,content)
	return urlindex.append({'uuid':cid,'location':idx,'url':url,'year':year},ignore_index=True)

def save(filename,content):
	f = open(filename, "w+")
	f.write(content)
	f.close()


def url_to_text(link,cid,ts,companyname,year,urlindex):
	discovered = list()
	## Get the BS object, if we can't access the page, we won't have the obj either
	try:
		resp = requests.get(link,timeout=3)
	except:
		return urlindex
	if resp.status_code == 404:
		return urlindex 
	try:
		soup = BeautifulSoup(resp.content, 'html.parser')
	except:
		return urlindex
	# save homepage
	content = text_from_html(soup)
	if content is None:
		return urlindex
	for tr in trash:
		content = content.replace(tr,'')
	if len(content)<1000:
		pass
	else:
		urlindex =record(resp.url,content,cid,year,urlindex)
		discovered = [resp.url]
	# now check all subpages and so on
	newlinks = get_all_links(soup,link,companyname) # just to use in traverse
	urlindex = traverse(newlinks,discovered,companyname,ts,cid,link,year,urlindex)
	return urlindex

def year_texts(url,year,uuid,urlindex):
	timestamp = timestamp_monthly(url,year)
	#print(timestamp)
	if timestamp is None:
		return urlindex # go back how you looks when you come 
	if len(timestamp) == 0:
		return urlindex
	companyname = webname(url)
	if companyname is None:
		return urlindex
	for ts in timestamp:
		link = "https://web.archive.org/web/"+str(ts)+"/"+url+"/"
		#print('herrrrr')
		urlindex = url_to_text(link,uuid,ts,companyname,year,urlindex)
	return urlindex



def wayback_mining(df,start,end):
	do = df[start:end]
	for index,row in do.iterrows(): # each company level
		# -- basic info 
		url = row['homepage_url']
		uuid = str(row['uuid'])
		#print(uuid)
		if not os.path.isdir('/NOBACKUP/scratch/al3836/startup_wb/'+uuid):
			os.mkdir('/NOBACKUP/scratch/al3836/startup_wb/'+uuid)
		fyear = int(row['founded_on'])
		# -- 
		urlindex = pd.DataFrame(columns = ['uuid','year','location','url'])

		for year in range(fyear,2020):
			#print(year)
			urlindex = pd.DataFrame(columns = ['uuid','year','location','url'])
			urlindex = year_texts(url,year,uuid,urlindex)
		urlindex.to_csv('/NOBACKUP/scratch/al3836/startup_wb/'+uuid+'/url_reference.csv',index=False)


###---- start. ------###
engine=create_engine('mysql+pymysql://*******@startup.ctbs9z8dwcln.us-east-2.rds.amazonaws.com/strategy')
query = "select * from years_for_web WHERE uuid is not NULL AND founded_on = wayback_on AND founded_on >2010"
business = pd.read_sql(query, engine)


#wayback_mining(business,0,1)
processors = 10

total = len(business)
block = ceil(total/processors)
# we should parallel in 10 processors
for m in range(processors):
	p= Process(target=wayback_mining, args=(business,m*block,(m+1)*block))
	p.start()
	#print(p)





