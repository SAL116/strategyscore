# 1. identify companies as public and startup
# 2. wayback machine fetch data snapshot
# 	2.1 identify a date to fetch 
# 	2.2 search for about pages 
# 	2.3 get text from those pages and save as one file
import pymysql
from sqlalchemy import create_engine
from webText import getText
import db
import wordProcessing
import sys
import os 
import pandas as pd

year = sys.argv[1]
engine=create_engine('mysql+pymysql://root:root123456@startup.ctbs9z8dwcln.us-east-2.rds.amazonaws.com/strategy')
companies = db.businessEachYear(year,engine)
path = os.getcwd()+"/%s/"%str(year)
if not os.path.exists(path):
	os.makedirs(path)

incumbent = dict()
startups = dict()

for index in companies.index:
	website = companies.loc[index,'homepage_url'] 
	text = getText(website,year)
	if text is None:
		continue
	tokens = wordProcessing.preProcessing(text)
	#tokens are the break down list of words(lemmenized)
	#allTokens.append(tokens)
	#startup & incumbent separator 
	#save token to local, we will use for cos similarity

	if companies.loc[index,"public_at"] is not None:
		incumbent[companies.loc[index,'uuid']] = tokens
	elif companies.loc[index,"founded_on"] is not None:
		startups[companies.loc[index,'uuid']] = tokens
	else:
		continue

# All companies processed, save to disk
wordProcessing.writeFile(path+'incumbent',incumbent)
wordProcessing.writeFile(path+'startups',startups)
# TFIDF (for this year) save the model to local
iList = list()
for k in list(incumbent.values()):
	iList.append(k)
del incumbent 
wordProcessing.TFIDF(iList,path,'incumbent')

sList = list()
for k in list(startups.values()):
	sList.append(k)
del startups 
wordProcessing.TFIDF(sList,path,'startups')

allTokens = sList + iList

wordProcessing.TFIDF(allTokens,path)

