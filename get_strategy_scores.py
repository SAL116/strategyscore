import re
import numpy as np
import pandas as pd
from nltk.tokenize import RegexpTokenizer
from nltk.stem.porter import PorterStemmer
from nltk.stem import WordNetLemmatizer
from stop_words import get_stop_words
from gensim import corpora, models, similarities
from gensim.corpora.textcorpus import TextCorpus
from gensim.test.utils import datapath, get_tmpfile
import glob
from sqlalchemy import create_engine
import os
import sys
import nltk 
######### FUNCTIONS ########################################
def pre_processing(data):
	#tokenize
	tokenizer = RegexpTokenizer(r'\w+')
	raw = data.lower()
	tokens_pre = tokenizer.tokenize(raw)
	#remove all numbers
	tokens=[i for i in tokens_pre if not re.match("\d+",i) and not(len(i)==1)]  
	#filter out stop words
	en_stop = get_stop_words('en')
	stopped_tokens = [i for i in tokens if not i in en_stop]
	#filter for nouns 
	tagged = nltk.pos_tag(stopped_tokens)
	nouns = [item[0] for item in tagged if item[1][0] == 'N']
	#stemming 
	lemmatizer = WordNetLemmatizer() 
	#p_stemmer = PorterStemmer() #porter semming algorithm 
	#texts = [p_stemmer.stem(i) for i in stopped_tokens]
	texts = [lemmatizer.lemmatize(i) for i in nouns]
	return(texts)
################################################################
if __name__ == "__main__":

	year=int(sys.argv[1])
	# connect to database 
	engine=create_engine('mysql+pymysql://*******@startup.ctbs9z8dwcln.us-east-2.rds.amazonaws.com/strategy')

	#load models for text mining 
	datadir = "/home/ubuntu/SIMItxt/"
	name = datadir+'/training/10k/models/'+str(year)
	mSimilar = similarities.MatrixSimilarity.load(name+'.mSimilar')
	dictionary = corpora.Dictionary.load(name+".dic")
	tfidf = models.TfidfModel.load(name+'.tfidf')
	#load corpus indexs (matching cik for incumbent companies)

	query = "SELECT cik FROM SIMI.10k_corpus_index WHERE year={0} ORDER BY position;".format(str(year))
	incumbent_index = pd.read_sql(query,con=engine)

	# Locate the files and form a file list 
	filelist_all = glob.glob("/home/ubuntu/SIMItxt/new_cb/new_cb_startup_texts/*.txt")
	name = '/home/ubuntu/SIMItxt/new_cb/new_cb_startup_texts/(.+)(&)'+str(year)+'(\d+).txt'	
	r = re.compile(name)
	filelist = list(filter(r.match,filelist_all))
	del filelist_all

	#list of companies founded a year before
	query = "SELECT uuid FROM strategy.companies WHERE year(founded_on) = {0};".format(str(year-1)) # founded on the previous year 
	founding_companies = pd.read_sql(query,con=engine)
	founding_companies = list(founding_companies.uuid)
	

	#filter for trash data 
	crunchbase_404 = "b\"Search the history of over 351 billion web pages on the Internet.         search  Search the Wayback Machine"
	godaddy = "Is this your domain? Add hosting, email and more.                    Want to buy this domain? Our Domain Buy Service can help you get it."
	godaddy2="Visit GoDaddy.com for:  New product releases"
	#for each company, get ~6000 scores and save em all 
	for f in filelist:
		uuid = f[49:f.index('&')] #will match uuid and uuid in the future 
		# check if it is founding company 
		if uuid not in founding_companies:
			continue
		# we only want to calculate for 2nd year companies 
		#read the file 
		try:
			with open(f,'r') as myfile:
				data=myfile.read().replace('\n', ' ')
			# we don't want to estimate trash 
			if data.startswith(crunchbase_404) or (godaddy in data) or (godaddy2 in data):
				continue
		except:
			continue
		#get scores 
		vec_bow = dictionary.doc2bow(pre_processing(data))
		vec_tfidf = tfidf[vec_bow]
		sim = mSimilar[vec_tfidf]
		mean = np.mean(sim)
		median = np.median(sim)
		# sort top 5 
		competitors6 = sorted(enumerate(sim),key=lambda x: x[1],reverse=True)[0:6]
		competitors5 = sorted(enumerate(sim),key=lambda x: x[1],reverse=True)[0:5]
		competitors4 = sorted(enumerate(sim),key=lambda x: x[1],reverse=True)[0:4]
		competitors3 = sorted(enumerate(sim),key=lambda x: x[1],reverse=True)[0:3]
		# 1 - mean of top 5 
		novelty6 = 1-sum([pair[1] for pair in competitors6])/6
		novelty5 = 1-sum([pair[1] for pair in competitors5])/5
		novelty4 = 1-sum([pair[1] for pair in competitors4])/4
		novelty3 = 1-sum([pair[1] for pair in competitors3])/3
		# append to df 
		df2 = pd.DataFrame(columns=['uuid','year','novelty6','novelty5','novelty4','novelty3','mean','median'])
		df2 = df2.append({'uuid':uuid,'year':year,'novelty6':novelty6,'novelty5':novelty5,'novelty4':novelty4,'novelty3':novelty3,'mean':mean,'median':median},ignore_index = True)
		df2.to_sql('founding_novelty_2',con=engine,index=False,if_exists='append')

		#formatting the scores 
		size = len(sim)
		df = pd.DataFrame(columns=['uuid','year','incumbent_id','similarity'])
		for i in range(size):
			if sim[i] == 0:
				continue 
			df = df.append({'uuid':uuid,'year':year,'incumbent_id':incumbent_index.cik[i],'similarity':sim[i]},ignore_index = True)

		df.to_sql('founding_score_pairs',con=engine,index=False,if_exists='append')
		
		directory = '/home/ubuntu/SIMItxt/csv/new_cb/{0}'.format(str(year))
		if not os.path.exists(directory):
			os.makedirs(directory)

		df.to_csv('/home/ubuntu/SIMItxt/csv/new_cb/{0}/{1}.csv'.format(str(year),str(uuid)),index=False)
