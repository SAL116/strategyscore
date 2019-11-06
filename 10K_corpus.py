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
### make dict and corpus 
def MakeTextsList(year):
	filelist = glob.glob("/home/ubuntu/SIMItxt/10k_texts/"+str(year)+"/*.txt")
	texts = []
	for filename in filelist:
		#cik = filename[36:filename.index('__')]
		with open(filename,'r') as myfile:
			data=myfile.read().replace('\n', ' ')
		myfile.close()
		
		text = pre_processing(data) #this is a list of tokens 
		texts.append(text)
	return(texts)
################################



if __name__ == "__main__":
	
	datadir = "/home/ubuntu/"
	#engine=create_engine('mysql+pymysql://root:root123456@startup.ctbs9z8dwcln.us-east-2.rds.amazonaws.com/SIMI')
	
	for year in range(int(sys.argv[1]),int(sys.argv[2])):
		name = datadir+'SIMItxt/training/'+"10k/models/"+str(year)
		
		texts = MakeTextsList(year)

		#make dictionary 
		dictionary = corpora.Dictionary(texts)
		#filter out words appears in more than 60% of documents 
		dictionary.filter_extremes(no_below=10, no_above=0.25, keep_n=None)
		#make corpus 
		corpus = [dictionary.doc2bow(text) for text in texts]
		# tfidf
		tfidf = models.TfidfModel(corpus)	
		corpus_tfidf = tfidf[corpus]
		# calculate matrix 
		mSimilar = similarities.MatrixSimilarity(tfidf[corpus])

		#save results to disk 
		dictionary.save(name+'.dic')
		corpora.MmCorpus.serialize(name+'.mm', corpus)
		tfidf.save(name+'.tfidf')
		corpora.MmCorpus.serialize(name+'_tfidf.mm', corpus_tfidf)
		mSimilar.save(name+'.mSimilar')
		##Save reference index to DB
		# labels = ['position','cik']
		# df = pd.DataFrame.from_records(list(enumerate(shortlist)), columns=labels)
		# df.insert(2,'year',str(year))
		# df.to_sql('10k_corpus_index', con=engine, index=False,if_exists='append')
