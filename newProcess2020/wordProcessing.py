import re
# import numpy as np
# import pandas as pd
from nltk.tokenize import RegexpTokenizer
from nltk.stem.porter import PorterStemmer
from nltk.stem import WordNetLemmatizer
from stop_words import get_stop_words
from gensim import corpora, models, similarities
from gensim.corpora.textcorpus import TextCorpus
from gensim.test.utils import datapath, get_tmpfile
# import glob
# from sqlalchemy import create_engine
import os
import sys
import nltk 
import pickle

def preProcessing(data):
	#tokenize
	tokenizer = RegexpTokenizer(r'\w+')
	raw = data.replace('\n',' ').lower()
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


def TFIDF(allTokens,path,indicator):
	name = path+indicator
	dictionary = corpora.Dictionary(allTokens)
	#dictionary.filter_extremes(no_below=10, no_above=0.25, keep_n=None)
	corpus = [dictionary.doc2bow(token) for token in allTokens]
	# tfidf
	tfidf = models.TfidfModel(corpus)	
	corpus_tfidf = tfidf[corpus]
	mSimilar = similarities.MatrixSimilarity(tfidf[corpus])
	#save results to disk 
	dictionary.save(name+'.dic')
	corpora.MmCorpus.serialize(name+'.mm', corpus)
	tfidf.save(name+'.tfidf')
	corpora.MmCorpus.serialize(name+'_tfidf.mm', corpus_tfidf)
	mSimilar.save(name+'.mSimilar')


def compareSimilarity(uuid,year,tfidf,mSimilar):
	#tfidf = models.TfidfModel.load(name+'.tfidf')
	#mSimilar = similarities.MatrixSimilarity.load(name+'.mSimilar')
	file = "~/wayBack/%s/texts/%s"%(str(year),uuid)
	vec_tfidf = tfidf[file]
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

	return {'uuid':uuid,'year':year,'novelty6':novelty6,'novelty5':novelty5,'novelty4':novelty4,'novelty3':novelty3,'mean':mean,'median':median}

def writeFile(path,data):
	list_file = open(path+'.pickle','wb')
	pickle.dump(data,list_file)
	list_file.close()