# This script will get about-us description text for companies in crunchbase that founded in a certain year 

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

#Return a full list of timestamp from start to now 
def timeline(website,start):
    def tslist(r,i):
        for j in range(4):
            for k in range(7):
                try:
                    if r[i][j][k]:
                        return(r[i][j][k]['ts'][0])
                except:
                    return([None])
    ts = []
    for y in range(start,2019):
        ylist = []
        year = str(y)
        try:
            r0 = requests.get("https://web.archive.org/__wb/calendarcaptures?url="+website+"&selected_year="+year,timeout=10)
        except:
            continue 
        for i in range(11):
            try:
                ylist.append(tslist(r0.json(),i))
            except:
                pass
        ts.append(ylist)
    return(ts)
#get response
def get_page(website,ts):
    link = "https://web.archive.org/web/"+str(ts)+"/"+website+"/"
    page = requests.get(link,timeout=10)
    return(page)



#2 levels of keyword
About_L1=["ABOUT","ABOUT-US"]
About_L2=["COMPANY","PRODUCTS","OVERVIEW","SERVICES","FEATURES","SOLUTIONS","WHAT WE DO","WHO WE ARE"]

def Locate_About_Page(resp):
    soup = BeautifulSoup(resp.content, 'html.parser')
    for i in About_L1:
        for link in soup.find_all('a', href=True):
            if i in link.text.upper():
                if link['href'].startswith('https://web.archive.org'):
                    return link['href']
                elif link['href'].startswith('/web/') and 'http' in link['href']:
                    return 'https://web.archive.org'+ link['href']
                else:
                    return resp.url+link['href']
            
    for i in About_L2:
        for link in soup.find_all('a', href=True):
            if i in link.text.upper():
                if link['href'].startswith('https://web.archive.org'):
                    return link['href']
                elif link['href'].startswith('/web/') and 'http' in link['href']:
                    return 'https://web.archive.org'+ link['href']
                else:
                    return resp.url+link['href']
    # if nothing found
    return "not found"

######GET PAGE CONTENT #############
def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def text_from_html(link):
    resp = requests.get(link,timeout=10)
    soup = BeautifulSoup(resp.content, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    raw = u" ".join(t.strip() for t in visible_texts)
    
    return str(raw.replace('\t',' ').replace('\\n',' ').replace('\n',' ').strip().encode('ascii', 'ignore'))
######GET PAGE CONTENT #############



###Get data and save to disk 
def GetAboutData(business,year,datadir):    #takes a pandas dataframe
    for index, row in business.iterrows():   #business level is each year 
        if row['homepage_url'] == None:
            continue 
        #timeline is a list of lists 
        print(row['homepage_url'])
        for ylist in (timeline(row['homepage_url'],year)): #ylist level is each year
            for i in list(reversed(ylist)):      #each i is a month timestamp
            #start from december     
                if i == None:
                    continue
                
                timestamp = i
                #print(timestamp)
                name = str(row['id'])+'&'+str(timestamp)
                ##if already did
                if os.path.exists(datadir+'startup/'+name+'.txt'):
                    continue
                try:
                    resp = get_page(row['homepage_url'],timestamp)
                except:
                    continue
                #if page not valid 
                if resp.status_code == 404:
                    continue 
                #for valid page 
                try:
                    about_page = Locate_About_Page(resp)
                except Exception as e:
                    #print(name,str(e))
                    continue
                #print('got page',i)
                if about_page == "not found":
                    about_page = get_page(row['homepage_url'],timestamp)
                    #if still not found then use the home page.
                try:
                    content = text_from_html(about_page)
                except:
                    continue
                #save to disk
                print('writing ',name)
                txt = open(datadir+'startup/'+name+'.txt','a+') 
                txt.write(content)
                txt.close()
                break  #to next year 
##Will get one page per year and break, this will save much time
##When we have more time, we can get for each month of the year, but not now 
##For each month of the year, in the compare script, we will get the largest among the months for each year, so still one per year at the end 




##########################################################
if __name__ == "__main__":
    #directory to hold all data 
    datadir = "/home/ubuntu/SIMItxt/"
    ##MySQL
    engine=create_engine('mysql+pymysql://*******@startup.ctbs9z8dwcln.us-east-2.rds.amazonaws.com/cb')
    
    year = int(sys.argv[1])

    query = "select id,homepage_url from selected_business \
    where year(founded_at)="+str(year)
    
    business = pd.read_sql(query, engine)
    #for each year 
    GetAboutData(business,year,datadir)
    print('finished ',str(year))









