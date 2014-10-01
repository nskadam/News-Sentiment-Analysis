# -*- coding: utf-8 -*-
"""
Created on Sun Sep 14 11:13:01 2014

@author: Nilesh
"""

import os
from lxml import html
import xlrd 
import time
import json
from datetime import datetime
import pandas as pd
from pandas import *
from datetime import datetime
import ner
import subprocess


os.chdir('D:\\0. Nilesh Files\\7.1. Personal\\12.3. News\\2. Data Processing\\31. Python News Scrapping')

def parse_news_content(content, link):
    # content = data['webpage_content']
    tree = html.fromstring(content)
    header = tree.xpath('//*[@id="mod-article-header"]/h1/text()')
    datetime_news_publish = tree.xpath('//*[@id="mod-article-byline"]/span[3]//text()')
    # datetime_news_publish = tree.xpath('//*[@id="pageContent"]/article/div[2]/div[1]//text()')
    body = tree.xpath('//*[@id="mod-a-body-first-para"]//text()')
    body = body + tree.xpath('//*[@id="mod-a-body-after-first-para"]//text()')
    return str(json.dumps({'link':link, 'header':header[0], 'datetime_news_publish' : datetime_news_publish[0], 'body' : body}))
        
        
def parse_news_webpages():
    date_number_first = 41894  
    date_number_last = 36892 #$41890 
    parsed_news = list()
    for date_number in range(date_number_first, date_number_last, -1):
        date_tup = xlrd.xldate_as_tuple(date_number,0)      
        date = str(date_tup[0])+'-'+str(date_tup[1])+'-'+str(date_tup[2])
        date_news_publish = str(datetime.strptime(date, '%Y-%m-%d'))
        webpage_file_name = '\\b_news_webpages_json_format\\economic_times_webpages_json_for_date_'+date_news_publish.split(' ')[0]+'.txt'
        try:        
            with open(os.getcwd()+webpage_file_name,'r') as f:
                for row in f:
                    data = json.loads(row)
                    # print data.viewkeys()
                    try:                
                        pn = parse_news_content(data['webpage_content'], data['link'])
                    except:
                        pn = str(json.dumps({'link':data['link'], 'header':'Error while Parsing', 'datetime_news_publish' : 'Error while Parsing', 'body' : 'Error while Parsing'}))
                    parsed_news.append(pn)
            print 'Webpages parsed for for date: '+ date_news_publish
        except:
            pass
    return parsed_news
        
def data_frame_from_parsed_news_json(parsed_news):
    body, header, link, datetime_news_publish = [], [], [], []
    for pn in parsed_news:
        pn = json.loads(pn)
        link.append(pn['link'])
        body.append(pn['body'])
        header.append(pn['header'])
        datetime_news_publish.append(pn['datetime_news_publish'])
    df = pd.DataFrame([link, datetime_news_publish, header, body]).T
    df.columns = ['link', 'datetime_news_publish', 'header', 'body']
    return df

def parser_data_frame_to_required_columns(df):
    df_out= df
    df_out['datetime_news_publish']    
    
def get_ner_tagged_text(df):
    tagger = ner.SocketNER(host='localhost', port=8080)
    txt_tagged =  []
    i = 1
    for txt in df.header.map(str) + df.body.map(str):
        print 'Working on item:', i, '\tof', len(df) 
        txt_tagged.append(tagger.get_entities(txt))
        if i % 100 == 0:
            df_temp = df.ix[range(i)]
            df_temp['ner'] = txt_tagged  
            df_temp = get_sentiment_score(df_temp)
            triplet =  get_entity_sentment_triplet(df_temp)
            write_triplet_to_file(triplet)
        i = i + 1
        #print 'Tagged:', tagger.get_entities(txt)
    df['ner'] = txt_tagged  
    print 'NER Finished' 
    #df_ner= pandas.io.json.read_json(txt_tagged)   
    return df
    
def get_sentiment_score(df):
    df['sentment_score'] = 3
    return df

def get_entity_sentment_triplet(df):
    triplet = []
    for i in range(len(df)):
        # print 'Working on link:', i, '\tof', len(df)
        try:        
            date_news_publish =  str(datetime.strptime(df.datetime_news_publish.iat[i][0:12], '%b %d, %Y'))
        except:
            date_news_publish = 'Error while Parsing'
        link = df.link.iat[i]
        sentment_score = df.sentment_score.iat[i]
        try:
            for org in set(df.ner.iat[i]['ORGANIZATION']):
                triplet.append([date_news_publish, link, org, sentment_score])
        except:
            pass
    return triplet
    
def write_triplet_to_file(triplet):
    file =  open('triplet_news.csv','w')    
    file.write('date_news_publish,link,organisation, sentiment_score\n')
    for item in triplet:
        for i in item:
            file.write(str(i))
            file.write(',')
        file.write('\n')
    file.close()
    
    
def main():
    time_start = datetime.now()
    parsed_news = parse_news_webpages()
    print 'Time taken:', datetime.now()-time_start
    df = data_frame_from_parsed_news_json(parsed_news)
    df.to_csv('parsed_news.csv')
    
    df = pd.read_csv('parsed_news.csv')
    time_start = datetime.now()
    df = get_ner_tagged_text(df)
    print 'Time taken:', datetime.now()-time_start
    df = get_sentiment_score(df)
    triplet =  get_entity_sentment_triplet(df)
    write_triplet_to_file(triplet)
    
    
    
if __name__ == '__main__':
    main()    










