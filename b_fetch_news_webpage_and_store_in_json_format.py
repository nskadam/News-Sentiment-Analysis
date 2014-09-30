# -*- coding: utf-8 -*-
"""
Created on Sat Sep 20 10:18:45 2014

@author: Nilesh
"""

from threading import Thread
from Queue import Queue
import requests
import os
import xlrd 
import json
from datetime import datetime


os.chdir('D:\\0. Nilesh Files\\7.1. Personal\\12.3. News\\2. Data Processing\\31. Python News Scrapping')

      
concurrent = 100

def que_task():
    while True:
        date_num_link_file_path = q.get()
        fetch_all_links_in_file(date_num_link_file_path)
        q.task_done()

def get_content_from_url(url):
    return requests.get(url).content
        
def write_list_in_json_format_to_file(list, file_name):
    with open(file_name,'w') as f:
        for item in list:
            f.write(item)
            f.write('\n')

def fetch_all_links_in_file(date_num_link_file_path,ops_max_links_to_fetch_per_file=5000000):
    [date_number, links_file_name] = date_num_link_file_path 
    date_tup = xlrd.xldate_as_tuple(date_number,0)      
    date = str(date_tup[0])+'-'+str(date_tup[1])+'-'+str(date_tup[2])
    date_news_publish = str(datetime.strptime(date, '%Y-%m-%d'))
    webpage_file_name = '\\b_news_webpages_json_format\\economic_times_webpages_json_for_date_'+date_news_publish.split(' ')[0]+'.txt'
    with open(links_file_name,'r') as f:
        webpage_json = list()
        count = 0                
        for row in f:
            if count < ops_max_links_to_fetch_per_file:
                print 'fetching for row count', count
                data = json.loads(row)
                # print data["link"]
                try:
                    content = get_content_from_url(data["link"])
                    js = {'main_site' : data['main_site'], 'date_news_publish': data['date_news_publish'], \
                    'link': data['link'], 'webpage_content':content}        
                    webpage_json.append(json.dumps(js))
                except:
                    pass
                count += 1
            if count % 50 == 0:
                write_list_in_json_format_to_file(webpage_json, os.getcwd()+webpage_file_name)
    write_list_in_json_format_to_file(webpage_json, os.getcwd()+webpage_file_name)
    print 'Webpages fetched and stored in file for date: '+ date_news_publish
 
def get_dates_link_file_names_not_yet_fetched():
    date_number_first = 41894  
    date_number_last = 36892
    date_num_link_file_names = []
    links_file_name_fetched = os.listdir(os.getcwd()+'\\b_news_webpages_json_format\\')
    date_nums_fetched =  [string[38:48] for string in links_file_name_fetched]
    for date_number in range(date_number_first, date_number_last, -1):
        date_tup = xlrd.xldate_as_tuple(date_number,0)      
        date = str(date_tup[0])+'-'+str(date_tup[1])+'-'+str(date_tup[2])
        date_news_publish = str(datetime.strptime(date, '%Y-%m-%d'))
        if date_news_publish[0:10]  not in date_nums_fetched:
            links_file_name = os.getcwd()+'\\a_news_links_json_format\\economic_times_links_json_for_date_'+date_news_publish.split(' ')[0]+'.txt'
            date_num_link_file_names .append((date_number, links_file_name))
    return date_num_link_file_names 


q = Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=que_task)
    t.daemon = True
    t.start()
    
date_num_link_file_paths = get_dates_link_file_names_not_yet_fetched()
for date_num_link_file_path in date_num_link_file_paths:
    print 'Adding to que:', date_num_link_file_path
    q.put(date_num_link_file_path)
q.join()
