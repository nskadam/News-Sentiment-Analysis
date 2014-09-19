# -*- coding: utf-8 -*-
"""
Created on Sun Sep 14 11:13:01 2014

@author: Nilesh
"""

import requests
import bs4
import re 
import os
import pickle
from lxml import html
import xlrd 
import time
import json
from datetime import datetime
from Queue import Queue
from threading import Thread

os.chdir('D:\\0. Nilesh Files\\7.1. Personal\\12.3. News\\2. Data Processing\\31. Python News Scrapping')



def get_content_from_url(url):
    return requests.get(url).content
    
        
def write_list_in_json_format_to_file(list, file_name):
    with open(file_name,'w') as f:
        for item in list:
            f.write(item)
            f.write('\n')
        
def get_dates_link_file_names_not_yet_fetched():
    date_number_first = 41894  
    date_number_last = 41092 #36892
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

        
def fetch_news_webpages_save_data_in_json_format(q, ops_max_links_to_fetch_per_file = 2):
    while True:
        date_num_link_file_names = q.get()
        print 'Working on file:', date_num_link_file_names
        for date_number, links_file_name in date_num_link_file_names :
            date_tup = xlrd.xldate_as_tuple(date_number,0)      
            date = str(date_tup[0])+'-'+str(date_tup[1])+'-'+str(date_tup[2])
            date_news_publish = str(datetime.strptime(date, '%Y-%m-%d'))
            webpage_file_name = '\\b_news_webpages_json_format\\economic_times_webpages_json_for_date_'+date_news_publish.split(' ')[0]+'.txt'
            with open(links_file_name,'r') as f:
                webpage_json = list()
                count = 0                
                for row in f:
                    print 'fetching for row count', count
                    if count < ops_max_links_to_fetch_per_file:
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
                    if count % 10 == 0:
                        write_list_in_json_format_to_file(webpage_json, os.getcwd()+webpage_file_name)
            write_list_in_json_format_to_file(webpage_json, os.getcwd()+webpage_file_name)
            print 'Webpages fetched and stored in file for date: '+ date_news_publish
    q.task_done()


    
def main():
    q = Queue(maxsize=0)
    num_threads = 10
    
    for i in range(num_threads):
        worker = Thread(target=fetch_news_webpages_save_data_in_json_format, args=(q,))
        worker.setDaemon(True)
        worker.start()
    
    date_num_link_file_names = get_dates_link_file_names_not_yet_fetched()

    for x in date_num_link_file_names:
        print 'Adding to que', x
        q.put(x)
    
    q.join()
    
if __name__ == '__main__':
    main()    









