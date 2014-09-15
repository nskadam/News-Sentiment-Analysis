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

os.chdir('D:\\0. Nilesh Files\\7.1. Personal\\12.3. News\\2. Data Processing\\31. Python News Scrapping')



def get_content_from_url(url):
    return requests.get(url).content
    
        
def write_list_in_json_format_to_file(list, file_name):
    with open(file_name,'w') as f:
        for item in list:
            f.write(item)
            f.write('\n')
        
def fetch_news_webpages_save_data_in_json_format(ops_max_links_to_fetch_per_file = 200000):
    date_number_first = 41894  
    date_number_last = 41090 #36892
    for date_number in range(date_number_first, date_number_last, -1):
        date_tup = xlrd.xldate_as_tuple(date_number,0)      
        date = str(date_tup[0])+'-'+str(date_tup[1])+'-'+str(date_tup[2])
        date_news_publish = str(datetime.strptime(date, '%Y-%m-%d'))
        links_file_name = '\\a_news_links_json_format\\economic_times_links_json_for_date_'+date_news_publish.split(' ')[0]+'.txt'
        webpage_file_name = '\\b_news_webpages_json_format\\economic_times_webpages_json_for_date_'+date_news_publish.split(' ')[0]+'.txt'
        with open(os.getcwd()+links_file_name,'r') as f:
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


    
def main():
    fetch_news_webpages_save_data_in_json_format()
    
if __name__ == '__main__':
    main()    










