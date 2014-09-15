# -*- coding: utf-8 -*-
"""
Created on Sun Sep 14 11:13:01 2014

@author: Nilesh
"""

import requests
import bs4
import re 
import os
import os.path
import pickle
from lxml import html
import xlrd 
import time
import json
from datetime import datetime

os.chdir('D:\\0. Nilesh Files\\7.1. Personal\\12.3. News\\2. Data Processing\\31. Python News Scrapping')



def get_content_from_url(url):
    return requests.get(url).content
    
def parse_html_from_content(content):
    soup = bs4.BeautifulSoup(content)
    # return soup.find('a')
    soup = soup.find('span', {'class':'pagetext'})
    return soup.contents[1]
    
def get_links_from_soup(soup):
    links = [a.attrs.get('href') for a in soup.findAll('a')]
    return links

def create_json_from_links_date(links, BASE_URL, date_news_publish):
    link_jsons= []
    for link in links:
        # link = links[91]
        link = BASE_URL + link
        js = {'main_site' : BASE_URL, 'date_news_publish': date_news_publish, 'link': link}        
        link_jsons.append(json.dumps(js))
    return link_jsons
        
def write_json_to_file(link_jsons, file_name):
    with open(file_name,'w') as f:
        for link_json in link_jsons:
            f.write(link_json)
            f.write('\n')
        
def fetch_links_for_date_and_save_json_in_file():
    BASE_URL = 'http://economictimes.indiatimes.com'
    date_number_first = 41894  
    date_number_last = 36892  # 2001-01-01
    for date_number in range(date_number_first, date_number_last, -1):
        date_tup = xlrd.xldate_as_tuple(date_number,0)      
        date = str(date_tup[0])+'-'+str(date_tup[1])+'-'+str(date_tup[2])
        date_news_publish = str(datetime.strptime(date, '%Y-%m-%d'))
        url = '/archivelist/year-2014,month-9,starttime-'+str(date_number)+'.cms'
        links_json_file = 'a_news_links_json_format/economic_times_links_json_for_date_'+date_news_publish.split(' ')[0]+'.txt'        
        if os.path.isfile(links_json_file) == False:
            try:        
                content = get_content_from_url(BASE_URL+url)
                soup = parse_html_from_content(content)
                links = get_links_from_soup(soup)
                link_jsons = create_json_from_links_date(links, BASE_URL, date_news_publish)
                write_json_to_file(link_jsons, 'a_news_links_json_format/economic_times_links_json_for_date_'+date_news_publish.split(' ')[0]+'.txt')
            except:
                pass
        print 'Links fetched and stored in file for date: '+date_news_publish


    
def main():
    fetch_links_for_date_and_save_json_in_file()
    
if __name__ == '__main__':
    main()    


























'''


base_url = 'http://economictimes.indiatimes.com'
print 'Fetching url'
r = requests.get(base_url+'/archive.cms')

soup = bs4.BeautifulSoup(r.text)

links = soup.findAll('a', attrs={'href': re.compile("^/archive/year")})
links = [a.attrs.get('href') for a in soup.findAll('a', attrs={'href': re.compile("^/archive/year")})]
links_monthly =[base_url+link for link in links]

# link = 'http://economictimes.indiatimes.com/archive/year-2014,month-9.cms'
for link in links_monthly:
    r = requests.get(link)    
    soup = bs4.BeautifulSoup(r.text)
    links = [a.attrs.get('href') for a in soup.findAll('a')]
    
file = open("news_links.txt", "w")  # @ReservedAssignment
file.write(str(soup.findAll('a')))
file.close()



[a.attrs.get('href') for a in soup.findAll('a', attrs={'href': re.compile("^/archivelist")})]





link = 'http://economictimes.indiatimes.com/archive/year-2014,month-9.cms'
r = requests.get(link)    
tree = html.fromstring(r.text)
#This will create a list of buyers:
buyers = tree.xpath('//*/tbody/tr[2]/td[2]/a/text()')
tree.xpath('//*/tbody/tr[2]/td[2]/a/text()').tag





r = requests.get('http://economictimes.indiatimes.com/archivelist/year-2014,month-9,starttime-41895.cms')

'/html/body/div[3]/div/div[5]/div[1]'

soup = bs4.BeautifulSoup(r.text)

links = [a.attrs.get('href') for a in soup.findAll('a')]

links_monthly =[base_url+link for link in links]



with open('news_links.txt', 'w') as f:
    for item in links:
        f.write("%s\n" % item)


from lxml import etree

url =  "http://www.example.com/servlet/av/ResultTemplate=AVResult.html"
response = urllib2.urlopen(url)
htmlparser = etree.HTMLParser()
tree = etree.parse(r, htmlparser)
tree.xpath(xpathselector)

tree = html.fromstring(r.text)
find_text = etree.XPath("/html/body/div[3]/div/div[5]/div[1]/")
text = find_text(tree)[0]
print text


'''


