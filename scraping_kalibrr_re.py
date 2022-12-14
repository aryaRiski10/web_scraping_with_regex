# -*- coding: utf-8 -*-
"""Scraping_Kalibrr.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/13UKmHzieclz9Oy3X85ySRVjeUzFeHkr2

# Code Scraping
"""

from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm

import requests
import pandas as pd
import re
import lxml
import time
import datetime
from dateutil.relativedelta import relativedelta
import db_mysql
import psutil
import memory_profiler
import os

# inner psutil function
def process_memory():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss
 
# decorator function
def profile(func):
    def wrapper(*args, **kwargs):
 
        mem_before = process_memory()
        result = func(*args, **kwargs)
        mem_after = process_memory()
        print("{}: memory before: {:,}, after: {:,}, consumed: {:,}".format(
            func.__name__,
            mem_before, mem_after, mem_after - mem_before))
 
        return result
    return wrapper
  
@profile
def scrapeData(tableName):
    start = time.time()
    
    global temp
        
    keywords = ["accounting and finance","administration and coordination","architecture and engineering",
                    "customer service","education and training","health and medical","it and software",
                    "management and consultancy","sales and marketing","sciences"]
    
    temp = pd.DataFrame(columns=['title','company','location','requirement','posted','image','link'])
    
    total_pages = 2
    
    total_all = []
    total_links_all = []
    for keyword in tqdm(keywords):
        total_job_keyword = []
        total_links_keyword = []
        for page in range(1,total_pages):
            time.sleep(30)
            
            def get_url(keyword):
                keyword = keyword.replace(r' ','-')
                template = 'https://www.kalibrr.com/job-board/i/{}/co/Indonesia/{}?sort=Freshness'
                url = template.format(keyword,page)
                return url
            
            
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.82 Safari/537.36'}
            url = get_url(keyword)
            res = requests.get(url,headers=headers)

            if res.status_code == requests.codes.ok :
                soup = BeautifulSoup(res.content,'lxml')
                
            soupHTML = str(soup)
            jobs = soup.find_all('div', itemprop=re.compile('itemListElement'))
            
            def get_link():
                re_href = r'<a class="k-text-primary-color" href="(.*?)" itemprop="name">'
                href = re.findall(re_href, soupHTML)
                    
                return href
            
            links = []
            for link in get_link():
                link = f'https://www.kalibrr.com/id-ID{link}'
                links.append(link)
                
            def get_data(job):
                link = links[job]
                link_res = requests.get(link)
                link_soup = BeautifulSoup(link_res.content, 'lxml')
                
                container = link_soup.find('div',class_=re.compile('k-container md:k-px-4 md:k-mb-12')).find('div',class_=re.compile('k-bg-white k-relative k-p-4 md:k-rounded-b-lg')).find('div',class_=re.compile('md:k-flex md:k-justify-between'))

                containerHTML = str(container)
                
                if re.findall(r'k-text-title', containerHTML):
                    re_job_name = r'<h1 class="k-text-title k-inline-flex k-items-center md:k-text-primary-head md:k-flex lg:k-mt-16" itemprop="title">(.*?)<!-- -->'
                    job_name = re.findall(re_job_name, containerHTML)
                else:
                    job_name = 'None'
                    
                if re.findall(r'/id-ID/.*', containerHTML):
                    re_company_name = r'<a href="/id-ID/.*"><h2 class="k-inline-block">(.*?)</h2></a>'
                    company_name = re.findall(re_company_name, containerHTML)
                else:
                    company_name = 'None'
                
                if re.findall(r'PostalAddress',  containerHTML):
                    re_location = r'<span itemscope="" itemtype="http://schema.org/PostalAddress">(.*?)</span>'
                    location = re.findall(re_location, containerHTML)
                else:
                    location = 'None'
                
                requirement= link_soup.find('div', class_='md:k-w-full md:k-pr-4').find('div', itemprop="qualifications")
                if requirement is not None:
                    requirement = requirement.text.strip()
                else:
                    requirement = 'None'
                
                if re.findall(r'(Posted|dipasang) (\w+|[0-9])+ \w+', containerHTML):
                    re_posted = r'<div class="k-text-subdued k-text-caption md:k-text-right"><p>(.*?)</p>'
                    posted = re.findall(re_posted, containerHTML)
                else:
                    posted = 'None'
                
                imageHTML = container.find('a', class_=re.compile('k-bg-white.*'))
                imageHTML = str(imageHTML)
                if re.findall(r'src="https://.*?"', imageHTML):
                    re_company_img = r'src="(https://.*?)"'
                    company_img = re.findall(re_company_img, imageHTML)
                else:
                    company_img= 'None'
                            
                data = [job_name,company_name,location,requirement,posted,company_img,link]

                return data
            
            for cjob in jobs:
                total_job_keyword.append(cjob)     
            for l in links:
                total_links_keyword .append(l)
                
            records=[]
            if len(jobs) != 0:
                for job in range(len(jobs)):
                    try:
                        record = get_data(job)
                        records.append(record)
                    except:
                        pass
            else:
                break 
                
            df = pd.DataFrame(records, columns=['title','company','location','requirement','posted','image','link'])

            temp = pd.concat([temp,df])

            temp.reset_index(inplace=True, drop=True)
            data_df = temp[['title','company','location','requirement','posted','image','link']].copy()

            data_df = data_df.dropna(how='any')
            data_df = data_df.reset_index(drop=True)
            data_df['title'] = data_df['title'].str.get(0)
            data_df['company'] = data_df['company'].str.get(0)
            data_df['location'] = data_df['location'].str.get(0)
            data_df['posted'] = data_df['posted'].str.get(0)
            data_df['image'] = data_df['image'].str.get(0)
        
        for j in total_job_keyword:
            total_all.append(j)
        for l in total_links_keyword:
            total_links_all.append(l)
        
        date_posted = []
        for i in data_df['posted']:
            if re.findall(r'[0-9]+ days', i):
                toSingleString = re.search(r'([0-9]+)', i)
                toSingleString = toSingleString.group()          
                toInt = (datetime.datetime.today() - datetime.timedelta(int(toSingleString))).strftime('%Y-%m-%d')
                date_posted.append(toInt)       
            elif re.findall(r'[0-9]+ months', i):
                toSingleString = re.search(r'([0-9]+)', i)
                toSingleString = toSingleString.group()
                toInt = (datetime.datetime.today() - relativedelta(months=int(toSingleString))).strftime('%Y-%m-%d')
                date_posted.append(toInt)         
            elif re.findall(r'a month', i):
                x = re.search(r'(a)', i)
                x = x.group()
                toSingleString = re.sub(r'a', r'1', x)
                toInt = (datetime.datetime.today() - relativedelta(months=int(toSingleString))).strftime('%Y-%m-%d')
                date_posted.append(toInt)
            elif re.findall(r'a day', i):
                x = re.search(r'(a)', i)
                x = x.group()
                toSingleString = re.sub(r'a', r'1', x)
                toInt = (datetime.datetime.today() - datetime.timedelta(int(toSingleString))).strftime('%Y-%m-%d')
                date_posted.append(toInt)
            else:
                date_posted.append(datetime.datetime.today().strftime('%Y-%m-%d'))
            
        data_df['date_posted'] = date_posted
        
        data_df_for_csv = data_df.copy()
        data_df_for_csv = data_df_for_csv.replace(r'\n','', regex=True)
        data_df_for_csv = data_df_for_csv.replace(r'\t','', regex=True)
        data_df_for_csv = data_df_for_csv.replace(r'\r','', regex=True)
        data_df_for_csv = data_df_for_csv.replace(r'\s\s\s','', regex=True)
        data_df_for_csv['location'] = [re.sub(r'(\w)([A-Z])', r'\1, \2', ele) for ele in data_df_for_csv['location']]
        data_df_for_csv['requirement'] = [re.sub(r'([a-zA-Z])([A-Z][a-z]+)', r'\1. \2', ele) for ele in data_df_for_csv['requirement']]
            
    date_scrape = datetime.datetime.today().strftime("%Y-%m-%d")
    data_df_for_csv.to_excel(f'data_csv/jobs_data_kalibrr({date_scrape}).xlsx',index=False)
    
    data_df.fillna('None')
    db_mysql.insertData(data_df, tableName)
    db_mysql.removeDuplicate(tableName)
 
    end = time.time()

    
    print('Scrape is finished..')
    print('Waktu scraping data kalibrr: ', end-start)
    print('total : ',len(data_df.index))
    print('total seluruh job :', len(total_all))
    print('total seluruh link:', len(total_links_all))
    print('total yang didapat :', len(data_df.index))
    return data_df

scrapeData('public.scrape_items')