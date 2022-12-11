# -*- coding: utf-8 -*-
"""Scraping_Jobstreet.ipynb

"""# Code Scraping"""

from bs4 import BeautifulSoup
from datetime import datetime
from tqdm import tqdm

import requests
import pandas as pd
import re
import lxml
import re
import time
import datetime
from dateutil.relativedelta import relativedelta

import db_mysql
import psutil
import os
import memory_profiler

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
    
  keywords = ["accounting finance","admin human resources","sales marketing","arts media communications","services","hotel restaurant",
              "education training","computer information technology","engineering","sciences"]

  temp = pd.DataFrame(columns=['title','company','location','requirement','posted','image','link'])
  
  total_pages = 11
  
  total_all = []
  total_links_all = []
  for keyword in tqdm(keywords):
    total_job_keyword = []
    total_links_keyword = []
    for page in range(1,total_pages):
        time.sleep(30)
        
        def get_url(keyword):
            keyword = keyword.replace(r' ','-')
            template = 'https://www.jobstreet.co.id/id/job-search/{}-jobs/{}/?createdAt=7d&sort=createdAt'
            url = template.format(keyword,page)
            return url

        headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36',
                    'cookie': 'ABTestID=6848a42f-1ffa-4764-a07d-30106354439c; sol_id=b60e4561-6b5b-4b2e-9f03-e6d05384456b; ABSSRP=1565; ABSSRPGroup=B; ABHPGroup=B; ABJDGroup=B; sol_id=b60e4561-6b5b-4b2e-9f03-e6d05384456b; s_fid=10E8BE6043C2B173-3683860FAEC32727; _ga=GA1.3.922958169.1636388930; s_vi=[CS]v1|30C4A8214BBB658A-400013280AEF775F[CE]; _hjid=0dc65ca9-dad4-4bcc-8fe2-deb78f4676bd; __gads=ID=a517dc225311e691:T=1636388930:S=ALNI_MZnX9iX1TeLDzMrXG7cc-fmFxQaUA; intercom-id-o7zrfpg6=d00cfdfa-3c98-4d57-a228-cc9b655b4d6c; Domain=.jobstreet.com; _hjSessionUser_757922=eyJpZCI6IjViZmYzMGQwLTY1ZGYtNTU3Yi04NmUwLWYwNGU2MGY1MDMzNiIsImNyZWF0ZWQiOjE2MzcyMTEzOTIyNjIsImV4aXN0aW5nIjp0cnVlfQ==; _hjSessionUser_640499=eyJpZCI6IjI0ODI4YmU5LTMwYWMtNWQ3MC1iZGY5LWEwN2U5MTk1NjhhZiIsImNyZWF0ZWQiOjE2MzcyMTU1MjQ5MjAsImV4aXN0aW5nIjp0cnVlfQ==; amp_7e455c_jobstreet.co.id=[CS]v1|30C4A8214BBB658A-400013280AEF775F[CE]...1fkoshnt2.1fkp3ibef.p.0.p; amp_7e455c=[CS]v1|30C4A8214BBB658A-400013280AEF775F[CE]...1fkr9qrrc.1fkr9r8qm.10.0.10; amp_240439=[CS]v1|30C4A8214BBB658A-400013280AEF775F[CE]...1fli2ofqm.1fli2og1u.q.0.q; amp_240439_jobstreet.co.id=[CS]v1|30C4A8214BBB658A-400013280AEF775F[CE]...1fli2ofqm.1fli2vn71.s.0.s; _gcl_au=1.1.1411539871.1650177116; _fbp=fb.2.1650177116112.800644262; intercom-session-o7zrfpg6=; RecentSearch=%7B%22Keyword%22%3A%5B%22dev%22%2C%22developer%22%2C%22uiux%22%2C%22uixux%22%2C%22back%20end%22%5D%7D; __cfruid=8df66b3286b85f1aa68a570a50d1d288d91fcf0e-1650510329; s_cc=true; _gid=GA1.3.58115257.1650510330; __cf_bm=EZCRT_XqkD3ppCHinBPXJhMVxMrpgGMe.OsRvXGq6kM-1650531563-0-AY5KDk0rtt7nFTpqYu2fyvMB/yqmM6c625mQFyjz719NXm9ft9oOMbsANGiIqJWgsO3ziyCNRMm9sACizI6e3rU=; da_sa_candi_sid=1650531564743; _gat_UA-82223804-6=1; _hjIncludedInSessionSample=0; _hjSession_757922=eyJpZCI6ImQzYmYzZjExLWE1OTQtNGQ3NS05OTE2LTIwMzY0ODQ1NDkzOCIsImNyZWF0ZWQiOjE2NTA1MzE1NjUwNzQsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=1; s_sq=%5B%5BB%5D%5D; utag_main=v_id:017d006180f400a4b5c1e7da835805072001c06a00bd0$_sn:28$_se:2$_ss:0$_st:1650533369105$vapi_domain:jobstreet.co.id$dc_visit:28$ses_id:1650531564743%3Bexp-session$_pn:2%3Bexp-session$dc_event:2%3Bexp-session$dc_region:ap-southeast-2%3Bexp-session'}
        url = get_url(keyword)
        res = requests.get(url, headers=headers)

        if res.status_code == requests.codes.ok :
            soup = BeautifulSoup(res.content,'lxml')

        soupHTML = str(soup)
        jobs = soup.find_all('article', class_=re.compile(r'.*c6ROG_0.*'))
        
        def get_link():
            hrefHTML = soup.find('div', class_=re.compile(r'sx2jih0 zcydq876 zcydq866 zcydq896 zcydq886 zcydq8n zcydq856 zcydq8f6 zcydq8eu'))
            hrefHTML = str(hrefHTML)
            if re.findall(r'/id/job/', hrefHTML):
                re_href = r'<a class="_1hr6tkx5 _1hr6tkx8 _1hr6tkxb sx2jih0 sx2jihf zcydq8h" href="(/id/job/.*?)" rel="nofollow noopener noreferrer" target="_top">'
                href = re.findall(re_href, soupHTML)
            else:
                re_href = r'<a class="_1hr6tkx5 _1hr6tkx8 _1hr6tkxb sx2jih0 sx2jihf zcydq8h" href="(/id/job/.*?)" rel="nofollow noopener noreferrer" target="_top">'
                href = re.findall(re_href, soupHTML)
            
            return href
        
        links = []
        for link in get_link():
            link = f'https://www.jobstreet.co.id{link}'
            links.append(link)
            
        def get_data(job):
            link = links[job]

            link_res = requests.get(link)
            link_soup = BeautifulSoup(link_res.content, 'lxml')

            container = link_soup.find('div', class_=re.compile('sx2jih0 zcydq8bm _18qlyvc14 _18qlyvc17 zcydq832 zcydq835'))
            containerHTML = container.find('div', class_=re.compile('sx2jih0 zcydq8r zcydq8p _16wtmva0 _16wtmva4')).find('div',class_=re.compile(r'sx2jih0 zcydq8a2 zcydq89k zcydq86i zcydq874 zcydq8n zcydq84u zcydq8ei _16wtmva1'))
            containerHTML = str(containerHTML)
            
            if re.findall(r'_18qlyvcp _18qlyvc1x', containerHTML):
                re_job_name = r'<h1 class="sx2jih0 _18qlyvc0 _18qlyvch _1d0g9qk4 _18qlyvcp _18qlyvc1x">(.*?)</h1>'
                job_name = re.findall(re_job_name, containerHTML)
            else:
                job_name = 'None'
                
            if re.findall(r'sx2jih0 zcydq84u _18qlyvc0 _18qlyvc1x _18qlyvc2 _1d0g9qk4 _18qlyvcb', containerHTML):
                re_company_name = r'<span class="sx2jih0 zcydq84u _18qlyvc0 _18qlyvc1x _18qlyvc2 _1d0g9qk4 _18qlyvcb">(.*?)</span>'
                company_name = re.findall(re_company_name, containerHTML)
            else:
                company_name = 'None'
            
            locationHTML = container.find('div', class_=re.compile('sx2jih0 zcydq86a')).find_all('div', class_=re.compile('sx2jih0 zcydq856'))
            
            locationHTML = locationHTML[0]
            locationHTML = str(locationHTML)
            
            location1 = container.find('div', class_=re.compile('sx2jih0 zcydq86a')).find('div', class_=re.compile('sx2jih0 zcydq856'))
            locationHTML1 = str(location1)
            
            location2 = container.find('div', class_=re.compile('sx2jih0 zcydq86a')).find('div', class_=re.compile('sx2jih0 zcydq856')).find('div', class_=re.compile('sx2jih0 zcydq86i zcydq87i'))
            locationHTML2 = str(location2)
            
            if location1 is not None:
                re_location = r'<div class="sx2jih0 zcydq856"><span class="sx2jih0 zcydq84u _18qlyvc0 _18qlyvc1x _18qlyvc1 _18qlyvca">(.*?)</span></div>'
                location = re.findall(re_location, locationHTML1)
            else:
                re_location = r'<span class="sx2jih0 zcydq84u _18qlyvc0 _18qlyvc1x _18qlyvc1 _18qlyvca">(.*?)</span>'
                location = re.findall(re_location, locationHTML2)
            
            requirement_tag = link_soup.find('div', class_=re.compile('YCeva_0'), attrs={'data-automation':'jobDescription'})
            if requirement_tag is not None:
                requirement = requirement_tag.text.strip()
            else:
                requirement = 'None'

            if re.findall(r'Ditayangkan \w+ [0-9]', containerHTML):
                re_posted = r'<span class="sx2jih0 zcydq84u _18qlyvc0 _18qlyvc1x _18qlyvc1 _18qlyvca">(Ditayangkan.*?)</span>' 
                posted = re.findall(re_posted, containerHTML)
            elif re.findall(r'Posted \w+ [0-9]', containerHTML):
                re_posted = r'<span class="sx2jih0 zcydq84u _18qlyvc0 _18qlyvc1x _18qlyvc1 _18qlyvca">(Posted.*?)</span>' 
                posted = re.findall(re_posted, containerHTML)
            
            if re.findall(r'_23SwX_0 _18qlyvc14 _18qlyvc17 zcydq832 zcydq835', containerHTML):
                imageHTML = container.find('div', class_=re.compile('.*_23SwX_0 _18qlyvc14 _18qlyvc17 zcydq832 zcydq835'))
                imageHTML = str(imageHTML)
                re_company_img = r'src="(.*?)"'
                company_img = re.findall(re_company_img, imageHTML)
            else:
                company_img = 'None'
            

            data = [job_name,company_name,location,requirement,posted,company_img,link]
            return data
        
        for cjob in jobs:
            total_job_keyword.append(cjob)     
        for l in links:
            total_links_keyword .append(l)
            
        records = []
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
      if re.findall(r'([0-9]+)-(.*)-([0-9]+)', i):
        toSingleString = re.search(r'([0-9]+)-(.*)-([0-9]+)', i)
        toSingleString = toSingleString.group()
 
        toInt = datetime.datetime.strptime(toSingleString, '%d-%b-%y')
        date_posted.append(toInt)
      else:
        date_posted.append(datetime.datetime.today().strftime('%Y-%m-%d'))
        
    data_df['date_posted'] = date_posted
    data_df_for_csv = data_df.copy()

    data_df_for_csv = data_df_for_csv.replace(r'\[.*?\]','', regex=True)
    data_df_for_csv = data_df_for_csv.replace(r'\t','', regex=True)
    data_df_for_csv = data_df_for_csv.replace(r'\r','', regex=True)
    data_df_for_csv = data_df_for_csv.replace(r'\s\s\s','', regex=True)
    data_df_for_csv['requirement'] = [re.sub(r'([a-zA-Z])([A-Z][a-z]+)', r'\1. \2', ele) for ele in data_df_for_csv['requirement']]
  
  date_scrape = datetime.datetime.today().strftime("%Y-%m-%d")
  data_df_for_csv.to_excel(f'data_csv/jobs_data_jobstreet({date_scrape}).xlsx',index=False)

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