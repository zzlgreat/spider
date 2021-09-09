#encoding=utf-8
from bs4 import BeautifulSoup
import requests
import json
import time
import random
import pandas as pd
import pymongo
conn = pymongo.MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'.format("root","zzl33818","192.187.116.66","26781","admin"))
db = conn.jav   # 直接写库名
k = db.list_collection_names(include_system_collections=True)  # 返回当前库下所有的collection名
lib = db['javlib']
print(k)
res = []
for ch in [chr(i) for i in range(97, 123)]:
    try:
        r = requests.get('http://www.javlibrary.com/tw/star_list.php?prefix=' + ch.upper()).content.decode('UTF-8')
        # r = requests.get('http://www.javlibrary.com/tw/star_list.php?prefix=A').content.decode('UTF-8')
        soup = BeautifulSoup(r, 'html.parser')
        x = soup.find_all(class_='starbox')[0]
        s = x.find_all('div')
        try:
            pags = int(soup.find_all(class_='page last')[0]['href'].split('page=')[-1])
        except:
            pags = 0
        for ps in range(1,pags+1):
            r = requests.get('http://www.javlibrary.com/tw/star_list.php?prefix=' + ch.upper()+'&page='+str(ps)).content.decode('UTF-8')
            # r = requests.get('http://www.javlibrary.com/tw/star_list.php?prefix=A').content.decode('UTF-8')
            soup = BeautifulSoup(r, 'html.parser')
            x = soup.find_all(class_='starbox')[0]
            s = x.find_all('div')
            for xx in s:
                time.sleep(random.random())
                href = 'http://www.javlibrary.com/tw/' + xx.find_all('a')[0]['href'] + '&mode=2'
                name = xx.text
                works = []
                try:
                    r2 = requests.get(href).content.decode('UTF-8')
                    soup = BeautifulSoup(r2, 'html.parser')
                    x2 = soup.find_all(class_='videos')[0]
                    s2 = x2.find_all('div')
                    works = []
                    try:
                        pages = int(soup.find_all(class_='page last')[0]['href'].split('page=')[-1])
                    except:
                        pages = 0
                    for ss2 in s2:
                        try:
                            a = ss2.find_all('a')[0]
                            title = a['title']
                            img = a.find_all('img')[0]['src']
                            works.append({'title': title, 'img': img})
                            # print({'title': title, 'img': img})
                        except:
                            pass
                    if pages > 0:
                        for page in range(2, pages + 1):
                            href = 'http://www.javlibrary.com/tw/' + xx.find_all('a')[0][
                                'href'] + '&mode=2' + '&page=' + str(page)
                            try:
                                r2 = requests.get(href).content.decode('UTF-8')
                                soup = BeautifulSoup(r2, 'html.parser')
                                x2 = soup.find_all(class_='videos')[0]
                                s2 = x2.find_all('div')
                                for ss2 in s2:
                                    try:
                                        a = ss2.find_all('a')[0]
                                        title = a['title']
                                        img = a.find_all('img')[0]['src']
                                        works.append({'title': title, 'img': img})
                                    except:
                                        pass
                            except:
                                pass
                except:
                    pass
                x = lib.insert_one({'names': name,
                                    'href': href,
                                    'works': works})
                print(x)


    except Exception as e:
        print(e)


