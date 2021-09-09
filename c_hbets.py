#coding=utf-8
import pandas as pd
import requests
import json
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pymysql

db = pymysql.connect(host="10.110.80.83",
                         port=3306,
                         database="tianyancha",
                         user="root",
                         password="6lfBxZLyopc8Q@UF",
                         charset="utf8")
conn = db.cursor()
engine = create_engine("mysql+pymysql://root:6lfBxZLyopc8Q@UF@10.110.80.83:3306/tianyancha", echo=False)

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Cookie': 'PHPSESSID=tsgsreciedvfek5bpuak3goof5; view=1629257767',
    'Host': 'www.hbets.cn',
    'Referer': "http://www.hbets.cn/list/13.html?page=3",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Upgrade-Insecure-Requests':'1'
}
for i in range(1,53):
    url = 'http://www.hbets.cn/list/13.html?page='+str(i)
    print(url)
    r = requests.get(url, headers=headers).text.encode('GBK','ignore').decode('GBK')
    #print(r)
    soup = BeautifulSoup(r, 'lxml')
    table = soup.find_all('div',class_='future_table')[0]
    #print(table)
    for n,u in enumerate(table.find_all('ul')):
        if n>1:
            c = []
            for l in u.find_all('li'):
                c.append(l.text)
            print(c)
            sql = 'insert into carbon_hbets (product,`date`,`newprice`,`change`,highestprice,lowestprice,vol,amount,lastdayprice) values ' + str(tuple(
                c))
            conn.execute(sql)
            db.commit()

    # df.columns = ['tdate', 'product', 'openprice', 'CLOSEPRICE', 'HIGHESTPRICE', 'LOWESTPRICE', 'change', 'change_rate',
    #               'vol', 'amount']
    # try:
    #     df.to_sql(name='carbon_cnemission', con=engine, chunksize=1000, if_exists='append', index=None)
    # except:

