#coding=utf-8
import pandas as pd
import requests
import json
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pymysql
import time
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
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'h-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Length': '53',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': '_h_="eNrz4MoqTi0uzszP80yRNE4sT88yKM/MM8hKNcnLKDA0MjItMEwzSBfISSwucUxOBqpMTQnJzE31YWBgrA73+P2MJ7koNbEEqB1FNAoALE8fCg=="',
    'Host': 'td.hxee.com.cn',
    'Origin': 'https://td.hxee.com.cn',
    'Referer': "https://td.hxee.com.cn/quotations/quotation_iframeList_more.htm",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Upgrade-Insecure-Requests':'1',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'sec-ch-ua-mobile': '?0',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1'
}
for i in range(1,3):
    time.sleep(1)
    url = 'https://td.hxee.com.cn/quotations/quotation_iframeList_more.htm'
    print(url)
    data= 'tradeDate=2020-06-19&varietyCode=010001&currentPage='+str(i)
    r = requests.post(url,data=data,headers = headers).content
    #print(r)
    soup = BeautifulSoup(r, 'lxml')
    table = soup.find_all('table')[1]
    df = pd.read_html(str(table), header=0)[0]

    df.columns = ['tdate', 'product', 'closeprice','vol','amount']
    try:
        df.to_sql(name='carbon_hxee', con=engine, chunksize=1000, if_exists='append', index=None)
    except:
        pass
    print(df)


