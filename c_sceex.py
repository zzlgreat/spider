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
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Cookie': 'JSESSIONID=3F69FE8CBE6E38EDB55F1AEEFD40E6A7; UM_distinctid=17b5749755c3ca-0f5aabe33d303-4343363-384000-17b5749755d453; scrollTop=253.3333282470703; CNZZDATA1275148414=230976969-1629251666-%7C1629257072',
    'Host': 'ets.sceex.com.cn',
    'Referer': "https://ets.sceex.com.cn/history.htm?k=li_shi_xing_qing&url=mrhq_ls&start_time=2021-08-17&end_time=2021-08-18",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Upgrade-Insecure-Requests':'1',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'sec-ch-ua-mobile': '?0',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1'
}
for i in range(1,62):
    time.sleep(1)
    url = 'https://ets.sceex.com.cn/history.htm?start_time=2017-08-18&end_time=2021-09-01&k=li_shi_xing_qing&url=mrhq_ls&pageIndex='+str(i)
    print(url)
    r = requests.get(url, headers=headers).text.encode('GBK','ignore').decode('GBK')
    soup = BeautifulSoup(r, 'lxml')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table), header=0)[0]

    df.columns = ['tdate', 'product', 'vol','all_vol']
    try:
        df.to_sql(name='carbon_sceex', con=engine, chunksize=1000, if_exists='append', index=None)
    except:
        pass
    print(df)


