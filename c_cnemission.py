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
    'Cookie': 'UM_distinctid=17b57537cb67c8-0a407cad1f03b1-4343363-384000-17b57537cb7335; JSESSIONID=gwrNhc1fCmhF5csLrm2yS2gN1zQGQtMvT9RRxXnzmWWkbhyvLDvl!-60276040',
    'Host': 'ets.cnemission.com',
    'Referer': "http://ets.cnemission.com/carbon/portalIndex/markethistory?Top=1",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Upgrade-Insecure-Requests':'1'
}
url = 'http://ets.cnemission.com/carbon/portalIndex/markethistory?Top=1&beginTime=2017-08-18&endTime=2021-09-01'
r = requests.get(url,headers = headers).content
soup = BeautifulSoup(r, 'lxml')
table = soup.find_all('table')[1]

df = pd.read_html(str(table), header=0)[0]
df.columns = ['tdate', 'product', 'openprice','CLOSEPRICE', 'HIGHESTPRICE', 'LOWESTPRICE','change','change_rate', 'vol', 'amount']
try:
    df.to_sql(name='carbon_cnemission', con=engine, chunksize=1000, if_exists='append', index=None)
except:
    pass
print(df)
