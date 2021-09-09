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
    'Cookie': 'clientlanguage=zh_CN',
    'Host': 'cerx.cn',
    'Origin': 'http://www.dce.com.cn',
    'Referer': "http://cerx.cn/dailynewsCN/index_3.htm",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Upgrade-Insecure-Requests':'1'
}
for i in range(1,404):
    url = 'http://cerx.cn/dailynewsCN/index_'+str(i)+'.htm'
    r = requests.get(url,headers = headers).content
    soup = BeautifulSoup(r, 'lxml')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table), header=0)[0]
    df.columns = ['tdate', 'tIndex', 'openprice', 'HIGHESTPRICE', 'LOWESTPRICE','avgprice',
                  'CLOSEPRICE', 'vol', 'amount']
    try:
        df.to_sql(name='carbon_cerx', con=engine, chunksize=1000, if_exists='append', index=None)
    except:
        pass
    print(df)
