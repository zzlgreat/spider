import requests
import json
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'Content-Length':'58',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'Hm_lvt_2b3be117d3d815ed2ecc438d0cd036ce=1629336023; ASP.NET_SessionId=144kwrdux0rokxygjizss2cc; Hm_lpvt_2b3be117d3d815ed2ecc438d0cd036ce=1629339650; acw_tc=8cf93d2016293463617998655ecd213d3751e247f4eabfe0c1662e9bd7',
    'Host': 'www.96369.net',
    'Origin': 'http://www.96369.net',
    'Referer': "http://www.96369.net/indices/65",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Upgrade-Insecure-Requests':'1',
}
data = 'txtStartTime=2011-08-03&txtEndTime=2021-08-19&txtyzcode=6'
r = requests.post('http://www.96369.net/indices/65',headers = headers,data = data).content.decode()
print(r)
soup = BeautifulSoup(r, 'lxml')
table = soup.find_all('table')[0]
df = pd.read_html(str(table), header=0)[0]
df.columns = ['tdate', 'price', 'change', 'change_rate']
try:
    df.to_sql(name='carbon_sceex', con=engine, chunksize=1000, if_exists='append', index=None)
except:
    pass
print(df)
