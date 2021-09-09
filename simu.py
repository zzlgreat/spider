# -*- coding: gbk -*-
#爬取中国基金业协会私募信息
import requests
import json
import re
import inspect
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import urllib.error
import time
import string
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import random
import socket
import urllib.parse
from bs4 import BeautifulSoup
import urllib3
urllib3.disable_warnings()
import cx_Oracle
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import VARCHAR,CHAR,DECIMAL,DATE,TEXT,DATETIME
db = pymysql.connect(host="10.110.80.83",
                         port=3306,
                         database="tianyancha",
                         user="root",
                         password="6lfBxZLyopc8Q@UF",
                         charset="utf8")
#db = cx_Oracle.connect('XBRL/123@localhost:1521/orcl.home.zzl.com', encoding='utf-8')
conn = db.cursor()
engine = create_engine("mysql+pymysql://root:6lfBxZLyopc8Q@UF@10.110.80.83:3306/tianyancha", echo=False)
#engine = create_engine("oracle+cx_oracle://XBRL:123@localhost:1521/ORCL", echo=False, encoding='utf-8')

def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: VARCHAR(256)})
        if "float" in str(j):
            dtypedict.update({i: DECIMAL(19,2)})
        if "int" in str(j):
            dtypedict.update({i: VARCHAR(19)})
    return dtypedict

socket.setdefaulttimeout(20)
headers={
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Connection': 'keep-alive',
    'Content-Length':'2',
    'Content-Type':'application/json',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
    'Host': 'gs.amac.org.cn',
    'Referer':'http://gs.amac.org.cn/amac-infodisc/res/pof/manager/index.html',
    'X-Requested-With': 'XMLHttpRequest'
}
headers2={
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Cache-Control': 'max-age=0',
    'Cookie': 'look=first',
    'If-Modified-Since': 'Tue, 28 Apr 2020 01:04:14 GMT',
    'If-None-Match': 'W/"5ea7810e-b572"',
    'Connection': 'keep-alive',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
    'Host': 'gs.amac.org.cn',
    'Referer':'http://gs.amac.org.cn/amac-infodisc/res/pof/manager/index.html',
    'Upgrade-Insecure-Requests': '1'
}

def simu_list():
    for i in range(327, 487):
        print(i)
        time.sleep(0.3)
        params = {'keyword': "有限合伙"}
        url = 'https://gs.amac.org.cn/amac-infodisc/api/pof/fund?rand=0.6843672418985238&page='+str(i)+'&size=100'
        datas = requests.post(url, headers=headers, data=json.dumps(params),verify=False).content.decode('utf-8')
        #print(datas)
        content = json.loads(datas).get('content')

        for s in content:
            #print(s)
            managersInfo = s.get('managersInfo')[0]
            managerId = managersInfo.get('managerId')
            managerUrl = managersInfo.get('managerUrl')
            managerName = managersInfo.get('managerName')
            s.update({'managerId':managerId,'managerUrl':managerUrl,'managerName':managerName})
            del s['managersInfo']
            df = pd.DataFrame.from_dict(data=s, orient='index').T
            # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
            dtypedict = mapping_df_types(df)
            #print(df)
            try:
                df.to_sql(name='simu_list_1', con=engine, chunksize=1000, if_exists='append', index=None,
                          dtype=dtypedict)
            except Exception as e:
                print("?")
                print(e)
def simu_details():
    sql = 'select DISTINCT managerId from SIMU_LIST_1'
    conn.execute(sql)
    symbol_1 = conn.fetchall()
    symbols_1 = []
    for i in symbol_1:
        symbols_1.append(i[0])
    for s in symbols_1:
        url = 'http://gs.amac.org.cn/amac-infodisc/res/pof/manager/'+s
        #url = 'http://gs.amac.org.cn/amac-infodisc/res/pof/manager/101000000350.html'
        res = requests.get(url, headers=headers).content.decode('utf-8')
        #print(res)
        soup = BeautifulSoup(res, 'lxml')
        table = soup.find_all('table')[2]
        df = pd.read_html(str(table), header=0)[0]
        print(df)
        break
        #
if __name__ == '__main__':
    simu_list()
    #simu_details()


