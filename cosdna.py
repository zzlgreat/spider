import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from sqlalchemy import create_engine
import xlrd
import mysql.connector
from sqlalchemy.dialects.mysql import \
            BLOB, CHAR, DATE, FLOAT , NVARCHAR, TIMESTAMP, VARCHAR,INTEGER
import pymysql
def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: VARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: FLOAT(precision=2, asdecimal=True)})
        #if "float" in str(j):
            #dtypedict.update({i: Numeric(precision=18,scale=4, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: INTEGER()})
    return dtypedict
engine = create_engine("mysql+mysqlconnector://root:123@localhost:3306/tianyancha", echo=False)
headers={
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Cookie': 'PHPSESSID=205k675e5s0roqjrdkens38pv6; __gads=ID=384d39935910af8a:T=1594950693:S=ALNI_MbiYfWaP2O-8U1vYtVQkD6hGKJWYA',
    'Content-Length':'0',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
    'Host': 'www.cosdna.com',
    'Upgrade-Insecure-Requests': '1',
    'Referer': 'http://www.cosdna.com/chs/product.php?q=%E9%9B%85%E8%AF%97%E5%85%B0%E9%BB%9B'
}
db = pymysql.connect(host='localhost',
                       port=3306,
                       database="tianyancha",
                       user="root",
                       password="123",
                       charset="utf8")
conn = db.cursor()
def get_proxies():
    res = json.loads(requests.post("http://47.105.205.177:5010/search").content)
    ip = res.get('result').get('ip')
    port = res.get('result').get('port')
    proxies = {'http': ip + ':' + port}
    proxies.update({'https': ip + ':' + port})
    return proxies

def get_urllist(keyword):
    urllist = []

    while True:
        try:
            proxies = get_proxies()
            print(proxies)
            base = requests.get('http://www.cosdna.com/chs/product.php?q=' + keyword,
                                headers=headers, proxies=proxies, timeout=(3, 12)).content.decode()
            soup = BeautifulSoup(base, 'lxml')
            break
        except:
            pass
    if len(soup.find_all('table'))==0:
        return []
    table = soup.find_all('table')[0]
    for product in table.find_all('tr'):
        url = product.find_all('a',href=True)[1]
        urllist.append({url.get_text().strip(' \n').strip('"').strip(): 'http://www.cosdna.com/' + url['href']})
    try:
        page = soup.find_all('li')[-2]
        page = int(page.text)
        print(page)
    except:
        page=1

    for i in range(2,page+1):
        url_base = 'http://www.cosdna.com/chs/product.php?q='+keyword+'&p='+str(i)
        while True:
            try:
                base = requests.get(url_base, headers=headers, proxies=proxies, timeout=(3, 12)).content.decode()
                soup = BeautifulSoup(base, 'lxml')
                table = soup.find_all('table')[0]
                break
            except:
                pass
        for product in table.find_all('tr'):
            url = product.find_all('a', href=True)[1]
            urllist.append({url.get_text().strip(' \n').strip('"').strip():'http://www.cosdna.com/'+url['href']})
    return urllist
def get_table(url,product,title):


    db_col= {'成分':'ingredient','概略特性':'characteristics','粉刺':'acne','刺激':'stimulate','安心度':'peace'}
    flag =1
    while True:
        if flag>10:
            df = pd.DataFrame({})
            break
        try:
            proxies = get_proxies()
            print(proxies)
            detail = requests.get(url, headers=headers, proxies=proxies, timeout=(3, 12)).content.decode()
            soup = BeautifulSoup(detail, 'lxml')
            table = soup.find_all('table')[0]
            df = pd.read_html(str(table))[0]
            df.rename(columns=db_col, inplace=True)
            df['brand'] = product
            df['url'] = url
            df['product'] = title
            break
        except:
            flag+=1
            pass
    return df


if __name__ == '__main__':
    query = 'select url from cosdna'
    conn.execute(query)
    urls = conn.fetchall()
    existed_urls = []
    for ur in urls:
        existed_urls.append(ur[0])
    workxls = xlrd.open_workbook("化妆品名称.xlsx")
    worksheet = workxls.sheet_by_name("Sheet1")
    row = worksheet.nrows  # 总行数
    for i in range(row):
        rowdate = worksheet.row_values(i)  # i行的list
        for a, b in enumerate(rowdate):
            nm = b.split(".")[-1].strip()
            urllist = get_urllist(nm)
            for url in urllist:
                print(list(url.values())[0],list(url.keys())[0])
                if list(url.values())[0] in existed_urls:
                    pass
                else:
                    try:
                        df = get_table(list(url.values())[0], nm, list(url.keys())[0])
                    except:
                        df = get_table(list(url.values())[0], nm, list(url.keys())[0])
                    df.to_sql(name='cosdna', con=engine, chunksize=1000, if_exists='append', index=None)



