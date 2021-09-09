import requests
import json
import cx_Oracle
import pandas as pd
import datetime
import time
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from sqlalchemy.dialects.oracle import \
            BFILE, BLOB, CHAR, CLOB, DATE, \
            DOUBLE_PRECISION, FLOAT, INTERVAL, LONG, NCLOB, \
            NUMBER, NVARCHAR, NVARCHAR2, RAW, TIMESTAMP, VARCHAR, \
            VARCHAR2
db = cx_Oracle.connect('lcgs709999/Abcd1234@10.0.19.92:1521/snyx')
engine = create_engine('oracle+cx_oracle://lcgs709999:Abcd1234@10.0.19.92:1521/snyx')
# db = cx_Oracle.connect('XBRL/123@localhost:1521/orcl.home.zzl.com')
# engine = create_engine('oracle://XBRL:123@127.0.0.1:1521/orcl')
conn = db.cursor()
url = 'http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html'
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Length': '68',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'JSESSIONID=3E0648170D0D9A2CC02AE02DF800426A; WMONID=DbHTcZAjzkj; Hm_lvt_a50228174de2a93aee654389576b60fb=1585195821; Hm_lpvt_a50228174de2a93aee654389576b60fb=1585195821',
    'Host': 'www.dce.com.cn',
    'Origin': 'http://www.dce.com.cn',
    'Referer': "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html",
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'Upgrade-Insecure-Requests':'1'
}
def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: VARCHAR(256)})
        if "float" in str(j):
            dtypedict.update({i: NUMBER(19,2)})
        if "int" in str(j):
            dtypedict.update({i: VARCHAR(19)})
    return dtypedict

def get_day(y,m,d):
    today = datetime.date.today()
    begin = datetime.date(y,m,d)
    days = []
    for i in range((today - begin).days + 1):
        day = begin + datetime.timedelta(days=i)
        date= str(day).split('-')
        days.append(date)
    return days

def get_data(db,data):
    database = cx_Oracle.connect('lcgs709999/Abcd1234@10.0.19.92:1521/snyx')
    engine = create_engine('oracle+cx_oracle://lcgs709999:Abcd1234@10.0.19.92:1521/snyx')
    # db = cx_Oracle.connect('XBRL/123@localhost:1521/orcl.home.zzl.com')
    # engine = create_engine('oracle://XBRL:123@127.0.0.1:1521/orcl')
    conn = database.cursor()
    query = 'SELECT UPDATE_TIME FROM "'+db+'" order BY UPDATE_TIME DESC'
    conn.execute(query)
    ymd = conn.fetchall()[0][0]
    print(int(ymd[:4]),int(ymd[4:6]),int(ymd[-2:]))
    days = get_day(int(ymd[:4]),int(ymd[4:6]),int(ymd[-2:]))
    for day in days:
        time.sleep(2)
        try:
            params = {'dayQuotes.variety': data, 'dayQuotes.trade_type': '0', 'year': day[0],
                      'month': str(int(day[1]) - 1),
                      'day': day[2]}
            # print(params)
            # res = requests.post(url, headers=headers, data=params).content.decode('utf-8')
            res = requests.post(url, headers=headers, data=params).content
            soup = BeautifulSoup(res, 'lxml')
            table = soup.find_all('table')[0]
            df = pd.read_html(str(table), header=0)[0]
            print(df)
            update_time = day[0] + day[1] + day[2]
            df.columns = ['PRODUCTNAME', 'DELIVERYMONTH', 'OPENPRICE', 'HIGHESTPRICE', 'LOWESTPRICE',
                          'CLOSEPRICE', 'PRESETTLEMENTPRICE', 'SETTLEMENTPRICE', 'ZD_CHG', 'ZD1_CHG', 'VOLUME',
                          'OPENINTEREST', 'OPENINTERESTCHG', 'TASVOLUME']
            df['update_time'] = update_time
            # print(df)
            dtypedict = mapping_df_types(df)
            df.to_sql(name=db, con=engine, chunksize=1000, if_exists='append', index=None, dtype=dtypedict)
        except:
            pass

        conn.close()
        database.close()

        # try:
        #
        # except Exception as e:
        #     print(e)

if __name__ == '__main__':
    get_data('ODS_DCE_JT','j')#焦炭
    get_data( 'ODS_DCE_JM', 'jm')#焦煤
    get_data( 'ODS_DCE_IRON', 'i')#铁