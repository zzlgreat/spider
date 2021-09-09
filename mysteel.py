import requests
import json
import cx_Oracle
import pandas as pd
import datetime
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.dialects.oracle import \
            BFILE, BLOB, CHAR, CLOB, DATE, \
            DOUBLE_PRECISION, FLOAT, INTERVAL, LONG, NCLOB, \
            NUMBER, NVARCHAR, NVARCHAR2, RAW, TIMESTAMP, VARCHAR, \
            VARCHAR2
headers={
    'Accept': 'text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Connection': 'keep-alive',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
    'Referer':'https://index.mysteel.com/xpic/detail.html?tabName=',
    'Cookie':'_last_loginuname=64661657; __gads=ID=dfb5a70169682934:T=1585040610:S=ALNI_MYadB1pRSa6AMbtZpRd3DfXAcXHVA; href=https%3A%2F%2Findex.mysteel.com%2Fxpic%2Fdetail.html%3FtabName%3Dpugang; Hm_lvt_1c4432afacfa2301369a5625795031b8=1590049394,1591313978; 0ddc735e70c097eaf017c46da9861323=; _login_psd=3e3aa30ae03dc74f35169b82909305148; _rememberStatus=true; accessId=5d36a9e0-919c-11e9-903c-ab24dbab411b; _last_ch_r_t=1591316918555; qimo_seosource_5d36a9e0-919c-11e9-903c-ab24dbab411b=%E5%85%B6%E4%BB%96%E7%BD%91%E7%AB%99; qimo_seokeywords_5d36a9e0-919c-11e9-903c-ab24dbab411b=%E6%9C%AA%E7%9F%A5; pageViewNum=9; Hm_lpvt_1c4432afacfa2301369a5625795031b8=1591330655',
    'Host': 'index.mysteel.com',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Dest': 'empty',
    'X-Requested-With': 'XMLHttpRequest'
}
# engine = create_engine('oracle+cx_oracle://XBRL:123@ORCL')
# db = cx_Oracle.connect('XBRL/123@localhost:1521/orcl.home.zzl.com')

def get_day(y,m,d):
    today = datetime.date.today()
    begin = datetime.date(y,m,d)
    days = []
    for i in range((today - begin).days + 1):
        day = begin + datetime.timedelta(days=i)
        date= str(day).split('-')
        days.append(date)
    return days

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
def base():
    today = datetime.date.today()
    x = requests.get(
        'https://index.mysteel.com/newxpic/getReport.ms?callback=detailTable8888&typeName=%25E5%2594%2590%25E5%25B1%25B1%25E9%2592%25A2%25E5%259D%25AF&tabName=TS_GP_ZH&dateType=day&startTime=2017-06-01&endTime=&returnType=&_=1591316322600').content.decode(
        'utf8')
    tsgp = json.loads(x[16:-2]).get('data')
    tk = requests.get(
        'https://index.mysteel.com/newxpic/getReport.ms?callback=detailTable945&typeName=%25E7%259F%25BF%25E7%259F%25B3%25E7%25BB%25BC%25E5%2590%2588&tabName=KUANGSHIZONGHE_ABS&dateType=day&startTime=2017-06-01&endTime=&returnType=&_=1591331823674').content.decode(
        'utf8')
    tkszs = json.loads(tk[15:-2]).get('data')
    lw = requests.get(
        'https://index.mysteel.com/newxpic/getReport.ms?callback=detailTable6175&typeName=%25E4%25B8%258A%25E6%25B5%25B7%25E8%259E%25BA%25E7%25BA%25B9%25E9%2592%25A2%25E4%25BA%25BA%25E6%25B0%2591%25E5%25B8%2581%25E8%25AE%25A1%25E4%25BB%25B7&tabName=LUOWEN_SH&dateType=day&startTime=&endTime=&returnType=&_=1591317057586').content.decode(
        'utf8')
    lwgzs = json.loads(lw[16:-2]).get('data')
    pg = requests.get(
        'https://index.mysteel.com/newxpic/getReport.ms?callback=detailTable8962&typeName=%25E9%2592%25A2%25E6%259D%2590%25E7%25BB%25BC%25E5%2590%2588&tabName=GANGCAIZONGHE_ABS&dateType=day&startTime=2017-06-01&endTime=&returnType=&_=1591331683843').content.decode(
        'utf8')
    pgzs = json.loads(pg[16:-2]).get('data')
    jt = requests.get(
        'https://index.mysteel.com/newxpic/getReport.ms?callback=detailTable71&typeName=%25E7%2584%25A6%25E7%2582%25AD%25E7%25BB%25BC%25E5%2590%2588&tabName=JT_ZONGHE_MD&dateType=day&startTime=2017-06-01&endTime=' + str(
            today) + '&returnType=&_=1591330786704').content.decode(
        'utf8')
    print(jt)
    jtzs = json.loads(jt[14:-2]).get('data')
    mysteel = [tsgp, tkszs, lwgzs, pgzs, jtzs]
    database = ['ODS_GZJ_TSGP', "ODS_GZJ_TKSZS", 'ODS_GZJ_LWGZS', 'ODS_GZJ_PGZS', 'ODS_GZJ_JTZS']
    db = pymysql.connect(host="10.110.80.83",
                         port=3306,
                         database="tianyancha",
                         user="root",
                         password="6lfBxZLyopc8Q@UF",
                         charset="utf8")
    engine = create_engine("mysql+pymysql://root:6lfBxZLyopc8Q@UF@10.110.80.83:3306/tianyancha", echo=False)
    conn = db.cursor()
    for i, s in zip(mysteel, database):
        for d in i:
            sql = '''SELECT "date" FROM "''' + s + '"'
            conn.execute(sql)
            times = conn.fetchall()
            time_had = []
            for time_1 in times:
                i = ''.join(time_1)
                time_had.append(i)
            if d.get('date') in time_had:
                pass
            else:
                df = pd.DataFrame.from_dict(data=d, orient='index').T
                dtypedict = mapping_df_types(df)
                df.to_sql(name=s.lower(), con=engine, chunksize=1000, if_exists='append', index=None,
                          dtype=dtypedict)
    conn.close()
    db.close()
if __name__ == '__main__':
    base()



