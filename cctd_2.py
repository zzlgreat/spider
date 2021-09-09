# -*- coding: gbk -*-
import requests
import json
import urllib.error
import time
import brotli
import string
import os
import random
import socket
import urllib.parse
from bs4 import BeautifulSoup
import cx_Oracle
import pymysql
from sqlalchemy import create_engine
import urllib3
urllib3.disable_warnings()
from sqlalchemy import create_engine
from sqlalchemy.dialects.oracle import \
            BFILE, BLOB, CHAR, CLOB, DATE, \
            DOUBLE_PRECISION, FLOAT, INTERVAL, LONG, NCLOB, \
            NUMBER, NVARCHAR, NVARCHAR2, RAW, TIMESTAMP, VARCHAR, \
            VARCHAR2
#db = cx_Oracle.connect('lcgs709999/Abcd1234@snyx')
#db = pymysql.connect(host="36.133.123.8",port=3306,database="lcgs709999",user="root",password="Abcd1234")
#engine = create_engine("mysql+pymysql://root:Abcd1234@36.133.123.8:3306/lcgs709999", echo=False)

db = pymysql.connect(host="192.168.50.18",
                         port=3306,
                         database="stockinfo",
                         user="root",
                         password="123456",
                         charset="utf8")
engine = create_engine("mysql+pymysql://root:123456@192.168.50.18:3306/stockinfo", echo=False)
conn = db.cursor()

socket.setdefaulttimeout(20)
headers={
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'path': '/datasql.php?data=%C8%AB%B9%FA%D4%AD%C3%BA%B2%FA%C1%BF(%D4%CB%CF%FA%D0%AD%BB%E1)&name=%C8%AB%B9%FA%D4%AD%C3%BA%B2%FA%C1%BF(%D4%CB%CF%FA%D0%AD%BB%E1)&time=%20where%20%20DATE_FORMAT(END_DATE,%27%Y-%m-%d%27)%20%3E=%20%27%27&draw=1&columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=false&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=false&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=2&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=false&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=3&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=false&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=4&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=false&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=5&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=false&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=6&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=false&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=7&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=false&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=8&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=false&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=9&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=false&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&start=0&length=30&search%5Bvalue%5D=&search%5Bregex%5D=false&extra_search=&_=1626914980376',
    'Connection': 'keep-alive',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
    'Referer':'https://www.cctd.com.cn/index.php?m=content&c=index&a=lists&catid=186&data=%B7%D6%B9%FA%B1%F0(%B5%D8%C7%F8)%BD%F8%BF%DA%C1%BF(%C1%B6%BD%B9%C3%BA)&tagle=13515&datatype=Z0381&name=%B7%D6%B9%FA%B1%F0(%B5%D8%C7%F8)%BD%F8%BF%DA%C1%BF(%C1%B6%BD%B9%C3%BA)',
    'Host': 'www.cctd.com.cn',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'X-Requested-With': 'XMLHttpRequest',
    'cookie':'PHPSESSID=lo0vpul1ufmb40fe83s5vicvr6; Hm_lvt_594886944cf2480d17095af56ff618e2=1620722459; Hm_lpvt_594886944cf2480d17095af56ff618e2=1620722460; lNGUp_auth=d7a046_WrS2Zj6WDLXaCDBKBY_z8BUgzy_z4BmZHs2xA7uBrqngoaLKDQIPaDieB1WEzM_urYdnR1Dh0wpWHrMHAkSFRNDQlTIpRhZzA5rX22clqM3LMdKcqr3RiVIL7qiWarOmN2Y_MHLvzN3zlLkqBAvvSzxI; lNGUp__userid=44b6r-xu0NPWyZHoId-D8Y2TsvBaONqGxBTD9JEq1FL2wA; lNGUp__username=137053ORiraHOYR2Vz80B7PQezwwc8N9psS0mHou4r6TRd1G7Q; lNGUp__groupid=1c6d1JWrQ-pJM9IVuEALR5Shv2TCx1J5GaXh0RS77w; lNGUp__nickname=8fa428CxaK8liT3Z8QygtbOnEF2t8c8WeLxJwv6AS7YOInKecGrYYOQGjzfuY9nTyg; acw_tc=8cf93c2016269149614981078e4270a8585848a5f3ecb93a9801267ef8'
}

#判断是否含有字段
def find_string(s,list):
    i = 0
    if s !=None and str(s) != '':
        for t in list:
            if str(s) in str(t):
                i += 1
        if i == 0:
            return False
        else:
            return True
    else:
        return True


def transform(chinese_str):
    url = ''
    # 先进行gb2312编码
    for i in chinese_str:
        if '(' == i or ')' == i:
            x = i
            url += x
        else:
            i = i.encode('gb2312')
            x = urllib.parse.quote(i)
            url += x
    return url

def update(key_name,num):

    name = list(all_dic.get(key_name).keys())
    names = []
    for nm in name:
        if nm == 'db_name' or nm == 'db_fields':
            pass
        else:
            names.append(nm)
    db_name = all_dic.get(key_name).get('db_name')   #获取数据库名

    print(names)
    for nm in names:
        # query = "select column_name from all_tab_columns where Table_Name= '" + db_name + "'"
        # conn.execute(query)
        # fields_1 = conn.fetchall()
        # fields = []
        # for i in fields_1:
        #     fields.append(i[0])
        # fields = reversed(fields)
        # fields = tuple(fields)  # 获取字段
        db_fields = tuple(all_dic.get(key_name).get(nm).get('db_fields'))
        print(nm)
        print(db_fields)
        if 'port_nm' in db_fields:
            get_date = '''select date from ''' + db_name + '' + ' where port_nm like ' + "'" + nm + "'"
        elif 'table_nm' in db_fields:
            get_date = '''select date from ''' + db_name + '' + ' where table_nm like ' + "'" + nm + "'"
        elif 'address' in db_fields:
            get_date = '''select date from ''' + db_name + '' + ' where address like ' + "'" + all_dic.get(
                key_name).get(nm).get('data') + "'"
        else:
            get_date = '''select date from ''' + db_name + ''
        print(get_date)
        conn.execute(get_date)
        fullname = conn.fetchall()
        dates = []

        for i in fullname:
            dates.append(i[0])
        db_fields = tuple(all_dic.get(key_name).get(nm).get('db_fields'))
        length = len(db_fields)
        data = transform(all_dic.get(key_name).get(nm).get('data'))
        name = transform(all_dic.get(key_name).get(nm).get('name'))   #获取表格中的数据
        ctimr = str(int(float(time.time())*1000))
        url_sql = "https://www.cctd.com.cn/datasql.php?data="+data+"&name="+name+"&time= where  DATE_FORMAT(END_DATE,'%Y-%m-%d') >= '-0021-11-30'&draw=1&columns[0][data]=0&columns[0][name]=&columns[0][searchable]=true&columns[0][orderable]=false&columns[0][search][value]=&columns[0][search][regex]=false&columns[1][data]=1&columns[1][name]=&columns[1][searchable]=true&columns[1][orderable]=false&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=2&columns[2][name]=&columns[2][searchable]=true&columns[2][orderable]=false&columns[2][search][value]=&columns[2][search][regex]=false&columns[3][data]=3&columns[3][name]=&columns[3][searchable]=true&columns[3][orderable]=false&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=4&columns[4][name]=&columns[4][searchable]=true&columns[4][orderable]=false&columns[4][search][value]=&columns[4][search][regex]=false&columns[5][data]=5&columns[5][name]=&columns[5][searchable]=true&columns[5][orderable]=false&columns[5][search][value]=&columns[5][search][regex]=false&columns[6][data]=6&columns[6][name]=&columns[6][searchable]=true&columns[6][orderable]=false&columns[6][search][value]=&columns[6][search][regex]=false&columns[7][data]=7&columns[7][name]=&columns[7][searchable]=true&columns[7][orderable]=false&columns[7][search][value]=&columns[7][search][regex]=false&columns[8][data]=8&columns[8][name]=&columns[8][searchable]=true&columns[8][orderable]=false&columns[8][search][value]=&columns[8][search][regex]=false&columns[9][data]=9&columns[9][name]=&columns[9][searchable]=true&columns[9][orderable]=false&columns[9][search][value]=&columns[9][search][regex]=false&start="+str(num)+"&length=30&search[value]=&search[regex]=false&extra_search=&_="+ctimr
        #print(url_sql)
        dic = {'Referer': 'https://www.cctd.com.cn/index.php?m=content&c=index&a=lists&catid=183&data=' + data + '&tagle=13515&datatype=Z0002&name=' + name,
                   'path':"/datasql.php?data="+data+"&name="+name+"&time= where  DATE_FORMAT(END_DATE,'%Y-%m-%d') >= '-0021-11-30'&draw=1&columns[0][data]=0&columns[0][name]=&columns[0][searchable]=true&columns[0][orderable]=false&columns[0][search][value]=&columns[0][search][regex]=false&columns[1][data]=1&columns[1][name]=&columns[1][searchable]=true&columns[1][orderable]=false&columns[1][search][value]=&columns[1][search][regex]=false&columns[2][data]=2&columns[2][name]=&columns[2][searchable]=true&columns[2][orderable]=false&columns[2][search][value]=&columns[2][search][regex]=false&columns[3][data]=3&columns[3][name]=&columns[3][searchable]=true&columns[3][orderable]=false&columns[3][search][value]=&columns[3][search][regex]=false&columns[4][data]=4&columns[4][name]=&columns[4][searchable]=true&columns[4][orderable]=false&columns[4][search][value]=&columns[4][search][regex]=false&columns[5][data]=5&columns[5][name]=&columns[5][searchable]=true&columns[5][orderable]=false&columns[5][search][value]=&columns[5][search][regex]=false&columns[6][data]=6&columns[6][name]=&columns[6][searchable]=true&columns[6][orderable]=false&columns[6][search][value]=&columns[6][search][regex]=false&columns[7][data]=7&columns[7][name]=&columns[7][searchable]=true&columns[7][orderable]=false&columns[7][search][value]=&columns[7][search][regex]=false&columns[8][data]=8&columns[8][name]=&columns[8][searchable]=true&columns[8][orderable]=false&columns[8][search][value]=&columns[8][search][regex]=false&columns[9][data]=9&columns[9][name]=&columns[9][searchable]=true&columns[9][orderable]=false&columns[9][search][value]=&columns[9][search][regex]=false&start="+str(num)+"&length=30&search[value]=&search[regex]=false&extra_search=&_="+ctimr}
        headers.update(dic)
        try:
            response = requests.get(url_sql, headers=headers, verify=False)

        except:
            time.sleep(2)
            response = requests.get(url_sql, headers=headers, verify=False)
        key = 'Content-Encoding'
        if (key in response.headers and response.headers['Content-Encoding'] == 'br'):
            data = brotli.decompress(response.content)
            text = data.decode('utf-8')
        else:
            print(response.text.encode('raw_unicode_escape'))
            text = response.text.encode('raw_unicode_escape').decode('gbk')

        print(text)
        con = json.loads(text)
        piece = con.get('data')
        for x in piece:
            if find_string(x[0],dates):
                pass
            else:
                lists_1 = x
                lists = []
                for strs in lists_1:
                    if strs == None:
                        strs = ''
                    lists.append(strs)
                length_1 = len(lists)
                if length_1 > length-1:
                    lists = lists[:length-1]
                else:
                    pass
                lists.append(nm)
                lists = tuple(lists)
                insert_brief = '''insert into '''+db_name+str(db_fields).replace("'", '`')+''' values '''
                print(insert_brief + str(lists))
                conn.execute(insert_brief + str(lists))
                conn.execute('commit')

if __name__ == '__main__':
    all_dic = {
        '煤价指数': {'db_name': 'ODS_MTZYW_INDEX',
                 '中国煤炭价格指数': {'data': '中国煤炭价格指数', 'name': '中国煤炭价格指数',
                           'db_fields': ['date', 'type', 'current', 'MOM_percent', 'MOM_point',
                                         'table_nm']},
                 '中国煤炭价格指数(喷吹煤)': {'data': '中国煤炭价格指数(喷吹煤)', 'name': '中国煤炭价格指数(喷吹煤)',
                          'db_fields': ['date', 'type', 'current', 'MOM_percent', 'MOM_point',
                                         'table_nm']},
                 '环渤海动力煤价格指数': {'data': '环渤海动力煤价格指数', 'name': '环渤海动力煤价格指数',
                                   'db_fields': ['date', 'type', 'current', 'MOM_percent', 'MOM_point',
                                                 'table_nm']}
                 },
        '化工品价格': {'db_name': 'ods_mtzyw_chem_price',
                 '甲醇价格': {'data': '甲醇价格', 'name': '甲醇价格',
                              'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                            'remark','table_nm']},
                  '甲醛价格': {'data': '甲醛价格', 'name': '甲醛价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '纯碱价格': {'data': '纯碱价格', 'name': '纯碱价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '粗苯价格': {'data': '粗苯价格', 'name': '粗苯价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '醋酐价格': {'data': '醋酐价格', 'name': '醋酐价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '醋酸乙酯价格': {'data': '醋酸乙酯价格', 'name': '醋酸乙酯价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '电石价格': {'data': '电石价格', 'name': '电石价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '沥青价格': {'data': '沥青价格', 'name': '沥青价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '烧碱价格': {'data': '烧碱价格', 'name': '烧碱价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '顺酐价格': {'data': '顺酐价格', 'name': '顺酐价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '液氨价格': {'data': '液氨价格', 'name': '液氨价格',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                 },
        '煤炭产量': {'db_name': 'ODS_MTZYW_PRODUCT_COAL',
                 '重点煤矿产量': {'data': '重点煤矿产量', 'name': '重点煤矿产量',
                            'db_fields': ['date', 'distinct', 'Degree_value', 'Degree_MoM', 'Degree_YoY', 'table_nm']},
                 '国有重点煤矿原煤产量': {'data': '国有重点煤矿原煤产量', 'name': '国有重点煤矿原煤产量',
                                'db_fields': ['date', 'Data_caliber', 'distinct', 'value_mon', 'YoY_growth',
                                              'Cum_value', 'Cum_change', 'Daily_pro', 'table_nm']},
                 '非国有煤矿原煤产量': {'data': '非国有煤矿原煤产量', 'name': '非国有煤矿原煤产量',
                               'db_fields': ['date', 'Cum_value', 'Cum_growth', 'value_mon', 'Daily_pro', 'table_nm']},
                 '大型煤炭企业产量': {'data': '大型煤炭企业产量', 'name': '大型煤炭企业产量',
                              'db_fields': ['date', 'Coal_type', 'Cum_value', 'Cum_growth', 'Daily_pro', 'table_nm']},
                 '全国原煤产量(统计局)': {'data': '全国原煤产量(统计局)', 'name': '全国原煤产量(统计局)',
                                 'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'Cum_value', 'Cum_growth',
                                               'table_nm']},
                 '原煤产量': {'data': '原煤产量', 'name': '原煤产量',
                          'db_fields': ['date', 'Cum_value', 'Cum_growth', 'value_mon', 'Daily_pro', 'table_nm']},
                 '商品煤产量': {'data': '商品煤产量', 'name': '商品煤产量',
                           'db_fields': ['date', 'Cum_value', 'Cum_growth', 'value_mon', 'Daily_pro', 'table_nm']},
                 '全国原煤产量(运销协会)': {'data': '全国原煤产量(运销协会)', 'name': '全国原煤产量(运销协会)',
                                  'db_fields': ['date', 'value_mon', 'value_period', 'YOY_change', 'YoY_growth',
                                                'Cum_value', 'Cum_period', 'Cum_change', 'Cum_growth', 'table_nm']},
                 '全国原煤产量(工业协会)': {'data': '全国原煤产量(工业协会)', 'name': '全国原煤产量(工业协会)',
                                  'db_fields': ['date', 'Cum_value', 'Cum_growth', 'Daily_pro', 'table_nm']},
                 '全国商品煤产量': {'data': '全国商品煤产量', 'name': '全国商品煤产量',
                             'db_fields': ['date', 'Cum_value', 'Cum_growth', 'value_mon', 'Daily_pro', 'table_nm']},
                 '大型煤炭企业洗精煤产量': {'data': '大型煤炭企业洗精煤产量', 'name': '大型煤炭企业产量',
                                 'db_fields': ['date', 'Coal_type', 'Cum_value', 'Cum_growth', 'table_nm']},
                 },
        '产量': {'db_name': 'ODS_MTZYW_PRODUCT',
               '铁矿石': {'data': '铁矿石', 'name': '铁矿石',
                       'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               '焦炭': {'data': '焦炭', 'name': '焦炭',
                      'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               '粗钢': {'data': '粗钢', 'name': '粗钢',
                      'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               '生铁': {'data': '粗钢', 'name': '粗钢',
                      'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               '水泥': {'data': '水泥', 'name': '水泥',
                      'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               '平板玻璃': {'data': '平板玻璃', 'name': '平板玻璃',
                        'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                      'table_nm']},
               '风力发电量': {'data': '风力发电量', 'name': '风力发电量',
                         'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                       'table_nm']},
               '火力发电量': {'data': '火力发电量', 'name': '火力发电量',
                         'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                       'table_nm']},
               '太阳能发电量': {'data': '太阳能发电量', 'name': '太阳能发电量',
                          'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                        'table_nm']},
               '核能发电量': {'data': '核能发电量', 'name': '核能发电量',
                         'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                       'table_nm']},
               '全国总发电量': {'data': '全国总发电量', 'name': '全国总发电量',
                          'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                        'table_nm']},
               },
        '期货': {'db_name': 'ODS_MTZYW_FUTURE_PRICE',
               '动力煤期货价格': {'data': '动力煤期货价格', 'name': '动力煤期货价格',
                           'db_fields': ['date', 'data_nm', 'price_type', 'price', 'MoM_increase', 'table_nm']},
               '铁矿石期货价格': {'data': '铁矿石期货价格', 'name': '铁矿石期货价格',
                           'db_fields': ['date', 'data_nm', 'price_type', 'price', 'MoM_increase', 'table_nm']},
               '焦炭期货价格': {'data': '焦炭期货价格', 'name': '焦炭期货价格',
                          'db_fields': ['date', 'data_nm', 'price_type', 'price', 'MoM_increase', 'table_nm']},
               '螺纹钢期货价格': {'data': '螺纹钢期货价格', 'name': '螺纹钢期货价格',
                           'db_fields': ['date', 'data_nm', 'price_type', 'price', 'MoM_increase', 'table_nm']},
               },
        '消费': {'db_name': 'ODS_MTZYW_CONSUME',
               '全社会用电量': {'data': '全社会用电量', 'name': '全社会用电量',
                          'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']},
               '第一产业用电量': {'data': '第一产业用电量', 'name': '第一产业用电量',
                           'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']},
               '第二产业用电量': {'data': '第二产业用电量', 'name': '第二产业用电量',
                           'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']},
               '第三产业用电量': {'data': '第三产业用电量', 'name': '第三产业用电量',
                           'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']},
               '城乡居民生活用电': {'data': '城乡居民生活用电', 'name': '城乡居民生活用电',
                            'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']}
               },
        '煤炭消费': {'db_name': 'ODS_MTZYW_CONSUME_COAL',
                 '全国商品煤消费量': {'data': '全国商品煤消费量', 'name': '全国商品煤消费量',
                              'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum',
                                            'grid', 'value_per', 'value_YoY', 'table_nm']},
                 '全国各电网煤炭消费量': {'data': '全国各电网煤炭消费量', 'name': '全国各电网煤炭消费量',
                                'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum',
                                              'grid', 'value_per', 'value_YoY', 'table_nm']},
                 '四大行业商品煤消费量': {'data': '四大行业商品煤消费量', 'name': '四大行业商品煤消费量',
                                'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum',
                                              'grid', 'value_per', 'value_YoY', 'table_nm']},
                 },
        '销量': {'db_name': 'ODS_MTZYW_SALES_COAL',
               '全国煤炭销量': {'data': '全国煤炭销量', 'name': '全国煤炭销量',
                          'db_fields': ['date', 'value_mon', 'change_YoY', 'growth_YoY', 'value_cum', 'changel_cum',
                                        'growth_cum', 'table_nm']},
               '国有重点煤矿总销量': {'data': '国有重点煤矿总销量', 'name': '国有重点煤矿总销量',
                             'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']},
               '国有重点煤矿供电力销量': {'data': '国有重点煤矿供电力销量', 'name': '国有重点煤矿供电力销量',
                               'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']},
               '国有重点煤矿供冶金销量': {'data': '国有重点煤矿供冶金销量', 'name': '国有重点煤矿供冶金销量',
                               'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']}
               },
        '焦炭价格': {'db_name': 'ODS_MTZYW_PRICE_COKE',
                 '安徽地区冶金焦价格': {'data': '安徽地区冶金焦价格', 'name': '安徽地区冶金焦价格',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '河南地区冶金焦价格': {'data': '河南地区冶金焦价格', 'name': '河南地区冶金焦价格',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '江苏地区冶金焦价格': {'data': '江苏地区冶金焦价格', 'name': '江苏地区冶金焦价格',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '山东地区冶金焦价格': {'data': '山东地区冶金焦价格', 'name': '山东地区冶金焦价格',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '山西地区冶金焦价格': {'data': '山西地区冶金焦价格', 'name': '山西地区冶金焦价格',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '河北地区冶金焦价格': {'data': '河北地区冶金焦价格', 'name': '河北地区冶金焦价格',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '山西地区铸造焦价格': {'data': '山西地区铸造焦价格', 'name': '山西地区铸造焦价格',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'table_nm']},
                 '河北地区铸造焦价格': {'data': '河北地区铸造焦价格', 'name': '河北地区铸造焦价格',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'table_nm']},
                 },
        '焦炭企业价格': {'db_name': 'ODS_MTZYW_PRICE_COKE_BY_CO',
                   '钢企采购价': {'data': '钢企采购价', 'name': '钢企采购价',
                             'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark', 'Co_name',
                                           'table_name']},
                   '焦企报价': {'data': '焦企报价', 'name': '焦企报价',
                            'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark', 'Co_name',
                                          'table_name']}
                   },
        '钢材价格': {'db_name': 'ODS_MTZYW_PRICE_STEEL',
                 '高线价格': {'data': '高线价格', 'name': '高线价格',
                          'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                        'Steel_type']},
                 '三级螺纹钢价格': {'data': '三级螺纹钢价格', 'name': '三级螺纹钢价格',
                             'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                           'Steel_type']},
                 '热卷板价格': {'data': '热卷板价格', 'name': '热卷板价格',
                           'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                         'Steel_type']},
                 '冷板价格': {'data': '冷板价格', 'name': '冷板价格',
                          'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                        'Steel_type']},
                 '普中板价格': {'data': '普中板价格', 'name': '普中板价格',
                           'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                         'Steel_type']},
                 },
        '产能': {'db_name': 'ODS_MTZYW_CAPACITY',
               '水泥熟料产能': {'data': '水泥熟料产能', 'name': '水泥熟料产能',
                          'db_fields': ['date', 'distinct', 'Co_name', 'Co_capacity', 'table_nm']},
               },
        # '环渤海参考价': {'db_name':'ODS_MTZYW_PRICE_HBH_5500K',
        #     'data': '环渤海参考价', 'name': '环渤海参考价'},
        '煤炭价格': {'db_name': 'ODS_MTZYW_PRICE_BY_ADDRESS',
                 '秦皇岛港煤炭价格': {'data': '秦皇岛', 'name': '港煤炭价格',
                              'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type', 'address']},
                 '曹妃甸港煤炭价格': {'data': '曹妃甸', 'name': '港煤炭价格',
                              'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type', 'address']},
                 # '国投京唐港煤炭价格': {'data': '国投京唐', 'name': '港煤炭价格',
                 #               'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                 #                             'Heat', 'price', 'Price_type', 'address']},
                 '黄骅港煤炭价格': {'data': '黄骅', 'name': '港煤炭价格',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '京唐港煤炭价格': {'data': '京唐', 'name': '港煤炭价格',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '广州港煤炭价格': {'data': '广州', 'name': '港煤炭价格',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                           'Heat', 'price', 'Price_type', 'address']},
                 '唐山市煤炭价格': {'data': '唐山市', 'name': '煤炭价格',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '灵石县煤炭价格': {'data': '灵石县', 'name': '煤炭价格',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '淮南市煤炭价格': {'data': '淮南市', 'name': '煤炭价格',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '柳林县煤炭价格': {'data': '柳林县', 'name': '煤炭价格',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']}
                 },
        '煤企报价': {'db_name': 'ODS_MTZYW_PRICE_BY_CO',
                 '神华集团煤炭现货价格': {'data': '神华集团煤炭现货价格', 'name': '神华集团煤炭现货价格',
                                'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                              'Heat', 'price', 'Price_type']},
                 '神华集团煤炭长协价格': {'data': '神华集团煤炭长协价格', 'name': '神华集团煤炭长协价格',
                                'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                              'Heat', 'price', 'Price_type']},
                 '中煤集团煤炭价格': {'data': '中煤集团煤炭价格', 'name': '中煤集团煤炭价格',
                              'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type']},
                 '淮北矿业煤炭价格': {'data': '淮北矿业煤炭价格', 'name': '淮北矿业煤炭价格',
                              'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type']},
                 '淮南矿业煤炭价格': {'data': '淮南矿业煤炭价格', 'name': '淮南矿业煤炭价格',
                              'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type']},
                 '山东能源煤炭价格': {'data': '山东能源煤炭价格', 'name': '山东能源煤炭价格',
                              'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type']}
                 },
        '煤炭运输': {'db_name': 'ODS_MTZYW_TRANSPORT',
                 '全国铁路货物发运量': {'data': '全国铁路货物发运量', 'name': '全国铁路货物发运量',
                               'db_fields': ['date', 'value_mon', 'value_per', 'change_YoY', 'growth_YoY', 'value_cum',
                                             'value_YoY', 'change_cum', 'growth_cum', 'table_nm']},
                 '全国铁路煤炭发运量': {'data': '全国铁路煤炭发运量', 'name': '全国铁路煤炭发运量',
                               'db_fields': ['date', 'value_mon', 'value_per', 'change_YoY', 'growth_YoY', 'value_cum',
                                             'value_YoY', 'change_cum', 'growth_cum', 'table_nm']},
                 '全国铁路电煤发运量': {'data': '全国铁路电煤发运量', 'name': '全国铁路电煤发运量',
                               'db_fields': ['date', 'value_mon', 'value_per', 'change_YoY', 'growth_YoY', 'value_cum',
                                             'value_YoY', 'change_cum', 'growth_cum', 'table_nm']},
                 '国有重点煤矿发运量': {'data': '国有重点煤矿发运量', 'name': '国有重点煤矿发运量',
                               'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']},
                 '大秦线发运量': {'data': '大秦线发运量', 'name': '大秦线发运量',
                            'db_fields': ['date', 'value_mon', 'value_per', 'change_YoY', 'growth_YoY', 'value_cum',
                                          'value_YoY', 'change_cum', 'growth_cum', 'table_nm']}
                 },
        '煤炭库存': {'db_name': 'ODS_MTZYW_INVENTORY_COAL',
                 '全国煤炭企业库存': {'data': '全国煤炭企业库存', 'name': '全国煤炭企业库存',
                              'db_fields': ['date', 'value_mon', 'change_YoY', 'table_nm']},
                 '国有重点煤矿库存': {'data': '国有重点煤矿库存', 'name': '国有重点煤矿库存',
                              'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']},
                 '全国各电网煤炭库存': {'data': '全国各电网煤炭库存', 'name': '全国各电网煤炭库存',
                               'db_fields': ['date', 'grid', 'value_mon', 'value_per', 'change_YoY', 'table_nm']}
                 },
        '库存': {'db_name': 'ODS_MTZYW_INVENTORY',
               '港口焦炭库存': {'data': '港口焦炭库存', 'name': '港口焦炭库存', 'db_fields': ['date', 'port', 'quatity', 'product']},
               '钢材社会库存': {'data': '钢材社会库存', 'name': '钢材社会库存', 'db_fields': ['date', 'product', 'quatity']},
               '港口铁矿石库存': {'data': '港口铁矿石库存', 'name': '港口铁矿石库存', 'db_fields': ['date', 'port', 'quatity', 'product']}
               },
        '港口库存及调度': {'db_name': 'ODS_MTZYW_GKKC',
                    '环渤海四港货船比': {'data': '环渤海四港货船比', 'name': '环渤海四港货船比',
                                 'db_fields': ['date', 'Total_inventory', 'port_nm']},
                    '秦皇岛港港口库存及调度': {'data': '秦皇岛港港口库存及调度', 'name': '秦皇岛港港口库存及调度',
                                    'db_fields': ['date', 'Rail_arrivals', 'Port_unload', 'Rail_trans', 'Throughput',
                                                  'Ship_Anchorage', 'Ship_Arrivals', 'Total_inventory', 'port_nm']},
                    '广州港港口库存及调度': {'data': '广州港港口库存及调度', 'name': '广州港港口库存及调度',
                                   'db_fields': ['date', 'Rail_arrivals', 'Port_unload', 'Rail_trans',
                                                 'Throughput', 'Ship_Anchorage', 'Ship_Arrivals',
                                                 'Total_inventory', 'port_nm']},
                    '天津港港口库存及调度': {'data': '天津港港口库存及调度', 'name': '天津港港口库存及调度',
                                   'db_fields': ['date', 'Rail_arrivals', 'Port_unload', 'Rail_trans',
                                                 'Throughput', 'Ship_Anchorage', 'Ship_Arrivals',
                                                 'Total_inventory', 'port_nm']},
                    '曹妃甸二期港口库存及调度': {'data': '曹妃甸二期港口库存及调度', 'name': '曹妃甸二期港口库存及调度',
                                     'db_fields': ['date', 'Rail_trans', 'Shipment', 'Ship_Anchorage', 'Ship_Arrivals',
                                                   'Total_inventory', 'port_nm']},
                    '曹妃甸港港口库存及调度': {'data': '曹妃甸港港口库存及调度', 'name': '曹妃甸港港口库存及调度',
                                    'db_fields': ['date', 'Port_unload', 'Rail_trans', 'Throughput', 'Ship_Anchorage',
                                                  'Ship_Arrivals', 'Total_inventory', 'port_nm']},
                    '国投京唐港港口库存及调度': {'data': '国投京唐港港口库存及调度', 'name': '国投京唐港港口库存及调度',
                                     'db_fields': ['date', 'Rail_trans', 'Shipment', 'Ship_Anchorage', 'Ship_Arrivals',
                                                   'Total_inventory', 'port_nm']},
                    '京唐老港港口库存及调度': {'data': '京唐老港港口库存及调度', 'name': '京唐老港港口库存及调度',
                                    'db_fields': ['date', 'Rail_trans', 'Shipment', 'Ship_Anchorage', 'Ship_Arrivals',
                                                  'Total_inventory', 'port_nm']},
                    '黄骅港港口库存及调度': {'data': '黄骅港港口库存及调度', 'name': '黄骅港港口库存及调度',
                                   'db_fields': ['date', 'Total_inventory', 'Shipment', 'Fir_inventory',
                                                 'Sec_inventory', 'Tir_inventory', 'Rail_trans', 'Throughput',
                                                 'Ship_Anchorage', 'port_nm']}
                    },
        '港口快报': {'db_name': 'ODS_MTZYW_INVENTORY_KCKB',
                 '港口库存快报数(月)': {'data': '港口库存快报数(月)', 'name': '港口库存快报数(月)',
                                'db_fields': ['date', 'port', 'inventory', 'table_nm']},
                 '港口库存明细数(月)': {'data': '港口库存明细数(月)', 'name': '港口库存明细数(月)',
                                'db_fields': ['date', 'port', 'inventory', 'table_nm']}
                 },
        '六大电厂': {'db_name': 'ODS_MTZYW_LDDC_SUPPY_CONSU',
                 '沿海六大电厂(日)': {'data': '沿海六大电厂(日)', 'name': '沿海六大电厂(日)',
                               'db_fields': ['date', 'company_nm', 'Daily_consu', 'Inventory', 'Available_days',
                                             'type_station']}
                 },
        '供售热量': {'db_name': 'ODS_MTZYW_HEAT_POWER',
                 '全国供热量': {'data': '全国供热量', 'name': '全国供热量',
                               'db_fields': ['date', 'pro_nm', 'value_cum', 'growth','table_nm']},
                 '全国供热耗用原煤': {'data': '全国供热耗用原煤', 'name': '全国供热耗用原煤',
                           'db_fields': ['date', 'pro_nm', 'value_cum', 'growth','table_nm']},
                 '全国售电量': {'data': '全国售电量', 'name': '全国售电量',
                              'db_fields': ['date', 'pro_nm', 'value_cum', 'growth','table_nm']},
                 '全国供电量': {'data': '全国供电量', 'name': '全国供电量',
                           'db_fields': ['date', 'pro_nm', 'value_cum', 'growth','table_nm']}

                 },
        '港口运价': {'db_name': 'ODS_MTZYW_GJMJ',
                 '纽卡斯尔港': {'data': '纽卡斯尔港', 'name': '纽卡斯尔港',
                           'db_fields': ['date', 'port', 'product_nm', 'ash', 'heat',
                                         'price', 'price_type']},
                 '理查兹港': {'data': '理查兹港', 'name': '理查兹港',
                          'db_fields': ['date', 'port', 'product_nm', 'ash', 'heat',
                                        'price', 'price_type']},
                 '欧洲三港': {'data': '欧洲三港', 'name': '欧洲三港',
                          'db_fields': ['date', 'port', 'product_nm', 'ash', 'heat',
                                        'price', 'price_type']}

                 },
        '出口': {'db_name': 'ODS_MTZYW_EXPORT',
               '分煤种出口量': {'data': '分煤种出口量', 'name': '分煤种出口量',
                          'db_fields': ['date', 'coal_type', 'quatity_mon', 'quatity_YOY', 'quatity_MOM', 'quatity_all',
                                        'amount_mon', 'product']},
               '焦炭及半焦炭出口': {'data': '焦炭及半焦炭出口', 'name': '焦炭及半焦炭出口',
                            'db_fields': ['date', 'provice', 'amount_mon', 'quatity_all', 'amount_all', 'product']},
               '分海关出口量(焦炭及半焦炭)': {'data': '分海关出口量(焦炭及半焦炭)', 'name': '分海关出口量(焦炭及半焦炭)',
                                  'db_fields': ['date', 'customs', 'quatity_mon', 'quatity_all', 'amount_mon',
                                                'amount_all', 'product']},
               '钢材出口': {'data': '钢材出口', 'name': '钢材出口',
                        'db_fields': ['date', 'provice', 'quatity_mon', 'amount_mon', 'quatity_all', 'amount_all',
                                      'product']},
               '分国别(地区)出口量(焦炭及半焦炭)': {'data': '分国别(地区)出口量(焦炭及半焦炭)', 'name': '分国别(地区)出口量(焦炭及半焦炭)',
                                      'db_fields': ['date', 'region', 'quatity_mon', 'quatity_all', 'amount_mon',
                                                    'amount_all', 'product']}
               },
        '进口': {'db_name': 'ODS_MTZYW_IMPORT',
               '钢材进口': {'data': '钢材进口', 'name': '钢材进口',
                        'db_fields': ['date', 'provice', 'quatity_mon', 'amount_mon', 'quatity_all', 'amount_all',
                                      'product']},
               '铁矿石进口': {'data': '铁矿石进口', 'name': '铁矿石进口',
                         'db_fields': ['date', 'provice', 'quatity_mon', 'amount_mon', 'quatity_all', 'amount_all',
                                       'product']},
               '炼焦煤进口量': {'data': '炼焦煤进口量', 'name': '炼焦煤进口量',
                          'db_fields': ['date', 'coal_type', 'quatity_mon', 'quatity_YOY', 'quatity_MOM', 'quatity_all',
                                        'amount_mon', 'product']},
               '分国别(地区)进口量(炼焦煤)': {'data': '分国别(地区)进口量(炼焦煤)', 'name': '分国别(地区)进口量(炼焦煤)',
                                   'db_fields': ['date', 'region', 'quatity_mon', 'quatity_MOM', 'quatity_all',
                                                 'amount_mon', 'amount_all', 'product']},
               '分省份进口量(炼焦煤)': {'data': '分省份出口量(炼焦煤)', 'name': '分省份出口量(炼焦煤)',
                               'db_fields': ['date', 'provice', 'quatity_mon', 'quatity_MOM', 'quatity_all',
                                             'amount_mon', 'amount_all', 'product']},
               '分海关进口量(炼焦煤)': {'data': '分海关出口量(炼焦煤)', 'name': '分海关出口量(炼焦煤)',
                               'db_fields': ['date', 'customs', 'quatity_mon', 'quatity_all', 'amount_mon',
                                             'amount_all', 'product']}
               }
    }
    for name in list(all_dic.keys()):
        print(name)
        for i in range(0,1000,30):
            time.sleep(random.random())
            try:
                update(name, i)
            except:
                pass
    #print(str(int(float(time.time())*1000)))


        # try:
        #     print(name)
        #     update(name)
        #     time.sleep(random.randint(1, 4))
        # except Exception as e:
        #     print('爬取出现问题')
        #     print(e)
        #     time.sleep(random.randint(1, 4))
        #     #update(name)
        # print(name)
        # update(name)