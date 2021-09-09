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

#�ж��Ƿ����ֶ�
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
    # �Ƚ���gb2312����
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
    db_name = all_dic.get(key_name).get('db_name')   #��ȡ���ݿ���

    print(names)
    for nm in names:
        # query = "select column_name from all_tab_columns where Table_Name= '" + db_name + "'"
        # conn.execute(query)
        # fields_1 = conn.fetchall()
        # fields = []
        # for i in fields_1:
        #     fields.append(i[0])
        # fields = reversed(fields)
        # fields = tuple(fields)  # ��ȡ�ֶ�
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
        name = transform(all_dic.get(key_name).get(nm).get('name'))   #��ȡ����е�����
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
        'ú��ָ��': {'db_name': 'ODS_MTZYW_INDEX',
                 '�й�ú̿�۸�ָ��': {'data': '�й�ú̿�۸�ָ��', 'name': '�й�ú̿�۸�ָ��',
                           'db_fields': ['date', 'type', 'current', 'MOM_percent', 'MOM_point',
                                         'table_nm']},
                 '�й�ú̿�۸�ָ��(�紵ú)': {'data': '�й�ú̿�۸�ָ��(�紵ú)', 'name': '�й�ú̿�۸�ָ��(�紵ú)',
                          'db_fields': ['date', 'type', 'current', 'MOM_percent', 'MOM_point',
                                         'table_nm']},
                 '����������ú�۸�ָ��': {'data': '����������ú�۸�ָ��', 'name': '����������ú�۸�ָ��',
                                   'db_fields': ['date', 'type', 'current', 'MOM_percent', 'MOM_point',
                                                 'table_nm']}
                 },
        '����Ʒ�۸�': {'db_name': 'ods_mtzyw_chem_price',
                 '�״��۸�': {'data': '�״��۸�', 'name': '�״��۸�',
                              'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                            'remark','table_nm']},
                  '��ȩ�۸�': {'data': '��ȩ�۸�', 'name': '��ȩ�۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '����۸�': {'data': '����۸�', 'name': '����۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '�ֱ��۸�': {'data': '�ֱ��۸�', 'name': '�ֱ��۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '�����۸�': {'data': '�����۸�', 'name': '�����۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '���������۸�': {'data': '���������۸�', 'name': '���������۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '��ʯ�۸�': {'data': '��ʯ�۸�', 'name': '��ʯ�۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '����۸�': {'data': '����۸�', 'name': '����۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '�ռ�۸�': {'data': '�ռ�۸�', 'name': '�ռ�۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  '˳���۸�': {'data': '˳���۸�', 'name': '˳���۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                  'Һ���۸�': {'data': 'Һ���۸�', 'name': 'Һ���۸�',
                           'db_fields': ['date', 'agency', 'type', 'price', 'price_type',
                                         'remark', 'table_nm']},
                 },
        'ú̿����': {'db_name': 'ODS_MTZYW_PRODUCT_COAL',
                 '�ص�ú�����': {'data': '�ص�ú�����', 'name': '�ص�ú�����',
                            'db_fields': ['date', 'distinct', 'Degree_value', 'Degree_MoM', 'Degree_YoY', 'table_nm']},
                 '�����ص�ú��ԭú����': {'data': '�����ص�ú��ԭú����', 'name': '�����ص�ú��ԭú����',
                                'db_fields': ['date', 'Data_caliber', 'distinct', 'value_mon', 'YoY_growth',
                                              'Cum_value', 'Cum_change', 'Daily_pro', 'table_nm']},
                 '�ǹ���ú��ԭú����': {'data': '�ǹ���ú��ԭú����', 'name': '�ǹ���ú��ԭú����',
                               'db_fields': ['date', 'Cum_value', 'Cum_growth', 'value_mon', 'Daily_pro', 'table_nm']},
                 '����ú̿��ҵ����': {'data': '����ú̿��ҵ����', 'name': '����ú̿��ҵ����',
                              'db_fields': ['date', 'Coal_type', 'Cum_value', 'Cum_growth', 'Daily_pro', 'table_nm']},
                 'ȫ��ԭú����(ͳ�ƾ�)': {'data': 'ȫ��ԭú����(ͳ�ƾ�)', 'name': 'ȫ��ԭú����(ͳ�ƾ�)',
                                 'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'Cum_value', 'Cum_growth',
                                               'table_nm']},
                 'ԭú����': {'data': 'ԭú����', 'name': 'ԭú����',
                          'db_fields': ['date', 'Cum_value', 'Cum_growth', 'value_mon', 'Daily_pro', 'table_nm']},
                 '��Ʒú����': {'data': '��Ʒú����', 'name': '��Ʒú����',
                           'db_fields': ['date', 'Cum_value', 'Cum_growth', 'value_mon', 'Daily_pro', 'table_nm']},
                 'ȫ��ԭú����(����Э��)': {'data': 'ȫ��ԭú����(����Э��)', 'name': 'ȫ��ԭú����(����Э��)',
                                  'db_fields': ['date', 'value_mon', 'value_period', 'YOY_change', 'YoY_growth',
                                                'Cum_value', 'Cum_period', 'Cum_change', 'Cum_growth', 'table_nm']},
                 'ȫ��ԭú����(��ҵЭ��)': {'data': 'ȫ��ԭú����(��ҵЭ��)', 'name': 'ȫ��ԭú����(��ҵЭ��)',
                                  'db_fields': ['date', 'Cum_value', 'Cum_growth', 'Daily_pro', 'table_nm']},
                 'ȫ����Ʒú����': {'data': 'ȫ����Ʒú����', 'name': 'ȫ����Ʒú����',
                             'db_fields': ['date', 'Cum_value', 'Cum_growth', 'value_mon', 'Daily_pro', 'table_nm']},
                 '����ú̿��ҵϴ��ú����': {'data': '����ú̿��ҵϴ��ú����', 'name': '����ú̿��ҵ����',
                                 'db_fields': ['date', 'Coal_type', 'Cum_value', 'Cum_growth', 'table_nm']},
                 },
        '����': {'db_name': 'ODS_MTZYW_PRODUCT',
               '����ʯ': {'data': '����ʯ', 'name': '����ʯ',
                       'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               '��̿': {'data': '��̿', 'name': '��̿',
                      'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               '�ָ�': {'data': '�ָ�', 'name': '�ָ�',
                      'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               '����': {'data': '�ָ�', 'name': '�ָ�',
                      'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               'ˮ��': {'data': 'ˮ��', 'name': 'ˮ��',
                      'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum', 'table_nm']},
               'ƽ�岣��': {'data': 'ƽ�岣��', 'name': 'ƽ�岣��',
                        'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                      'table_nm']},
               '����������': {'data': '����������', 'name': '����������',
                         'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                       'table_nm']},
               '����������': {'data': '����������', 'name': '����������',
                         'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                       'table_nm']},
               '̫���ܷ�����': {'data': '̫���ܷ�����', 'name': '̫���ܷ�����',
                          'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                        'table_nm']},
               '���ܷ�����': {'data': '���ܷ�����', 'name': '���ܷ�����',
                         'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                       'table_nm']},
               'ȫ���ܷ�����': {'data': 'ȫ���ܷ�����', 'name': 'ȫ���ܷ�����',
                          'db_fields': ['date', 'distinct', 'value_mon', 'YoY_growth', 'value_cum', 'growth_cum',
                                        'table_nm']},
               },
        '�ڻ�': {'db_name': 'ODS_MTZYW_FUTURE_PRICE',
               '����ú�ڻ��۸�': {'data': '����ú�ڻ��۸�', 'name': '����ú�ڻ��۸�',
                           'db_fields': ['date', 'data_nm', 'price_type', 'price', 'MoM_increase', 'table_nm']},
               '����ʯ�ڻ��۸�': {'data': '����ʯ�ڻ��۸�', 'name': '����ʯ�ڻ��۸�',
                           'db_fields': ['date', 'data_nm', 'price_type', 'price', 'MoM_increase', 'table_nm']},
               '��̿�ڻ��۸�': {'data': '��̿�ڻ��۸�', 'name': '��̿�ڻ��۸�',
                          'db_fields': ['date', 'data_nm', 'price_type', 'price', 'MoM_increase', 'table_nm']},
               '���Ƹ��ڻ��۸�': {'data': '���Ƹ��ڻ��۸�', 'name': '���Ƹ��ڻ��۸�',
                           'db_fields': ['date', 'data_nm', 'price_type', 'price', 'MoM_increase', 'table_nm']},
               },
        '����': {'db_name': 'ODS_MTZYW_CONSUME',
               'ȫ����õ���': {'data': 'ȫ����õ���', 'name': 'ȫ����õ���',
                          'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']},
               '��һ��ҵ�õ���': {'data': '��һ��ҵ�õ���', 'name': '��һ��ҵ�õ���',
                           'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']},
               '�ڶ���ҵ�õ���': {'data': '�ڶ���ҵ�õ���', 'name': '�ڶ���ҵ�õ���',
                           'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']},
               '������ҵ�õ���': {'data': '������ҵ�õ���', 'name': '������ҵ�õ���',
                           'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']},
               '������������õ�': {'data': '������������õ�', 'name': '������������õ�',
                            'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum','table_nm']}
               },
        'ú̿����': {'db_name': 'ODS_MTZYW_CONSUME_COAL',
                 'ȫ����Ʒú������': {'data': 'ȫ����Ʒú������', 'name': 'ȫ����Ʒú������',
                              'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum',
                                            'grid', 'value_per', 'value_YoY', 'table_nm']},
                 'ȫ��������ú̿������': {'data': 'ȫ��������ú̿������', 'name': 'ȫ��������ú̿������',
                                'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum',
                                              'grid', 'value_per', 'value_YoY', 'table_nm']},
                 '�Ĵ���ҵ��Ʒú������': {'data': '�Ĵ���ҵ��Ʒú������', 'name': '�Ĵ���ҵ��Ʒú������',
                                'db_fields': ['date', 'Industry', 'value_mon', 'growth_YoY', 'value_cum', 'growth_cum',
                                              'grid', 'value_per', 'value_YoY', 'table_nm']},
                 },
        '����': {'db_name': 'ODS_MTZYW_SALES_COAL',
               'ȫ��ú̿����': {'data': 'ȫ��ú̿����', 'name': 'ȫ��ú̿����',
                          'db_fields': ['date', 'value_mon', 'change_YoY', 'growth_YoY', 'value_cum', 'changel_cum',
                                        'growth_cum', 'table_nm']},
               '�����ص�ú��������': {'data': '�����ص�ú��������', 'name': '�����ص�ú��������',
                             'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']},
               '�����ص�ú�󹩵�������': {'data': '�����ص�ú�󹩵�������', 'name': '�����ص�ú�󹩵�������',
                               'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']},
               '�����ص�ú��ұ������': {'data': '�����ص�ú��ұ������', 'name': '�����ص�ú��ұ������',
                               'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']}
               },
        '��̿�۸�': {'db_name': 'ODS_MTZYW_PRICE_COKE',
                 '���յ���ұ�𽹼۸�': {'data': '���յ���ұ�𽹼۸�', 'name': '���յ���ұ�𽹼۸�',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '���ϵ���ұ�𽹼۸�': {'data': '���ϵ���ұ�𽹼۸�', 'name': '���ϵ���ұ�𽹼۸�',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '���յ���ұ�𽹼۸�': {'data': '���յ���ұ�𽹼۸�', 'name': '���յ���ұ�𽹼۸�',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 'ɽ������ұ�𽹼۸�': {'data': 'ɽ������ұ�𽹼۸�', 'name': 'ɽ������ұ�𽹼۸�',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 'ɽ������ұ�𽹼۸�': {'data': 'ɽ������ұ�𽹼۸�', 'name': 'ɽ������ұ�𽹼۸�',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 '�ӱ�����ұ�𽹼۸�': {'data': '�ӱ�����ұ�𽹼۸�', 'name': '�ӱ�����ұ�𽹼۸�',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark',
                                             'Coke_type','table_nm']},
                 'ɽ���������콹�۸�': {'data': 'ɽ���������콹�۸�', 'name': 'ɽ���������콹�۸�',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'table_nm']},
                 '�ӱ��������콹�۸�': {'data': '�ӱ��������콹�۸�', 'name': '�ӱ��������콹�۸�',
                               'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'table_nm']},
                 },
        '��̿��ҵ�۸�': {'db_name': 'ODS_MTZYW_PRICE_COKE_BY_CO',
                   '����ɹ���': {'data': '����ɹ���', 'name': '����ɹ���',
                             'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark', 'Co_name',
                                           'table_name']},
                   '���󱨼�': {'data': '���󱨼�', 'name': '���󱨼�',
                            'db_fields': ['date', 'city', 'product_nm', 'price', 'price_type', 'Remark', 'Co_name',
                                          'table_name']}
                   },
        '�ֲļ۸�': {'db_name': 'ODS_MTZYW_PRICE_STEEL',
                 '���߼۸�': {'data': '���߼۸�', 'name': '���߼۸�',
                          'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                        'Steel_type']},
                 '�������Ƹּ۸�': {'data': '�������Ƹּ۸�', 'name': '�������Ƹּ۸�',
                             'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                           'Steel_type']},
                 '�Ⱦ��۸�': {'data': '�Ⱦ��۸�', 'name': '�Ⱦ��۸�',
                           'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                         'Steel_type']},
                 '���۸�': {'data': '���۸�', 'name': '���۸�',
                          'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                        'Steel_type']},
                 '���а�۸�': {'data': '���а�۸�', 'name': '���а�۸�',
                           'db_fields': ['date', 'city', 'specification', 'Production_unit', 'Price', 'Price_type',
                                         'Steel_type']},
                 },
        '����': {'db_name': 'ODS_MTZYW_CAPACITY',
               'ˮ�����ϲ���': {'data': 'ˮ�����ϲ���', 'name': 'ˮ�����ϲ���',
                          'db_fields': ['date', 'distinct', 'Co_name', 'Co_capacity', 'table_nm']},
               },
        # '�������ο���': {'db_name':'ODS_MTZYW_PRICE_HBH_5500K',
        #     'data': '�������ο���', 'name': '�������ο���'},
        'ú̿�۸�': {'db_name': 'ODS_MTZYW_PRICE_BY_ADDRESS',
                 '�ػʵ���ú̿�۸�': {'data': '�ػʵ�', 'name': '��ú̿�۸�',
                              'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type', 'address']},
                 '�������ú̿�۸�': {'data': '������', 'name': '��ú̿�۸�',
                              'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type', 'address']},
                 # '��Ͷ���Ƹ�ú̿�۸�': {'data': '��Ͷ����', 'name': '��ú̿�۸�',
                 #               'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                 #                             'Heat', 'price', 'Price_type', 'address']},
                 '�����ú̿�۸�': {'data': '����', 'name': '��ú̿�۸�',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '���Ƹ�ú̿�۸�': {'data': '����', 'name': '��ú̿�۸�',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '���ݸ�ú̿�۸�': {'data': '����', 'name': '��ú̿�۸�',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                           'Heat', 'price', 'Price_type', 'address']},
                 '��ɽ��ú̿�۸�': {'data': '��ɽ��', 'name': 'ú̿�۸�',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '��ʯ��ú̿�۸�': {'data': '��ʯ��', 'name': 'ú̿�۸�',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '������ú̿�۸�': {'data': '������', 'name': 'ú̿�۸�',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']},
                 '������ú̿�۸�': {'data': '������', 'name': 'ú̿�۸�',
                             'db_fields': ['date', 'product', 'Origin', 'Ash', 'Volatile', 'Sulfur', 'Adhesion', 'Heat',
                                           'price', 'Price_type', 'address']}
                 },
        'ú�󱨼�': {'db_name': 'ODS_MTZYW_PRICE_BY_CO',
                 '�񻪼���ú̿�ֻ��۸�': {'data': '�񻪼���ú̿�ֻ��۸�', 'name': '�񻪼���ú̿�ֻ��۸�',
                                'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                              'Heat', 'price', 'Price_type']},
                 '�񻪼���ú̿��Э�۸�': {'data': '�񻪼���ú̿��Э�۸�', 'name': '�񻪼���ú̿��Э�۸�',
                                'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                              'Heat', 'price', 'Price_type']},
                 '��ú����ú̿�۸�': {'data': '��ú����ú̿�۸�', 'name': '��ú����ú̿�۸�',
                              'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type']},
                 '������ҵú̿�۸�': {'data': '������ҵú̿�۸�', 'name': '������ҵú̿�۸�',
                              'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type']},
                 '���Ͽ�ҵú̿�۸�': {'data': '���Ͽ�ҵú̿�۸�', 'name': '���Ͽ�ҵú̿�۸�',
                              'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type']},
                 'ɽ����Դú̿�۸�': {'data': 'ɽ����Դú̿�۸�', 'name': 'ɽ����Դú̿�۸�',
                              'db_fields': ['date', 'origin', 'product', 'Ash', 'Volatile', 'Sulfur', 'Adhesion',
                                            'Heat', 'price', 'Price_type']}
                 },
        'ú̿����': {'db_name': 'ODS_MTZYW_TRANSPORT',
                 'ȫ����·���﷢����': {'data': 'ȫ����·���﷢����', 'name': 'ȫ����·���﷢����',
                               'db_fields': ['date', 'value_mon', 'value_per', 'change_YoY', 'growth_YoY', 'value_cum',
                                             'value_YoY', 'change_cum', 'growth_cum', 'table_nm']},
                 'ȫ����·ú̿������': {'data': 'ȫ����·ú̿������', 'name': 'ȫ����·ú̿������',
                               'db_fields': ['date', 'value_mon', 'value_per', 'change_YoY', 'growth_YoY', 'value_cum',
                                             'value_YoY', 'change_cum', 'growth_cum', 'table_nm']},
                 'ȫ����·��ú������': {'data': 'ȫ����·��ú������', 'name': 'ȫ����·��ú������',
                               'db_fields': ['date', 'value_mon', 'value_per', 'change_YoY', 'growth_YoY', 'value_cum',
                                             'value_YoY', 'change_cum', 'growth_cum', 'table_nm']},
                 '�����ص�ú������': {'data': '�����ص�ú������', 'name': '�����ص�ú������',
                               'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']},
                 '�����߷�����': {'data': '�����߷�����', 'name': '�����߷�����',
                            'db_fields': ['date', 'value_mon', 'value_per', 'change_YoY', 'growth_YoY', 'value_cum',
                                          'value_YoY', 'change_cum', 'growth_cum', 'table_nm']}
                 },
        'ú̿���': {'db_name': 'ODS_MTZYW_INVENTORY_COAL',
                 'ȫ��ú̿��ҵ���': {'data': 'ȫ��ú̿��ҵ���', 'name': 'ȫ��ú̿��ҵ���',
                              'db_fields': ['date', 'value_mon', 'change_YoY', 'table_nm']},
                 '�����ص�ú����': {'data': '�����ص�ú����', 'name': '�����ص�ú����',
                              'db_fields': ['date', 'distinct', 'value_mon', 'change_YoY', 'table_nm']},
                 'ȫ��������ú̿���': {'data': 'ȫ��������ú̿���', 'name': 'ȫ��������ú̿���',
                               'db_fields': ['date', 'grid', 'value_mon', 'value_per', 'change_YoY', 'table_nm']}
                 },
        '���': {'db_name': 'ODS_MTZYW_INVENTORY',
               '�ۿڽ�̿���': {'data': '�ۿڽ�̿���', 'name': '�ۿڽ�̿���', 'db_fields': ['date', 'port', 'quatity', 'product']},
               '�ֲ������': {'data': '�ֲ������', 'name': '�ֲ������', 'db_fields': ['date', 'product', 'quatity']},
               '�ۿ�����ʯ���': {'data': '�ۿ�����ʯ���', 'name': '�ۿ�����ʯ���', 'db_fields': ['date', 'port', 'quatity', 'product']}
               },
        '�ۿڿ�漰����': {'db_name': 'ODS_MTZYW_GKKC',
                    '�������ĸۻ�����': {'data': '�������ĸۻ�����', 'name': '�������ĸۻ�����',
                                 'db_fields': ['date', 'Total_inventory', 'port_nm']},
                    '�ػʵ��۸ۿڿ�漰����': {'data': '�ػʵ��۸ۿڿ�漰����', 'name': '�ػʵ��۸ۿڿ�漰����',
                                    'db_fields': ['date', 'Rail_arrivals', 'Port_unload', 'Rail_trans', 'Throughput',
                                                  'Ship_Anchorage', 'Ship_Arrivals', 'Total_inventory', 'port_nm']},
                    '���ݸ۸ۿڿ�漰����': {'data': '���ݸ۸ۿڿ�漰����', 'name': '���ݸ۸ۿڿ�漰����',
                                   'db_fields': ['date', 'Rail_arrivals', 'Port_unload', 'Rail_trans',
                                                 'Throughput', 'Ship_Anchorage', 'Ship_Arrivals',
                                                 'Total_inventory', 'port_nm']},
                    '���۸ۿڿ�漰����': {'data': '���۸ۿڿ�漰����', 'name': '���۸ۿڿ�漰����',
                                   'db_fields': ['date', 'Rail_arrivals', 'Port_unload', 'Rail_trans',
                                                 'Throughput', 'Ship_Anchorage', 'Ship_Arrivals',
                                                 'Total_inventory', 'port_nm']},
                    '��������ڸۿڿ�漰����': {'data': '��������ڸۿڿ�漰����', 'name': '��������ڸۿڿ�漰����',
                                     'db_fields': ['date', 'Rail_trans', 'Shipment', 'Ship_Anchorage', 'Ship_Arrivals',
                                                   'Total_inventory', 'port_nm']},
                    '������۸ۿڿ�漰����': {'data': '������۸ۿڿ�漰����', 'name': '������۸ۿڿ�漰����',
                                    'db_fields': ['date', 'Port_unload', 'Rail_trans', 'Throughput', 'Ship_Anchorage',
                                                  'Ship_Arrivals', 'Total_inventory', 'port_nm']},
                    '��Ͷ���Ƹ۸ۿڿ�漰����': {'data': '��Ͷ���Ƹ۸ۿڿ�漰����', 'name': '��Ͷ���Ƹ۸ۿڿ�漰����',
                                     'db_fields': ['date', 'Rail_trans', 'Shipment', 'Ship_Anchorage', 'Ship_Arrivals',
                                                   'Total_inventory', 'port_nm']},
                    '�����ϸ۸ۿڿ�漰����': {'data': '�����ϸ۸ۿڿ�漰����', 'name': '�����ϸ۸ۿڿ�漰����',
                                    'db_fields': ['date', 'Rail_trans', 'Shipment', 'Ship_Anchorage', 'Ship_Arrivals',
                                                  'Total_inventory', 'port_nm']},
                    '����۸ۿڿ�漰����': {'data': '����۸ۿڿ�漰����', 'name': '����۸ۿڿ�漰����',
                                   'db_fields': ['date', 'Total_inventory', 'Shipment', 'Fir_inventory',
                                                 'Sec_inventory', 'Tir_inventory', 'Rail_trans', 'Throughput',
                                                 'Ship_Anchorage', 'port_nm']}
                    },
        '�ۿڿ챨': {'db_name': 'ODS_MTZYW_INVENTORY_KCKB',
                 '�ۿڿ��챨��(��)': {'data': '�ۿڿ��챨��(��)', 'name': '�ۿڿ��챨��(��)',
                                'db_fields': ['date', 'port', 'inventory', 'table_nm']},
                 '�ۿڿ����ϸ��(��)': {'data': '�ۿڿ����ϸ��(��)', 'name': '�ۿڿ����ϸ��(��)',
                                'db_fields': ['date', 'port', 'inventory', 'table_nm']}
                 },
        '����糧': {'db_name': 'ODS_MTZYW_LDDC_SUPPY_CONSU',
                 '�غ�����糧(��)': {'data': '�غ�����糧(��)', 'name': '�غ�����糧(��)',
                               'db_fields': ['date', 'company_nm', 'Daily_consu', 'Inventory', 'Available_days',
                                             'type_station']}
                 },
        '��������': {'db_name': 'ODS_MTZYW_HEAT_POWER',
                 'ȫ��������': {'data': 'ȫ��������', 'name': 'ȫ��������',
                               'db_fields': ['date', 'pro_nm', 'value_cum', 'growth','table_nm']},
                 'ȫ�����Ⱥ���ԭú': {'data': 'ȫ�����Ⱥ���ԭú', 'name': 'ȫ�����Ⱥ���ԭú',
                           'db_fields': ['date', 'pro_nm', 'value_cum', 'growth','table_nm']},
                 'ȫ���۵���': {'data': 'ȫ���۵���', 'name': 'ȫ���۵���',
                              'db_fields': ['date', 'pro_nm', 'value_cum', 'growth','table_nm']},
                 'ȫ��������': {'data': 'ȫ��������', 'name': 'ȫ��������',
                           'db_fields': ['date', 'pro_nm', 'value_cum', 'growth','table_nm']}

                 },
        '�ۿ��˼�': {'db_name': 'ODS_MTZYW_GJMJ',
                 'Ŧ��˹����': {'data': 'Ŧ��˹����', 'name': 'Ŧ��˹����',
                           'db_fields': ['date', 'port', 'product_nm', 'ash', 'heat',
                                         'price', 'price_type']},
                 '����ȸ�': {'data': '����ȸ�', 'name': '����ȸ�',
                          'db_fields': ['date', 'port', 'product_nm', 'ash', 'heat',
                                        'price', 'price_type']},
                 'ŷ������': {'data': 'ŷ������', 'name': 'ŷ������',
                          'db_fields': ['date', 'port', 'product_nm', 'ash', 'heat',
                                        'price', 'price_type']}

                 },
        '����': {'db_name': 'ODS_MTZYW_EXPORT',
               '��ú�ֳ�����': {'data': '��ú�ֳ�����', 'name': '��ú�ֳ�����',
                          'db_fields': ['date', 'coal_type', 'quatity_mon', 'quatity_YOY', 'quatity_MOM', 'quatity_all',
                                        'amount_mon', 'product']},
               '��̿���뽹̿����': {'data': '��̿���뽹̿����', 'name': '��̿���뽹̿����',
                            'db_fields': ['date', 'provice', 'amount_mon', 'quatity_all', 'amount_all', 'product']},
               '�ֺ��س�����(��̿���뽹̿)': {'data': '�ֺ��س�����(��̿���뽹̿)', 'name': '�ֺ��س�����(��̿���뽹̿)',
                                  'db_fields': ['date', 'customs', 'quatity_mon', 'quatity_all', 'amount_mon',
                                                'amount_all', 'product']},
               '�ֲĳ���': {'data': '�ֲĳ���', 'name': '�ֲĳ���',
                        'db_fields': ['date', 'provice', 'quatity_mon', 'amount_mon', 'quatity_all', 'amount_all',
                                      'product']},
               '�ֹ���(����)������(��̿���뽹̿)': {'data': '�ֹ���(����)������(��̿���뽹̿)', 'name': '�ֹ���(����)������(��̿���뽹̿)',
                                      'db_fields': ['date', 'region', 'quatity_mon', 'quatity_all', 'amount_mon',
                                                    'amount_all', 'product']}
               },
        '����': {'db_name': 'ODS_MTZYW_IMPORT',
               '�ֲĽ���': {'data': '�ֲĽ���', 'name': '�ֲĽ���',
                        'db_fields': ['date', 'provice', 'quatity_mon', 'amount_mon', 'quatity_all', 'amount_all',
                                      'product']},
               '����ʯ����': {'data': '����ʯ����', 'name': '����ʯ����',
                         'db_fields': ['date', 'provice', 'quatity_mon', 'amount_mon', 'quatity_all', 'amount_all',
                                       'product']},
               '����ú������': {'data': '����ú������', 'name': '����ú������',
                          'db_fields': ['date', 'coal_type', 'quatity_mon', 'quatity_YOY', 'quatity_MOM', 'quatity_all',
                                        'amount_mon', 'product']},
               '�ֹ���(����)������(����ú)': {'data': '�ֹ���(����)������(����ú)', 'name': '�ֹ���(����)������(����ú)',
                                   'db_fields': ['date', 'region', 'quatity_mon', 'quatity_MOM', 'quatity_all',
                                                 'amount_mon', 'amount_all', 'product']},
               '��ʡ�ݽ�����(����ú)': {'data': '��ʡ�ݳ�����(����ú)', 'name': '��ʡ�ݳ�����(����ú)',
                               'db_fields': ['date', 'provice', 'quatity_mon', 'quatity_MOM', 'quatity_all',
                                             'amount_mon', 'amount_all', 'product']},
               '�ֺ��ؽ�����(����ú)': {'data': '�ֺ��س�����(����ú)', 'name': '�ֺ��س�����(����ú)',
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
        #     print('��ȡ��������')
        #     print(e)
        #     time.sleep(random.randint(1, 4))
        #     #update(name)
        # print(name)
        # update(name)