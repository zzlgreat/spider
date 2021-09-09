
from selenium.webdriver.support.ui import  WebDriverWait
import pandas as pd
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
import requests
import random
import time
import cx_Oracle
from jsonpath import jsonpath #浠巎sonpath搴撲腑瀵煎叆jsonpath鏂规硶

url = 'https://www.sse.net.cn/index/singleIndex?indexType=cbcfi'

#涓浗娌挎捣鐓ょ偔杩愪环鎸囨暟
def get_cbcfi(date):
    db = cx_Oracle.connect('lcgs709999/Abcd1234@10.0.19.92:1521/snyx')
    # engine = create_engine('oracle+cx_oracle://lcgs709999:Abcd1234@10.0.19.92:1521/snyx')
    # db = cx_Oracle.connect('XBRL/123@localhost:1521/orcl.home.zzl.com')
    # db = cx_Oracle.connect('lcgs709999/Abcd1234@snyx')
    conn = db.cursor()
    option = webdriver.ChromeOptions()
    option.add_argument('鈥揹isable-dev-shm-usage')
    option.add_argument('--no-sandbox')
    option.add_argument('headless')
    prefs = {"profile.managed_default_content_settings.images": 1}
    option.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(chrome_options=option)
    driver.get(url)
    time.sleep(1)
    num = len(driver.find_elements_by_xpath('//*[@id="right"]/table/tbody/tr'))
    current = driver.find_element_by_xpath('//*[@id="right"]/table/tbody/tr[1]/td[4]').text[3:]
    print(current)
    get_date = '''
                                    select "update_time" from ODS_SSE_CBIFI
                                '''
    conn.execute(get_date)
    fullname = conn.fetchall()
    dates = []
    for i in fullname:
        dates.append(i[0])
    print(dates)
    for i in range(2,num+1):
        price=[]
        for x in range(1,6):
            type = driver.find_element_by_xpath(
                '//*[@id="right"]/table/tbody/tr['+str(i)+']/td['+str(x)+']').text
            price.append(type)
        price.append(current)
        if price[-1] in dates:
            pass
        else:
            price = tuple(price)
            print(price)
            insert_brief = '''insert into ODS_SSE_CBIFI("route","unit","last","current","change","update_time") values '''
            print(insert_brief + str(price))
            conn.execute(insert_brief + str(price))
            conn.execute('commit')
    return driver

def get_fdi(driver):
    db = cx_Oracle.connect('lcgs709999/Abcd1234@10.0.19.92:1521/snyx')
    # engine = create_engine('oracle+cx_oracle://lcgs709999:Abcd1234@10.0.19.92:1521/snyx')
    # db = cx_Oracle.connect('XBRL/123@localhost:1521/orcl.home.zzl.com')
    # db = cx_Oracle.connect('lcgs709999/Abcd1234@snyx')
    conn = db.cursor()
    url = 'https://www.sse.net.cn/index/singleIndex?indexType=fdi'
    driver.get(url)
    time.sleep(1)
    num = len(driver.find_elements_by_xpath('//*[@id="right"]/table/tbody/tr'))
    current = driver.find_element_by_xpath('//*[@id="right"]/table/tbody/tr[1]/td[6]').text[9:]
    print(current)
    get_date = '''
                                    select "update_time" from ODS_SSE_FDI
                                '''
    conn.execute(get_date)
    fullname = conn.fetchall()
    dates = []
    for i in fullname:
        dates.append(i[0])
    print(dates)
    for i in range(2,num+1):
        price=[]
        for x in range(1,8):
            type = driver.find_element_by_xpath(
                '//*[@id="right"]/table/tbody/tr['+str(i)+']/td['+str(x)+']').text
            price.append(type)
        price.append(current)
        if price[-1] in dates:
            pass
        else:
            price = tuple(price)
            print(price)
            insert_brief = '''insert into ODS_SSE_FDI("route","contact","load","ship","unit","index","change","update_time") values '''
            print(insert_brief + str(price))
            conn.execute(insert_brief + str(price))
            conn.execute('commit')

def get_cdfi(driver):
    db = cx_Oracle.connect('lcgs709999/Abcd1234@10.0.19.92:1521/snyx')
    # engine = create_engine('oracle+cx_oracle://lcgs709999:Abcd1234@10.0.19.92:1521/snyx')
    # db = cx_Oracle.connect('XBRL/123@localhost:1521/orcl.home.zzl.com')
    # db = cx_Oracle.connect('lcgs709999/Abcd1234@snyx')
    conn = db.cursor()
    url = 'https://www.sse.net.cn/index/singleIndex?indexType=cdfi'
    driver.get(url)
    time.sleep(1)
    num = len(driver.find_elements_by_xpath('//*[@id="right"]/table/tbody/tr'))
    current = driver.find_element_by_xpath('//*[@id="right"]/table/tbody/tr[1]/td[5]').text[6:]
    print(current)
    get_date = '''
                                    select "update_time" from ODS_SSE_CDFI
                                '''
    conn.execute(get_date)
    fullname = conn.fetchall()
    dates = []
    for i in fullname:
        dates.append(i[0])
    print(dates)
    for i in range(2,num+1):
        price=[]
        for x in range(1,7):
            type = driver.find_element_by_xpath(
                '//*[@id="right"]/table/tbody/tr['+str(i)+']/td['+str(x)+']').text
            price.append(type)
        price.append(current)
        if price[-1] in dates:
            pass
        else:
            price = tuple(price)
            print(price)
            insert_brief = '''insert into ODS_SSE_CDFI("route","load","ship","unit","index","change","update_time") values '''
            print(insert_brief + str(price))
            conn.execute(insert_brief + str(price))
            conn.execute('commit')
def base():
    db = cx_Oracle.connect('lcgs709999/Abcd1234@10.0.19.92:1521/snyx')
    # engine = create_engine('oracle+cx_oracle://lcgs709999:Abcd1234@10.0.19.92:1521/snyx')
    # db = cx_Oracle.connect('XBRL/123@localhost:1521/orcl.home.zzl.com')
    # db = cx_Oracle.connect('lcgs709999/Abcd1234@snyx')
    conn = db.cursor()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    driver = get_cbcfi(date)
    get_fdi(driver)
    get_cdfi(driver)
    conn.close()
    db.close()
    driver.quit()
if __name__ == '__main__':
    base()