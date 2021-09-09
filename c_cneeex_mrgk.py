import requests
from bs4 import BeautifulSoup
import pymysql
import re
import time
db = pymysql.connect(host="10.110.80.83",
                         port=3306,
                         database="tianyancha",
                         user="root",
                         password="6lfBxZLyopc8Q@UF",
                         charset="utf8")
conn= db.cursor()
list = ['https://www.cneeex.com/qgtpfqjy/mrgk/',
        'https://www.cneeex.com/qgtpfqjy/mrgk/index_2.shtml']

for l in list:
    print(l)
    r = requests.get(l).content.decode()
    soup = BeautifulSoup(r, 'lxml')
    table = soup.find_all('ul', class_="list-unstyled articel-list")[0]
    for n,i in enumerate(table.find_all('li')):
        time.sleep(1)
        if n%2==0:
            date = i.find('span').text
            href = 'https://www.cneeex.com/' + i.find('a')['href']
            title = i.find('a').text
            s_id = i.find('a')['href'].split('/')[-1].split('.')[0]
            r = requests.get(href).content.decode()
            soup = BeautifulSoup(r, 'lxml')
            table = soup.find_all('div', class_="article-con font16")[0]
            s1 = table.find_all('span')[0]
            s2 = table.find_all('span')[0]
            content = s1.text+s2.text
            print(re.findall(r"\d+\.?\d*"+r"\,?\d*"+r"\.?\d*", content))
            lists = re.findall(r"\d+\.?\d*"+r"\,?\d*"+r"\,?\d*"+r"\.?\d*", content)

            lists.extend([date, href, title, s_id,content])
            # c = [date, href, title, s_id,content]
            # sql = ""
            print(len(lists))
            if len(lists) ==23:
                sql = "insert into carbon_cneeex_detail (" \
                      "vol,amount,openprice,highestprice,lowestprice,closeprice,change_rate," \
                      "CEAVOL,CEAAMOUNT,CEAVOL1,CEAAMOUNT1,CEAOPENPRICE,CEAHIGHIGHESTPRICE," \
                      "CEALOWESTPRICE,CEACLOSEPRICE,CEACHANGE,CEAALLVOL,CEAALLAMOUNT,date,href,title,s_id,content) values " + str(
                    tuple(lists)) + ";"
            elif len(lists) ==27:
                sql = "insert into carbon_cneeex_detail (" \
                      "vol,amount,openprice,highestprice,lowestprice,closeprice,change_rate," \
                      "dzjyvol,dzjyamount,CEAVOL,CEAAMOUNT,CEAVOL1,CEAAMOUNT1,CEAOPENPRICE,CEAHIGHIGHESTPRICE," \
                      "CEALOWESTPRICE,CEACLOSEPRICE,CEACHANGE,CEADZJYVOL,CEADZJYAMOUNT,CEAALLVOL,CEAALLAMOUNT,date,href,title,s_id,content) values " + str(
                    tuple(lists)) + ";"


            print(sql)
            conn.execute(sql)
            db.commit()





