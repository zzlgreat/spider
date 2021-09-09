import requests
from bs4 import BeautifulSoup
import pymysql
db = pymysql.connect(host="10.110.80.83",
                         port=3306,
                         database="tianyancha",
                         user="root",
                         password="6lfBxZLyopc8Q@UF",
                         charset="utf8")
conn= db.cursor()
list = ['https://www.cneeex.com/xwzx/tzgg/index.shtml',
        'https://www.cneeex.com/xwzx/tzgg/index_2.shtml',
        'https://www.cneeex.com/xwzx/tzgg/index_3.shtml']
list2 = ['https://www.cneeex.com/xwzx/zcdt/index.shtml',
        'https://www.cneeex.com/xwzx/zcdt/index_2.shtml',
        'https://www.cneeex.com/xwzx/zcdt/index_3.shtml',
        'https://www.cneeex.com/xwzx/zcdt/index_4.shtml',
        'https://www.cneeex.com/xwzx/zcdt/index_5.shtml',]
for l in list2:
    print(l)
    r = requests.get(l).content.decode()
    soup = BeautifulSoup(r, 'lxml')
    table = soup.find_all('ul', class_="list-unstyled articel-list")[0]
    for n,i in enumerate(table.find_all('li')):
        if n%2==0:
            date = i.find('span').text
            href = 'https://www.cneeex.com/' + i.find('a')['href']
            title = i.find('a').text
            s_id = i.find('a')['href'].split('/')[-1].split('.')[0]
            c = [date, href, title, s_id]
            sql = "insert into carbon_cneeex_strategy (date,href,title,s_id) values " + str(tuple(c)) + ";"
            # print(sql)
            conn.execute(sql)
            db.commit()





