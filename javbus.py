#encoding=utf-8
from bs4 import BeautifulSoup
import requests
import json
import time
import random
import pymongo

def get_mag(fh):
    headers = {
        'authority': 'www.javbus.com',
        'method': 'GET',
        'path': '/ajax/uncledatoolsbyajax.php?gid=47208275556&lang=zh&img=/pics/cover/8dwy_b.jpg&uc=0&floor=851',
        'scheme': 'https',
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'cookie': 'PHPSESSID=mvfqu4pvlds94pnk354ro0o421; existmag=mag; starinfo=glyphicon%20glyphicon-minus',
        'referer': 'https://www.javbus.com',
        'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
        'sec-ch-ua-mobile': '?0',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest'
    }
    a = requests.get('https://www.javbus.com/'+fh, headers=headers).content.decode('utf-8')
    print(a.split('var gid = ')[1].split(';')[0])
    gid = a.split('var gid = ')[1].split(';')[0]
    print(a.split('var img = ')[1].split(';')[0])
    img = a.split('var img = ')[1].split(';')[0]
    headers.update({'path': '/ajax/uncledatoolsbyajax.php?gid=' + gid + '&lang=zh&img=' + img + '&uc=0&floor=851',
                    'referer': 'https://www.javbus.com/'+fh,})
    b = requests.get(
        'https://www.javbus.com/ajax/uncledatoolsbyajax.php?gid=' + gid + '&lang=zh&img=' + img + '&uc=0&floor=851',
        headers=headers).content.decode()
    soup = BeautifulSoup(b, 'html.parser')
    tr = soup.find_all('tr')
    res = []
    for t in tr:
        size = t.find_all('td')[1].text.split('\t')[1].split()[0]
        link = t.find_all('td')[1].find_all('a')[0]['href']
        print(size,link)
        x = lib.insert_one({'fh':fh,'size':size,'link':link})
        print(x)
    return res
if __name__ == '__main__':
    conn = pymongo.MongoClient(
        'mongodb://{}:{}@{}:{}/?authSource={}'.format("root", "zzl33818", "192.187.116.66", "26781", "admin"))
    db = conn.jav  # 直接写库名
    k = db.list_collection_names(include_system_collections=True)  # 返回当前库下所有的collection名
    lib = db['javbus']
    jav = db['jav321']
    agg = [{"$unwind": "$works"},
           {"$project": {"works": 1}}]
    mydoc = jav.aggregate(agg)
    ex_list = lib.distinct('fh')
    print(ex_list)
    for x in mydoc:
        if x.get('works').get('resource') == {}:
            if x.get('works').get('work_num') not in ex_list:
                print(x.get('works').get('work_num'))
                try:
                    get_mag(x.get('works').get('work_num'))
                except Exception as e:
                    print(e)




