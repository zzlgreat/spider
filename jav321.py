#encoding=utf-8
from bs4 import BeautifulSoup
import requests
import json
import time
import random
import pandas as pd
import wikipedia
import pymongo
wikipedia.set_lang("ja")
conn = pymongo.MongoClient('mongodb://{}:{}@{}:{}/?authSource={}'.format("root","zzl33818","192.187.116.66","26781","admin"))
db = conn.jav   # 直接写库名
k = db.list_collection_names(include_system_collections=True)  # 返回当前库下所有的collection名
lib = db['jav321']
print(k)
res = []
headers = {
'authority': 'www.jav321.com',
'method': 'GET',
'path': '/video/xvsr00600',
'scheme':'https',
'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
'accept-encoding': 'gzip, deflate, br',
'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
'cache-control': 'max-age=0',
'cookie': 'is_loyal=1; _ga=GA1.2.1045412192.1625479992; _gid=GA1.2.522503771.1625479992; _gat=1; __atuvc=20%7C27; __atuvs=60e44b1738249699005',
'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
'sec-ch-ua-mobile': '?0',
'sec-fetch-dest': 'document',
'sec-fetch-mode': 'navigate',
'sec-fetch-site': 'none',
'sec-fetch-user': '?1',
'upgrade-insecure-requests': '1',
'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
}
def getav():
    for t in range(6, 797):
        actress_url = 'http://www.jav321.com/stars/' + str(t)
        x = requests.get(actress_url).content.decode('UTF-8')
        soup = BeautifulSoup(x, 'html.parser')
        body = soup.find_all(class_='panel-body')[0]
        rows = body.find_all(class_='row')
        print(len(rows))
        for row in rows:
            singles = row.find_all(class_='thumbnail')
            print(len(singles))
            for single in singles:
                # 对每个女演员
                names = []
                for x in lib.find():
                    names.append(x.get('names'))
                name = single.text
                # if "?" in name or name in names:
                #     continue
                try:
                    ny = wikipedia.page(single.text)
                    summary = ny.content
                    # print(summary)
                    try:
                        Hepburn = summary.split('（')[1].split('、')[1].split('）')[0]
                    except:
                        Hepburn = ''
                    try:
                        born = summary.split('（')[1].split('、')[0]
                        # if len(born) > 16:
                        #     born = summary.split('born')[1].split(',')[0] + summary.split('born')[1].split(',')[1]
                    except:
                        born = ''
                except:
                    summary = ''
                    Hepburn = ''
                    born = ''
                href = single.find_all('a')[0]['href'].split('/')[2]
                work_url = 'http://www.jav321.com/star/' + href + '/'
                work_record = []
                print(name, Hepburn, born,work_url)
                for i in range(1, 1000):
                    #如果没有了
                    try:
                        works = requests.get(work_url + str(i)).content.decode()
                        soup = BeautifulSoup(works, 'html.parser')
                        body = soup.find_all(class_='panel-body')[0]
                        rows = body.find_all(class_='row')
                        for row in rows:
                            arts = row.find_all(class_='thumbnail')
                            for art in arts:
                                # 对每个作品
                                work_name = art.text  # 作品名称
                                work_num = art.text.split(' ')[-1]  # 番号
                                work_href = art.find_all('a')[0]['href']  # 链接
                                # print('http://www.jav321.com' + work_href)
                                headers.update({'path': art.find_all('a')[0]['href']})
                                try:
                                    c = requests.get('http://www.jav321.com' + work_href, headers=headers,
                                                     stream=True).content.decode()
                                    # print(c)
                                    soup = BeautifulSoup(c, 'html.parser')
                                    info = soup.find_all(class_='col-md-9')[0]
                                    # info = info.find_all(class_='row')[0]
                                    # print(info.text)
                                    # info = info.find_all('div')[1]
                                    actress = []
                                    company = []
                                    for links in info.find_all('a'):
                                        if 'star' in links['href']:
                                            actress.append([links.text, links['href']])
                                        if 'company' in links['href']:
                                            company= [links.text, links['href']]
                                    open_date = ''
                                    length = ''
                                    try:
                                        score = info.find_all('img')[0]['data-original'].split('/')[-1].split('.')[0]
                                    except:
                                        score = 0
                                    if '配信開始日' in info.text:
                                        open_date = info.text.split('配信開始日: ')[1][:10]
                                    if '収録時間: ' in info.text:
                                        length = info.text.split('収録時間: ')[1].split(' ')[0]
                                    try:
                                        soup = BeautifulSoup(c, 'html.parser')
                                        magnet = soup.find_all(class_='panel panel-info')[-2]
                                        # table = soup.find_all('table')[0]
                                        magnet = magnet.find_all('table')[0]
                                        link = magnet.find_all('a')
                                        hrefs = []
                                        for l in link:
                                            hrefs.append(l['href'])
                                        df = pd.read_html(str(magnet))[0]
                                        df['Magnet Link'] = hrefs
                                        #print(df)
                                        data = df.to_dict(orient='records')
                                        print(data)
                                    except Exception as e:
                                        data = {}
                                        #print(e)

                                    work_record.append({
                                        'work_name': work_name,
                                        'work_num': work_num,
                                        'work_href': work_href,
                                        'actress': actress,
                                        'company': company,
                                        'open_date': open_date,
                                        'length': length,
                                        'score': score,
                                        'resource': data
                                    })
                                except:
                                    pass
                    except Exception as e:
                        print(e)
                        break

                datas = {'names': name,
                                    'href': href,
                                    'Hepburn': Hepburn,
                                    'born': born,
                         'summary':summary,
                                    'works': work_record}
                #print(datas)
                x = lib.insert_one(datas)
                print(x)

if __name__ == '__main__':
    getav()




