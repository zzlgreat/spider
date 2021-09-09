import requests
import json
import pymysql
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
headers={
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Content-Length':'611',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'JSESSIONID=1F1F44B02E8896F00AE8DDF5422DF7E0; cookie_www=36802747; __jsluid_s=8fae39d44b5d9c9bec2455df4df52eca; Hm_lvt_3b83938a8721dadef0b185225769572a=1629262056; Hm_lpvt_3b83938a8721dadef0b185225769572a=1629262120',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Host': 'www.cqggzy.com',
    'Origin':'https://www.cqggzy.com',
    'Referer':'https://www.cqggzy.com/jyjg/005006/transaction_detail.html',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'sec-ch-ua-mobile': '?0',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}
headers2 = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Cookie': 'cookie_www=36802747; __jsluid_s=8fae39d44b5d9c9bec2455df4df52eca; Hm_lvt_3b83938a8721dadef0b185225769572a=1629262056; Hm_lpvt_3b83938a8721dadef0b185225769572a=1629266503',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Host': 'www.cqggzy.com',
    'If-Modified-Since': 'Tue, 17 Aug 2021 07:58:43 GMT',
    'If-None-Match': '"611b6c33-672a"',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
    'sec-ch-ua-mobile': '?0',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}
engine = create_engine("mysql+pymysql://root:6lfBxZLyopc8Q@UF@10.110.80.83:3306/tianyancha", echo=False)
def get_list():
    #for i in range(0, 1260, 20):
    for i in range(0, 200, 20):

        data = '{"token":"","pn":' + str(
            i) + r',"rn":20,"sdt":"","edt":"","wd":" ","inc_wd":"","exc_wd":"","fields":"title;projectno;","cnum":"001","sort":"{\"webdate\":\"0\"}","ssort":"title","cl":200,"terminal":"","condition":[{"fieldName":"categorynum","equal":"005006","notEqual":null,"equalList":null,"notEqualList":["014001018","004002005","014001015","014005014","014008011"],"isLike":true,"likeType":2}],"time":[{"fieldName":"webdate","startTime":"2021-08-17 00:00:00","endTime":"2021-09-01 23:59:59"}],"highlights":"title","statistics":null,"unionCondition":[],"accuracy":"","noParticiple":"0","searchRange":null,"isBusiness":"1"}'
        r = requests.post('https://www.cqggzy.com/interface/rest/inteligentSearch/getFullTextData', headers=headers,
                          data=data).content.decode()
        a = json.loads(r).get('result').get('records')
        for x in a:
            df = pd.DataFrame.from_dict(x)
            print(df)
            try:
                df.to_sql(name='carbon_cqggzy', con=engine, chunksize=1000, if_exists='append', index=None)
            except:
                pass
def get_detail():
    db = pymysql.connect(host="10.110.80.83",
                         port=3306,
                         database="tianyancha",
                         user="root",
                         password="6lfBxZLyopc8Q@UF",
                         charset="utf8")
    conn = db.cursor()
    sql = "SELECT DISTINCT webdate,id FROM `carbon_cqggzy` order by pubinwebdate desc"
    conn.execute(sql)
    detail = conn.fetchall()
    for d in detail:
        dates = str(d[0]).split(' ')[0].replace('-','')
        url = 'https://www.cqggzy.com/jyjg/005006/005006001/'+dates+'/'+d[1][:-13]+'.html'
        r = requests.get(url,headers = headers2).content.decode()
        try:
            soup = BeautifulSoup(r, 'lxml')
            table = soup.find_all('table')[0]
            df = pd.read_html(str(table), header=0)[0]
            print(df)
            df.columns = ['tdate', 'productnm', 'productid', 'avgprice', 'HIGHESTPRICE', 'LOWESTPRICE', 'change_rate','vol',
                          'amount', 'vol1', 'amount1']
            try:
                df.to_sql(name='carbon_cqggzy_detail', con=engine, chunksize=1000, if_exists='append', index=None)
            except:
                pass
        except:
            pass


if __name__ == '__main__':
    #get_list()
    get_detail()






