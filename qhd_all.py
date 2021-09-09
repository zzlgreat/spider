import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import datetime
headers={
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Content-Length':'143',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'SESSION=1c02e6b1-8744-4721-9e54-3058f4f762f6; JSESSIONID=oFxoBdEmKU9UI4UIK--WSFmR.undefined; __51cke__=; account=sdnyyx; __tins__19092507=%7B%22sid%22%3A%201629331933659%2C%20%22vd%22%3A%203%2C%20%22expires%22%3A%201629333791250%7D; __51laig__=3',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Host': 'www.cqcoal.com',
    'Origin':'http://www.cqcoal.com',
    'Referer':'http://www.cqcoal.com/dataport/ldgkjgqj.jsp?item=lm360&pg=3',
    'X-Requested-With': 'XMLHttpRequest'
}
today = datetime.date.today()
url_dic = {'hbh5500':{'url':'http://www.cqcoal.com/mars-web//fygmtjg/bspimtjghq',
                      'data':'fstrdate=2006-08-01&fenddate='+str(today)+'&sort=asc'},
           'ldgkprice':{'url':'http://www.cqcoal.com/mars-web//indexmark/lisldgk',
                        'data':'toDate='+str(today)+'&fromDate=2016-08-03&place=%E7%A7%A6%E7%9A%87%E5%B2%9B&coalPrice=&_search=false&nd=1629332069904&rows=-1&page=1&sidx=&sord=asc'},
           'hbhcc':{'url':'http://www.cqcoal.com/mars-web//ggscsb/getYardStore',
                      'data':'fromDate=2011-08-04&toDate='+str(today)+'&_search=false&nd=1629334129987&rows=10&page=1&sidx=fdate&sord=desc'},
           }
def get_detail(type):
    url = url_dic.get(type).get('url')
    #data = url_dic.get(type).get('data')
    for i in range(1,370):
        data= 'fromDate=2011-08-04&toDate='+str(today)+'&_search=false&nd=1629334129987&rows=10&page='+str(i)+'&sidx=fdate&sord=desc'
        print(str(today))
        engine = create_engine("mysql+pymysql://root:6lfBxZLyopc8Q@UF@10.110.80.83:3306/tianyancha", echo=False)
        r = requests.post(url, headers=headers, data=data).content.decode()
        print(r)
        a = json.loads(r).get('data').get('list')
        for x in a:
            print(x)
            df = pd.DataFrame(data=x, index=[0])
            print(df)
            try:
                df.to_sql(name='ods_qhd_' + type, con=engine, chunksize=1000, if_exists='append', index=None)
            except:
                pass
        print(r)

if __name__ == '__main__':
    get_detail('hbhcc')