#先通过charles抓包ios中微信小红书程序客户端，得知headers如下所示，其中x-sign疑似自身js加密，charles没有截获js请求。故逆向微信小程序
#夜神模拟器中re浏览器获取小红书wxapkg.使用nodejs反编译。全局搜索x-sign，定位到DEFAULT_SIGN_HEADER变量，有一个encryptFeApiToken函数来定义，但该函数并未加密。根据fe-api-sign.js中的定义可知这是一个md5编码，直接套用。
#mysql 不支持emoji表情。实际项目中可以考虑别的存储方式。本次只做技术验证。
import time
import random
import requests
import urllib3
import pandas as pd
import pymysql
import json
from sqlalchemy import create_engine
from hashlib import md5

#根据爬取小红书
def fake_xsign(url):
    my_md5 = md5()
    my_md5.update((url[url.index('/fe_api'):] + 'WSUDD').encode(encoding='utf-8'))
    x_sign = 'X' + my_md5.hexdigest()
    print(x_sign)
    return x_sign

# fake_xsign('https://www.xiaohongshu.com/fe_api/burdock/weixin/v2/search/notes?keyword=%E5%A4%B1%E7%9C%A0&sortBy=create_time_desc&page=1&pageSize=20&prependNoteIds=&needGifCover=true')

header = {
#:method	GET
# 'scheme':'https',
# ':path':'/fe_api/burdock/weixin/v2/search/recommend?keyword=%E5%A4%B1%E7%9C%A0',
# ':authority':'www.xiaohongshu.com',
'accept':'*/*',
'content-type':'application/json',
'x-sign':'X24dc6d25a893a5aeaef19382f0a3c5ae',
#'x-sign':'Xed5949de4e47cb90e23948afa25dffea',
#'x-sign':'X8d4561678f8418e3e4e141efcde8096e',
#'x-sign':'Xaea55bb294c0b39970dd61867c9ba178',
#'x-sign':'Xeed09bdffe579ed9330785406152ffbd',
#'x-sign':'Xd137645bb83195a1022923a1b792485b',
'device-fingerprint':'WHJMrwNw1k/GII+DhtZK8NFwAkkxMTD9E9k5rXdwOze7aC+UfenE2EfOwfOD/6Gv5fMg5RvC8hP68LXtH+WnaS/uM2CAOCWqYdCW1tldyDzmauSxIJm5Txg==1487582755342',
'authorization':'wxmp.3acd1868-2f2a-48de-8d32-5cf00007f4c4',
'referer':'https://servicewechat.com/wxb296433268a1c654/56/page-frame.html',
'accept-language':'zh-cn',
'user-agent':'Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.12(0x18000c27) NetType/WIFI Language/zh_CN',
'accept-encoding':'gzip,compress,br,deflate'}
engine = create_engine("mysql+pymysql://root:123456@192.168.50.18:3306/stockinfo", echo=False)
db = pymysql.connect(host="192.168.50.18",
                         port=3306,
                         database="stockinfo",
                         user="root",
                         password="123456",
                         charset="utf8")
def is_chinese(uchar):
    if uchar >= '\u4e00' and uchar <= '\u9fa5':
        return True
    else:
        return False

def reserve_chinese(content):
    content_str = ''
    for i in content:
        if is_chinese(i):
            content_str += i
    return content_str

#获取列表
def get_list():
    for i in range(1, 20):
        print(i)
        time.sleep(random.randint(1, 3))
        url = 'https://www.xiaohongshu.com/fe_api/burdock/weixin/v2/search/notes?keyword=%E5%A4%B1%E7%9C%A0&sortBy=create_time_desc&page=' + str(
            i) + '&pageSize=20&prependNoteIds=&needGifCover=true'
        header.update({'x-sign': fake_xsign(url)})
        print(header)
        a = requests.get(url, headers=header, verify=False).content.decode()
        print(a)
        j = json.loads(a).get('data').get('notes')

        for detail in j:
            print(detail)
            detail.update({'cover': str(detail.get('cover'))})
            detail.update({'title': reserve_chinese(detail.get('title'))})
            detail.update({'userid': detail.get('user').get('id')})
            detail.update({'userimage': detail.get('user').get('image')})
            detail.update({'user': reserve_chinese(detail.get('user').get('nickname'))})
            df = pd.DataFrame.from_dict(data=detail, orient='index').T
            # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
            df.to_sql(name='shimian', con=engine, chunksize=1000, if_exists='append', index=None)

#获取评论
def get_comment():
    sql = 'SELECT DISTINCT id FROM `shimian`'
    conn = db.cursor()
    conn.execute(sql)
    all_id = conn.fetchall()
    for i in all_id:
        #频率反爬手段未触发，尚不确定触发条件
        time.sleep(random.randint(1, 3))
        print(i)
        #url = 'https://www.xiaohongshu.com/fe_api/burdock/weixin/v2/notes/'+i[0]+'/comments?pageSize=10&endId='+endId
        url = 'https://www.xiaohongshu.com/fe_api/burdock/weixin/v2/notes/' + i[
            0] + '/comments?pageSize=10'
        header.update({'x-sign': fake_xsign(url)})
        a = requests.get(url, headers=header, verify=False).content.decode()
        print(a)
        j = json.loads(a).get('data').get('comments')
        endId = ''
        for detail in j:
            endId = detail.get('id')
            detail.update({'subComments': reserve_chinese(str(detail.get('subComments')))})
            detail.update({'ats': reserve_chinese(str(detail.get('ats')))})
            detail.update({'ats': str(detail.get('ats'))})
            detail.update({'content': reserve_chinese(str(detail.get('content')))})
            detail.update({'user': reserve_chinese(str(detail.get('user')))})
            #detail.update({'nickname': reserve_chinese(str(detail.get('nickname')))})
            detail.update({'hashTags': str(detail.get('hashTags'))})
            df = pd.DataFrame.from_dict(data=detail, orient='index').T
            # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
            df.to_sql(name='shimian_comment', con=engine, chunksize=1000, if_exists='append', index=None)
        while len(j) != 0:
            time.sleep(random.randint(1, 3))
            url = 'https://www.xiaohongshu.com/fe_api/burdock/weixin/v2/notes/'+i[0]+'/comments?pageSize=10&endId='+endId
            header.update({'x-sign': fake_xsign(url)})
            a = requests.get(url, headers=header, verify=False).content.decode()
            print(a)
            j = json.loads(a).get('data').get('comments')
            endId = ''
            for detail in j:
                endId = detail.get('id')
                detail.update({'subComments': reserve_chinese(str(detail.get('subComments')))})
                detail.update({'ats': reserve_chinese(str(detail.get('ats')))})
                detail.update({'user': reserve_chinese(str(detail.get('user')))})
                detail.update({'content': reserve_chinese(str(detail.get('content')))})
                #detail.update({'nickname': reserve_chinese(str(detail.get('nickname')))})
                detail.update({'hashTags': str(detail.get('hashTags'))})
                df = pd.DataFrame.from_dict(data=detail, orient='index').T
                # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
                df.to_sql(name='shimian_comment', con=engine, chunksize=1000, if_exists='append', index=None)

if __name__ == '__main__':
    get_comment()

