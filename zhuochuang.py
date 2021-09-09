#coding=gbk
import requests
#from hyper.contrib import HTTP20Adapter
import time
import urllib3
import urllib
import pandas as pd
import json
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import VARCHAR,CHAR,DECIMAL,DATE,TEXT
urllib3.disable_warnings()
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
headers ={
'Host': 'index.sci99.com',
'Connection': 'keep-alive',
#'Content-Length': '149',
'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
'Accept': 'gzip, deflate, br',
'X-Requested-With': 'XMLHttpRequest',
'sec-ch-ua-mobile': '?0',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
'Content-Type': 'application/json',
'Origin': 'https://index.sci99.com',
'Sec-Fetch-Site': 'same-origin',
'Sec-Fetch-Mode': 'cors',
'Sec-Fetch-Dest': 'empty',
'Referer': 'https://index.sci99.com/channel/product/path3/%E7%A1%AB%E9%85%B8%E9%92%BE%E5%87%BA%E5%8E%82%E4%BB%B7%E6%A0%BC%E6%8C%87%E6%95%B0/1.html',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
'Cookie': 'UM_distinctid=17a1232d6c5443-0ff0f79104fdbe-f7f1939-e1000-17a1232d6c64b2; guid=26f20bd7-81ec-a30b-6758-88251fa4ba7c; BAIDU_SSP_lcr=https://www.baidu.com/link?url=bg_grwiNUnuwppIoeNxVjFvB9J0eHiXxj9LjhafOxEW&wd=&eqid=a231c2bd00284be60000000460c94448; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1623802960; Hm_lpvt_44c27e8e603ca3b625b6b1e9c35d712d=1623802960; isCloseOrderZHLayer=0; route=1c4ddf6e27e46b5b9d9da7e8bff51560; ASP.NET_SessionId=jit3e5bn2cgnev0svdkel14v; Hm_lvt_15007cbebbb86a0bd510a643b534d4fb=1623803207; href=https%3A%2F%2Findex.sci99.com%2Fchannel%2Fproduct.aspx%3Fpath3%3D%25E7%25A3%25B7%25E8%2582%25A5%25E5%2587%25BA%25E5%258E%2582%25E4%25BB%25B7%25E6%25A0%25BC%25E6%258C%2587%25E6%2595%25B0%26type%3D1; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; STATReferrerIndexId=1; pageViewNum=3; Hm_lpvt_15007cbebbb86a0bd510a643b534d4fb=1623807240'
}

headers1 = {
'Host': 'index.sci99.com',
'Connection': 'keep-alive',
'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="91", "Chromium";v="91"',
'Accept': '*/*',
'X-Requested-With': 'XMLHttpRequest',
'sec-ch-ua-mobile': '?0',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36',
'Sec-Fetch-Site': 'same-origin',
'Sec-Fetch-Mode': 'cors',
'Sec-Fetch-Dest': 'empty',
'Referer':'https://index.sci99.com/channel/product/path2/%E7%9F%B3%E6%B2%B9%E4%BB%B7%E6%A0%BC%E6%8C%87%E6%95%B0/1.html',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
'Cookie': 'UM_distinctid=17a1232d6c5443-0ff0f79104fdbe-f7f1939-e1000-17a1232d6c64b2; guid=26f20bd7-81ec-a30b-6758-88251fa4ba7c; BAIDU_SSP_lcr=https://www.baidu.com/link?url=bg_grwiNUnuwppIoeNxVjFvB9J0eHiXxj9LjhafOxEW&wd=&eqid=a231c2bd00284be60000000460c94448; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1623802960; Hm_lpvt_44c27e8e603ca3b625b6b1e9c35d712d=1623802960; isCloseOrderZHLayer=0; route=1c4ddf6e27e46b5b9d9da7e8bff51560; ASP.NET_SessionId=jit3e5bn2cgnev0svdkel14v; Hm_lvt_15007cbebbb86a0bd510a643b534d4fb=1623803207; href=https%3A%2F%2Findex.sci99.com%2Fchannel%2Fproduct.aspx%3Fpath3%3D%25E7%25A3%25B7%25E8%2582%25A5%25E5%2587%25BA%25E5%258E%2582%25E4%25BB%25B7%25E6%25A0%25BC%25E6%258C%2587%25E6%2595%25B0%26type%3D1; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; STATReferrerIndexId=1; qimo_seosource_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=%E7%AB%99%E5%86%85; qimo_seokeywords_b101a8c0-85cc-11ea-b67c-831fe7f7f53e=; Hm_lpvt_15007cbebbb86a0bd510a643b534d4fb=1623809310; pageViewNum=19'
}
def get_list():
    lists1 = json.loads(requests.get('https://index.sci99.com/api/nav/zh-cn/1', headers=headers1,verify=False).content.decode())
    headers1.update({ 'Content-Type':'application/x-www-form-urlencoded',
        'Referer':'https://index.sci99.com/channel/product/path3/%E5%9F%BA%E7%A1%80%E6%B2%B9%E4%BB%B7%E6%A0%BC%E6%8C%87%E6%95%B0/1.html'})
    list2=json.loads(requests.get('https://index.sci99.com/api/nav/zh-cn/2?_=1623810273723',headers = headers1,verify=False).content.decode())
    catagory = []
    for l in lists1:
        catagory.append(l.get('Name'))
    print(catagory)
    values =[]
    for l in list2:
        for c in catagory:
            if c in l.get('PID'):
                values.append({"path1": c+'价格指数', "path2": l.get('Name'),"hy": c})
    #print(values)
    values1 = []
    for l in list2:
        for v in values:
            if v.get('path2') == l.get('PID') and v.get('path1') != v.get('path2'):
                values1.append({"path1": v.get('path1'), "path2": v.get('path2'),"path3": l.get('Name'),"hy":v.get('hy')})
    #print(values1)
    values2 = []
    for l in list2:
        for v in values1:
            if v.get('path3') in l.get('PID') and v.get('path2') != v.get('path3'):
                #print(l,v)
                values2.append({"path1": v.get('path1'), "path2": v.get('path2'),"path3": v.get('path3'),"path4": l.get('Name'),"hy":v.get('hy')})
    #print(values2)
    values =values2+values1+values
    return values
def get_values(d):
    cookie = 'UM_distinctid=17a1232d6c5443-0ff0f79104fdbe-f7f1939-e1000-17a1232d6c64b2; guid=26f20bd7-81ec-a30b-6758-88251fa4ba7c; BAIDU_SSP_lcr=https://www.baidu.com/link?url=bg_grwiNUnuwppIoeNxVjFvB9J0eHiXxj9LjhafOxEW&wd=&eqid=a231c2bd00284be60000000460c94448; Hm_lvt_44c27e8e603ca3b625b6b1e9c35d712d=1623802960; Hm_lpvt_44c27e8e603ca3b625b6b1e9c35d712d=1623802960; isCloseOrderZHLayer=0; route=1c4ddf6e27e46b5b9d9da7e8bff51560; ASP.NET_SessionId=jit3e5bn2cgnev0svdkel14v; Hm_lvt_15007cbebbb86a0bd510a643b534d4fb=1623803207; href=https%3A%2F%2Findex.sci99.com%2Fchannel%2Fproduct.aspx%3Fpath3%3D%25E7%25A3%25B7%25E8%2582%25A5%25E5%2587%25BA%25E5%258E%2582%25E4%25BB%25B7%25E6%25A0%25BC%25E6%258C%2587%25E6%2595%25B0%26type%3D1; accessId=b101a8c0-85cc-11ea-b67c-831fe7f7f53e; STATReferrerIndexId=1; pageViewNum=3; Hm_lpvt_15007cbebbb86a0bd510a643b534d4fb=' + str(
        int(time.time()))
    # data = {"type": "3", "path1": "农资价格指数", "path2": "化肥出厂价格指数", "path3": "硫酸钾出厂价格指数", "path4": "", "level": 2,
    #         "hy": "农资"}

    data = {"path1": '', "path2": '',"path3": '',"path4": '',"hy":''}
    data.update(d)
    data.update({"type": "3", "level": 2})
    print(data)
    if data.get('path4') != '' and data.get('path3') != '':
        headers.update({'Referer': 'https://index.sci99.com/channel/product/' + 'path4/' + transform(
            data.get('path4')) + '/1.html'})
        data = json.dumps(data)
        print(data)
        headers.update({'Cookie': cookie})

        content = requests.post('https://index.sci99.com/api/zh-cn/dataitem/datavalue', headers=headers, data=data,
                                verify=False).content.decode()
        print(content)
        r = json.loads(content).get('List')
    else:
        r = None

    return r
if __name__ == '__main__':
    engine = create_engine("mysql+pymysql://root:6lfBxZLyopc8Q@UF@10.110.80.83:3306/tianyancha", echo=False)
    values = get_list()
    for i in values:
        time.sleep(0.5)
        print(i)
        details = get_values(i)
        print(details)
        if details is not None:
            for d in details:
                df = pd.DataFrame.from_dict(data=d, orient='index').T
                # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
                df.to_sql(name='zc_index', con=engine, chunksize=1000, if_exists='append', index=None)


