import requests
from bs4 import BeautifulSoup
import os
import time
import pandas as pd
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
header = {
'Accept': 'text/html, */*; q=0.01',
'Accept-Encoding': 'gzip, deflate, br',
'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
'Cache-Control': 'no-cache',
'Connection': 'keep-alive',
'Host': '18p.fun',
'devID':'',
'inbox': '0',
'Origin': 'http://www.gohaveababy.com',
'Pragma': 'no-cache',
'qubox': '0',
'Referer': 'http://www.gohaveababy.com/',
'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
'sec-ch-ua-mobile': '?0',
'Sec-Fetch-Dest': 'empty',
'Sec-Fetch-Mode': 'cors',
'Sec-Fetch-Site': 'cross-site',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'
}
innernum = 1
for num in range(116185,116643):
    innernum +=1
    path = r"C:\Users\chia1\Desktop\javlist"+'\尸兄 第'+str(innernum)+'话'
    os.makedirs(path)
    a = requests.get('https://18p.fun/ForInject/Chapter/?id='+str(num)+'&_=' + str(int(float(time.time()) * 1000)),
                     headers=header, verify=False).content.decode()
    print(a)
    soup = BeautifulSoup(a, 'lxml')
    d = soup.find_all('img')
    i = 0
    for dd in d:
        try:
            src = dd['data-src']
            print(src)
            img = requests.get(src).content
            # print(img)
            i += 1
            paths = os.path.join(path,str(i) + '.jpg')
            with open(paths, 'wb') as f:
                f.write(img)
            f.close()


        except Exception as e:
            print(e)




