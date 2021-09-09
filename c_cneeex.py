import requests
import json
import pymysql
headers={
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Content-Length':'19',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'Host': 'www.cneeex.com',
    'Origin':'https://www.cneeex.com',
    'Referer':'https://www.cneeex.com/cneeex/daytrade/detail?SiteID=122',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
'sec-ch-ua-mobile': '?0',
    'Sec-Fetch-Dest': 'empty',
'Sec-Fetch-Mode': 'cors',
'Sec-Fetch-Site': 'same-origin',
}

for i in range(2018,2022):
    db = pymysql.connect(host="10.110.80.83",
                         port=3306,
                         database="tianyancha",
                         user="root",
                         password="6lfBxZLyopc8Q@UF",
                         charset="utf8")
    conn = db.cursor()
    data = 'Date='+str(i)+'&Type=YEAR'
    r = requests.post('https://www.cneeex.com/cneeex/daytrade/selectData', headers=headers, data=data).content
    con = eval(r)
    print(con)
    for c in con:
        sql = "insert into carbon_cneex (date,product,vol,amount) values "+str(tuple(c))+";"
        print(sql)
        conn.execute(sql)
        db.commit()




