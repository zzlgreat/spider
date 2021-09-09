# encoding=UTF-8
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from sqlalchemy import create_engine
import json
from elasticsearch import Elasticsearch
import selenium
import tyc_login
import random
headers={
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Connection': 'keep-alive',
    'Cookie': 'csrfToken=JmgOZkzL01qCcguys0lzvgjI; aliyungf_tc=AQAAAMBE1XUQnQkAGTAIcKIHWLklj5ax; TYCID=851ec3b04e4411eab466753f1cb0a776; undefined=851ec3b04e4411eab466753f1cb0a776; ssuid=9227192054; _ga=GA1.2.2043644185.1581586682; bad_id658cce70-d9dc-11e9-96c6-833900356dc6=28428331-6888-11ea-99bd-27c960eca83c; nice_id658cce70-d9dc-11e9-96c6-833900356dc6=28428332-6888-11ea-99bd-27c960eca83c; bannerFlag=true; show_activity_modal=20200517; openChat658cce70-d9dc-11e9-96c6-833900356dc6=true; cid=396778332; ss_cidf=1; tyc-user-phone=%255B%252218610023038%2522%252C%2522132%25205658%25200109%2522%252C%2522185%25200074%25203107%2522%255D; refresh_page=0; _gid=GA1.2.606128846.1600652671; RTYCID=03e0bc82e5bd405e9e43c1d65fb8695a; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1600652709; CT_TYCID=45092a20df174bdfb446bb914dc2b1ae; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1600656860; cloud_token=dfecd8cd4f974a94991d290ecf92b967; tyc-user-info={%22claimEditPoint%22:%220%22%2C%22explainPoint%22:%220%22%2C%22vipToMonth%22:%22false%22%2C%22personalClaimType%22:%22none%22%2C%22integrity%22:%2210%25%22%2C%22state%22:%224%22%2C%22score%22:%22649%22%2C%22surday%22:%22253%22%2C%22announcementPoint%22:%220%22%2C%22messageShowRedPoint%22:%220%22%2C%22bidSubscribe%22:%22-1%22%2C%22vipManager%22:%220%22%2C%22monitorUnreadCount%22:%220%22%2C%22discussCommendCount%22:%220%22%2C%22onum%22:%2210%22%2C%22showPost%22:null%2C%22messageBubbleCount%22:%220%22%2C%22claimPoint%22:%220%22%2C%22token%22:%22eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxODYxMDAyMzAzOCIsImlhdCI6MTYwMDY1Njk2MSwiZXhwIjoxNjMyMTkyOTYxfQ.5OrK5WUOaasuZ_m-O4RAgc7xMc0KZf7xqJ1jiYPCgsvN7FX4OD5RvOlvwRa9qi9y2TTV1e3Onl7b7D91sldXkQ%22%2C%22schoolAuthStatus%22:%222%22%2C%22userId%22:%22209016092%22%2C%22vipToTime%22:%221622476799999%22%2C%22scoreUnit%22:%22%22%2C%22redPoint%22:%220%22%2C%22myTidings%22:%220%22%2C%22companyAuthStatus%22:%222%22%2C%22originalScore%22:%22649%22%2C%22myAnswerCount%22:%220%22%2C%22myQuestionCount%22:%220%22%2C%22signUp%22:%220%22%2C%22privateMessagePointWeb%22:%220%22%2C%22nickname%22:%22%E5%A4%A7%E5%8D%AB%C2%B7%E6%B4%A5%E6%9B%BC%22%2C%22privateMessagePoint%22:%220%22%2C%22bossStatus%22:%222%22%2C%22isClaim%22:%220%22%2C%22yellowDiamondEndTime%22:%220%22%2C%22isExpired%22:%220%22%2C%22yellowDiamondStatus%22:%22-1%22%2C%22pleaseAnswerCount%22:%220%22%2C%22bizCardUnread%22:%220%22%2C%22vnum%22:%220%22%2C%22mobile%22:%2218610023038%22}; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxODYxMDAyMzAzOCIsImlhdCI6MTYwMDY1Njk2MSwiZXhwIjoxNjMyMTkyOTYxfQ.5OrK5WUOaasuZ_m-O4RAgc7xMc0KZf7xqJ1jiYPCgsvN7FX4OD5RvOlvwRa9qi9y2TTV1e3Onl7b7D91sldXkQ; token=c2adcf7180354d6bbea4e938f1b3cf25; _utm=06b909ccbed644f8b6a31868555aa693',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
    'Host': 'www.tianyancha.com',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Referer':'https://www.tianyancha.com/company/396778332',
    'X-Requested-With': 'XMLHttpRequest',
    #'X-AUTH-TOKEN': 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxODUwMDc0MzEwNyIsImlhdCI6MTU5ODA0ODI1MCwiZXhwIjoxNjI5NTg0MjUwfQ.AJHa7bHfTkToeSkAuzG_uIl1TT1_QfupGekjYkpFbsCFqMjl-9nVh2yHtY3f1YhSF4CwPwVSXNZD8tj10PfGKg'
}
headers1={
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Connection': 'keep-alive',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
    'Host': 'capi.tianyancha.com',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Origin': 'https://www.tianyancha.com',
    'Referer':'https://www.tianyancha.com/company/396778332',
    'version': 'TYC-Web',
    'X-AUTH-TOKEN': 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxODYxMDAyMzAzOCIsImlhdCI6MTU5OTA5NjgyNiwiZXhwIjoxNjMwNjMyODI2fQ.dYO15BcOS_mKMuzKD2WolSId_3ZP3lTN3QRkSGFitvKCTWD3DjZa56k8zr8gQT4TWElGlyg8QUTNjN0Wu9S7AA'
}
all_dic = {
           #建筑资质-注册人员
           'constructHuman':[2,'no', 'name', 'reg_type', 'reg_num', 'reg_sub', 'detail'],
           #建筑资质-资质资格
           'constructQualification':[1,'no', 'q_date','q_period','q_type', 'q_no', 'q_name', 'q_department', 'detail'],
           'certificate':[1,'no', 'c_date', 'c_type', 'c_num', 'c_period', 'detail'],
           #行政处罚【工商局】序号	处罚日期	决定文书号	处罚事由/违法行为类型	处罚结果/内容	处罚单位	数据来源	操作
           'punish':[1,'no', 'p_date','p_num','p_type','p_content', 'p_department','source','detail'],
           #被执行人
           'zhixing':[1,'no', 'z_date','z_target','z_caseno', 'z_department', 'detail'],
           #开庭公告
           'announcementcourt':[1,'no', 'a_date','a_caseno','a_case', 'a_defendant','court', 'detail'],
           #法律诉讼
           'lawsuit':[1,'no', 'l_date','l_name','l_caseno','l_case', 'l_person', 'l_result','court','res_date', 'detail'],
           #主要人员
           'staff':[2,'no', 'name','staff','ratio', 'f_ratio'],
           #法院公告
           'court':[1,'no', 'l_date','case_no','case_reason','respondent', 'ann_type', 'court', 'detail'],
           #送达公告
           'sendAnnouncements':[1,'no', 's_date','ann_name','court','detail'],
           #立案信息
           'courtRegister':[1,'no', 'o_date','case_id','plaintiff','court','detail'],
           #股东信息
           'holder':[2,'no','holder','ratio','quatity','fin_share','h_date'],
           #对外投资
           'invest':[3,'no','invesed_co','legal_person','found_date','quatity','ratio','attr','product','agency'],
            #实际控制权
           'companyholding':[2,'no','invesed_co','invested_ratio','invested_link'],
            #变更记录
           'changeinfo':[1,'no','change_time','change_project','before','after'],
            #分支机构
           'branch':[3,'no', 'b_name','b_human','b_date', 'b_attr'],
            #股东及出资信息——序号	股东（发起人）	持股比例	认缴出资额（万元）	认缴出资日期	实缴出资额（万元）	实缴出资日期
            'holderList':[6,'no', 'holder','hold_ratio','sub_capital', 'sub_time','actual_capital','act_time','detail'],
            #股权变更信息——序号	股东（发起人）	变更前股权比例	变更后股权比例	股权 变更日期	公示日期
            'stockChangeInfo':[1,'no', 'holder','before_ratio','after_ratio','change_date', 'ann_date'],
            #融资历程——序号	披露日期 	交易金额	 融资轮次	估值	比例
            'rongzi':[1,'no', 'r_time','invest','round', 'value','ratio','investor','source'],
            #核心团队
            'teamMember':[2,'no', 'name','job','brief'],
            #投资事件——序号	投资日期	参与轮次	投资方	投资金额	产品名称 产品标签	所属地	简介
            'touzi':[2,'no', 'i_date','round','investor','investment','product','tag','area','brief'],
            #竞品信息
            'jingpin':[3,'no', 'p_name','round','value','found_date','tag','area','brief','belong'],
    #行政许可
    'licensing':[1,'no','from_time','to_time', 'f_no','f_name','agency','content','source','detail'],
    'licensingXyzg':[1,'no', 'f_no','agency','determine_time','detail'],
    #税务评级
    'taxcredit':[1,'no', 'year','credit_level','type','credit','agency'],
    #信用评级
    'creditRating':[2,'no', 'year','credit_level','type','credit','agency','report'],
    #抽查检查
    'check':[1,'no', 'c_date','c_type','c_result','agency'],
     #招投标
    'bid':[1,'no', 'b_date','title','buyer'],
     #微信公众号
    'wechat':[2,'no', 'wechat_nm','wechat','qcode','breif'],
    #公告研报
    'relatedAnnouncement':[1,'no', 'r_date','r_type','r_content'],
    #地块公示
    'landPublicitys':[1,'no', 'l_date','loc','distinct','area','usage','agency','deatail'],
    #进出口信用
    'importAndExport':[1,'no', 'customs','industry','type','reg_date','detail'],
    #债券信息
    'bond':[1,'no', 'b_date','b_name','b_code','b_type','level','l_date','detail'],
    #购地信息
    'purchaselandV2':[1,'no', 'p_loc','p_usage','p_area','distinct','p_way','p_date','detail'],
    #供应商
    'supplies':[1,'no', 'supplier','ratio','quatity','dates','source','relationship'],
    #供应商
    'clients':[2,'no', 'client','ratio','quatity','dates','source','relationship'],
    #商标
    'tmInfo':[1,'no', 't_date','tm','t_name','reg_no','category','state','detail'],
    #专利信息序号	申请日	专利名称	专利类型	法律状态	申请号	公开（公布）号	公开（公告）日	操作
    'patent':[1,'no', 'p_date','patent_nm','p_category','legal_state','apply_no','patent_no','p_publish_date','detail'],
    #作品著作权
    'copyrightWorks':[1,'no', 'c_nm','c_code','c_type','complete_date','c_date','first_date'],
    #网站备案
    'icp':[1,'no', 'i_date','website','homepage','url','beian']
           }
def get_cookie():
    # 关键步骤 1：下面两行代码是用来设置特性，获取request的信息前提步骤。
    tyc = tyc_login.Tyc()
    driver = tyc.entrace('15154102703', 'zzl33818')
    driver.get('https://www.tianyancha.com/pagination/constructProject.xhtml?ps=10&pn=2&id=396778332&name=%E6%B1%9F%E8%8B%8F%E4%B8%AD%E5%8D%97%E5%BB%BA%E7%AD%91%E4%BA%A7%E4%B8%9A%E9%9B%86%E5%9B%A2%E6%9C%89%E9%99%90%E8%B4%A3%E4%BB%BB%E5%85%AC%E5%8F%B8')
    time.sleep(3)
    cookie = driver.get_cookies()
    jsonCookies = json.dumps(cookie)
    with open('tyc.json', 'w') as f:
        f.write(jsonCookies)
    with open('tyc.json', 'r', encoding='utf-8') as f:
        listCookies = json.loads(f.read())
    cookie = [item["name"] + "=" + item["value"] for item in listCookies]
    cookiestr = '; '.join(item for item in cookie)
    driver.quit()
    return cookiestr

def get_baseInfo(section,id,name,ps,pn):
    #cookies = get_cookie()
    #
    with open('tyc.json', 'r', encoding='utf-8') as f:
        listCookies = json.loads(f.read())
    cookie = [item["name"] + "=" + item["value"] for item in listCookies]
    cookies = '; '.join(item for item in cookie)
    headers.update({'Cookie': cookies})
    try:
        table = requests.get(
            'https://www.tianyancha.com/pagination/' + section + '.xhtml?ps=' + ps + '&pn=' + pn + '&id=' + id + '&name=' + name + '&_=1598700180970',
            headers=headers).content.decode('utf-8')
        #print('https://www.tianyancha.com/pagination/' + section + '.xhtml?ps=' + ps + '&pn=' + pn + '&id=' + id + '&name=' + name + '&_=1598700180970')
    except:
        print('等待4s')
        time.sleep(4)
        table = requests.get(
            'https://www.tianyancha.com/pagination/' + section + '.xhtml?ps=' + ps + '&pn=' + pn + '&id=' + id + '&name=' + name + '&_=1598700180970',
            headers=headers).content.decode('utf-8')
    #print(table)
    if table =='Unauthorized':
        time.sleep(10)
        with open('tyc.json', 'r', encoding='utf-8') as f:
            listCookies = json.loads(f.read())
        cookie = [item["name"] + "=" + item["value"] for item in listCookies]
        cookies = '; '.join(item for item in cookie)
        headers.update({'Cookie': cookies})
        # cookies = get_cookie()
        # headers.update({'Cookie': cookies})
        url = 'https://www.tianyancha.com/pagination/' + section + '.xhtml?ps=' + ps + '&pn=' + pn + '&id=' + id
        table = requests.get(url,headers=headers).content.decode('utf-8')
    soup = BeautifulSoup(table, 'lxml')
    tables = soup.find_all("span", attrs={"class": "link-click"}, recursive=True)
    if len(tables)==0:
        tables = soup.find_all("a", attrs={"class": "link-click"}, recursive=True)
    p = pd.DataFrame()
    try:
        pa = pd.read_html(str(table), header=0)[0]
        print(pa)
        pa.columns = all_dic.get(section)[1:]
        for x, row in pa.iterrows():
            if x % all_dic.get(section)[0] == 0:
                if len(tables)>0:
                    if tables[int(x / all_dic.get(section)[0])].get('data-businessid') != None:
                        #print(tables[int(x / all_dic.get(section)[0])])
                        row['detail'] = tables[int(x / all_dic.get(section)[0])]['data-businessid']
                    elif tables[int(x / all_dic.get(section)[0])].get('onclick') != None:
                        #print(tables[int(x / all_dic.get(section)[0])]['onclick'])
                        row['detail'] = tables[int(x / all_dic.get(section)[0])]['onclick'].split('"')[1]
                    elif tables[int(x / all_dic.get(section)[0])].get('href') != None:
                        #print(tables[int(x / all_dic.get(section)[0])]['onclick'])
                        row['detail'] = tables[int(x / all_dic.get(section)[0])]['href'].split('/')[-1]
                    else:
                        pass
                row['key_id'] = id
                p = p.append(row)
                # row['name'] = row['name'][1:]
                # print(row['name'])

        return p
    except Exception as e:
        print(section)
        #print(e)
        return p

def getDetail(section,id,innerid):
    d_type = {
        'announcementcourt':['https://www.tianyancha.com/judicialcase/related.json?detailId='+innerid+'&type=court_notices&gid='+id+'&_=1607310282303','json'],
        'bid':['https://www.tianyancha.com/bid/'+innerid,'html',],
        'constructhuman':['https://capi.tianyancha.com/cloud-newdim/construct/getRegHumanDetail.json?businessId='+innerid+'&pageSize=20&pageNum=1&_=1607310753577','json'],
        'constructqualification':['https://capi.tianyancha.com/cloud-newdim/construct/getQualificationDetail.json?businessId='+innerid+'&_=1607310753578','json'],
        'courtRegister':['https://www.tianyancha.com/judicialcase/related.json?detailId='+innerid+'&type=court_register&gid='+id+'&_=1607310753580','json'],
        'landpublicitys':['https://www.tianyancha.com/company/getLandPublicityDetailV3.json?businessId='+innerid+'&_=1607310282479','json'],
        'lawsuit':['https://susong.tianyancha.com/'+innerid,'html'],
        'punish':['https://capi.tianyancha.com/cloud-operating-risk/operating/punishment/getPunishmentInfoDetail?businessId='+innerid+'&_=1607310753581','json'],
        'purchaselandv2':['https://www.tianyancha.com/company/getPurchaseLandV2Detail.json?businessId=7zvv4944597e227d6de29212dlf9v2e7&_=1607310753584','json']
    }
    with open('tyc.json', 'r', encoding='utf-8') as f:
        listCookies = json.loads(f.read())
    cookie = [item["name"] + "=" + item["value"] for item in listCookies]
    cookies = '; '.join(item for item in cookie)
    headers.update({'Cookie': cookies})
    if d_type.get(section)==None:
        try:
            content = requests.get(d_type.get(section)[0], headers=headers).content.decode('utf-8')
        except:
            time.sleep(1)
            content = requests.get(d_type.get(section)[0], headers=headers).content.decode('utf-8')
        print(content)

    return content

def get_es(city):

    begin = random.randint(10, 1000)
    response = es.search(
        index="enterprisebaseinfo",  # 索引名
        body={  # 请求体
            "from": begin,'size':1,
            "query": {  # 关键字，把查询语句给 query
                "bool": {  # 关键字，表示使用 filter 查询，没有匹配度
                # "filter": {
                #     "script": {
                #         "script": {
                #             "source": "doc['reg_cap.keyword'][0].length()>11",
                #             "lang": "painless"
                #         }
                #     }
                # },
                    'must':
                    [
                        {
                            "match": {  # 关键字，表示需要匹配的元素
                                "prov.keyword": city
                            }
                        },
                        {
                    "exists": {  # 关键字，表示需要匹配的元素
                        "field": "website"
                    }}
                    ],
                    "should": [  # 表示里面的条件必须匹配，多个匹配元素可以放在列表里
                        {
                            "term": {  # 关键字，表示需要匹配的元素
                                "isDetail": "false"
                            },
                        },
                        {"bool": {  # 关键字，表示使用 filter 查询，没有匹配度
                            "must_not": [  # 表示里面的条件必须匹配，多个匹配元素可以放在列表里
                                {
                                    "exists": {  # 关键字，表示需要匹配的元素
                                        "field": "isDetail"
                                    },
                                },

                            ],
                            # "must": [{
                            #     "exists": {  # 关键字，表示需要匹配的元素
                            #         "field": "website"
                            #     },
                            #
                            # }
                            # ]
                        }}

                    ]
                }

            }
        }
    )
    #res_lst = response["hits"]["hits"]
    #print(response)
    res_url = response["hits"]["hits"][0]['_source'].get('website')
    #print(response)
    id = response["hits"]["hits"][0]['_id']
    fullname = response["hits"]["hits"][0]['_source'].get('fullname')
    #es.close()
    return res_url,fullname, id

def insert_es(uid):

    updateBody = {
        "doc":{
            'isDetail': 'true'
        }
    }
    try:
        res = es.update(index="enterprisebaseinfo", doc_type="_doc", body=updateBody, id=uid)
    except:
        time.sleep(0.3)
        res = es.update(index="enterprisebaseinfo", doc_type="_doc", body=updateBody, id=uid)
    #es.close()
    return None
if __name__ == '__main__':
    es = Elasticsearch(["10.110.80.83:9200"])
    engine = create_engine("mysql+pymysql://root:6lfBxZLyopc8Q@UF@10.110.80.83:3306/tianyancha", echo=False)
    #info = get_baseInfo('punish', '13637692', '万科企业股份有限公司', '10', '1')
    #print(info)
    #info.to_sql('tyc_holder_baseinfo', if_exists='append', con=engine, index=False)

    #print(get_es('连云港市'))
    a = get_baseInfo('court', '396778332', '江苏中南建筑产业集团有限责任公司', '10', '1')
    print(a)
    #a.to_sql('tyc_staff_baseinfo', if_exists='append', con=engine, index=False)
    for i in range(0,1000000):
        #time.sleep(random.randint(0,2))
        website,fullname,uid = get_es('北京')
        print(website)
        tyid = website.split('/')[-1]
        dics = list(all_dic.keys())
        for item in dics:
            time.sleep(1)
            #print(item)
            for x in range(1,500):
                #print(x)
                item_info = get_baseInfo(item, tyid, fullname, '10', str(x))
                #print(item_info)
                flag = 0
                if len(item_info) != 0:
                    print(item)
                    for ix in item_info.loc[0].values:
                        #print(item_info.loc[0].values)
                        if '当前筛选条件下暂无数据，可切换条件再试' == ix or '抱歉，没有找到相关信息，请更换关键词重试' == ix:
                            flag=1
                            break
                    if flag==1:
                        break
                    else:
                        try:
                            item_info.to_sql('tyc_' + item + '_baseinfo', if_exists='append', con=engine, index=False)
                        except:
                            break
                else:

                    break

        insert_es(uid)
