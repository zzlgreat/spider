import json
import pandas as pd
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import time
import datetime
import requests
import pymysql
import socket
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import VARCHAR,CHAR,DECIMAL,DATE,TEXT,DATETIME
def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "V" in str(i) and 'F' in str(i):
            dtypedict.update({i: VARCHAR(length=255)})
        elif "N" in str(i) and 'F' in str(i):
            dtypedict.update({i: DECIMAL(22,6)})
        elif "D" in str(i) and 'F' in str(i):
            dtypedict.update({i: DATE()})
        elif "C" in str(i) and 'F' in str(i):
            dtypedict.update({i: CHAR(10)})
        elif "DATE" in str(i):
            dtypedict.update({i: DATETIME()})
        else:
            dtypedict.update({i: TEXT()})
    return dtypedict

socket.setdefaulttimeout(20)
headers={
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-HK;q=0.6',
    'Connection': 'keep-alive',
    'Content-Length':'0',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36',
    'Host': 'webapi.cninfo.com.cn',
    'mcode':'MTU4NjczOTQxOA==',
    #   'Origin':'http://webapi.cninfo.com.cn',
    'Referer':'http://webapi.cninfo.com.cn/',
    'X-Requested-With': 'XMLHttpRequest'
}
dic = {
#        #股票列表
#        'stocklist':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2101',
#                'method':'all',
#                'rule':'replace'
#                },
# #陆港通证券标的证券表
#        'hkconnlist':{'url':'http://webapi.cninfo.com.cn/api/stock/p_mhkconn4500',
#                'method':'all',
#                'rule':'replace',
#                'name':'陆港通证券标的证券表'
#                },
#        #详细信息
#        'baseinfo':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2100',
#                'method':['scode'],
#                'rule':'replace'
#                },
#        #公司管理人员任职情况
#        'management':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2102',
#                'method':['scode'],
#                'rule':'replace'
#                },
#        #股票所属板块
#        "sector":{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock0004',
#                'method':['scode'],
#                'rule':'replace'
#                },
# #证券简称变更情况
#        "shortchange":{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2109',
#                'method':['scode'],
#                'rule':'replace'
#                },
# #上市公司行业归属的变动情况
#        "indchange":{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2110',
#                'method':['scode'],
#                'rule':'replace'
#                },
# #行业分类数据
#        'industry':{'url':'http://webapi.cninfo.com.cn/api/stock/p_public0002?indtype=008001',
#                'method':'all',
#                'rule':'replace',
#                'name':'行业分类数据'
#                },
#        #定期报告预披露时间
#        "pretime":{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2237',
#                'method':['scode','sdate','edate'],
#                'rule':'append'
#                },
#        #上市公司业绩预告
#        'forecast':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2238',
#                'method':['scode','sdate','edate'],
#                'rule':'append'
#                },
#        #定期报告审计意见
#        'audit':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2239',
#                'method':['scode','sdate','edate'],
#                'rule':'append'
#                },
#        #个股报告期资产负债表
#        'asset':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2300',
#                'method':['scode','rdate'],
#                'rule':'append'
#                },
#        #个股报告期利润表
#        'profit':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2301',
#                'method':['scode','rdate'],
#                'rule':'append'
#                },
#        #个股报告期现金表
#        'cashflow':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2302',
#                'method':['scode','rdate'],
#                'rule':'append'
#                },
#        #个股报告期指标表
#        'financialindex':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2303',
#                'method':['scode','rdate'],
#                'rule':'append'
#                },
#        #业绩快报
#        'Performance':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2328',
#                'method':['scode'],
#                'rule':'append'
#                },
#     #上市公司智能资讯
# 'stockinfo':{'url':'http://webapi.cninfo.com.cn/api/info/p_info3020',#深小信资讯
#                'method':['edate','DECLAREDATE'],
#                'rule':'append'
#                },
#        #单一股票质押比例
#        'pledgeratio':{'url':'http://webapi.cninfo.com.cn/api/stock/p_rzrq3106',
#                'method':['scode','sdate','edate'],
#                'rule':'append',
#                'num':1
#                },
#        #证券交易特别提示
#        'hottip':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2202',
#                'method':['scode','sdate','edate'],
#                'rule':'append',
#                'num':20
#                },
#        #证券交易停复牌信息
#        'suspension':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2203',
#                'method':['scode','sdate','edate'],
#                'rule':'append'
#                },
#        #沪深异动证券公开信息
#        'unusual':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2204',
#                'method':['tdate','TRADEDATE'],
#                'rule':'append'
#                },
#        #日行情
#        'trade':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2402',
#                'method':['edate','TRADEDATE'],
#                'rule':'append'
#                },
# #融资融券明细
#        'funding':{'url':'http://webapi.cninfo.com.cn/api/stock/p_rzrq3104',
#                'method':['scode','sdate','edate'],
#                'rule':'append',
#                'num':300
#                },
# #陆港通每日市场统计数据
#        'hkconntrade':{'url':'http://webapi.cninfo.com.cn/api/stock/p_mhkconn4502',
#                'method':['tdate','TRADEDATE'],
#                'rule':'append',
#                'name':'陆港通每日市场统计数据'
#                },
#陆股通实时卖空数据-按日期查询
       'hkconnsell':{'url':'http://webapi.cninfo.com.cn/api/stock/p_mhkconn4501',
               'method':['tdate','TRADEDATE'],
               'rule':'append',
               'name':'陆股通实时卖空数据'
               },
#陆港通每日持股记录数据-按日期查询-沪港通
       'hkconnholdhu':{'url':'http://webapi.cninfo.com.cn/api/stock/p_mhkconn4503?sortcode=227002',
               'method':['tdate','TRADEDATE'],
               'rule':'append',
               'name':'陆港通每日持股记录数据'
               },
#陆港通每日持股记录数据-按日期查询-深港通
       'hkconnholdshen':{'url':'http://webapi.cninfo.com.cn/api/stock/p_mhkconn4503?sortcode=227004',
               'method':['tdate','TRADEDATE'],
               'rule':'append',
               'name':'陆港通每日持股记录数据'
               },
#陆港通每日市场十大成交-按日期查询-沪港通
       'hkconntenhu':{'url':'http://webapi.cninfo.com.cn/api/stock/p_mhkconn4504?sortcode=227002',
               'method':['tdate','TRADEDATE'],
               'rule':'append',
               'name':'陆港通每日市场十大成交'
               },
#陆港通每日市场十大成交-按日期查询-深港通
       'hkconntenshen':{'url':'http://webapi.cninfo.com.cn/api/stock/p_mhkconn4504?sortcode=227004',
               'method':['tdate','TRADEDATE'],
               'rule':'append',
               'name':'陆港通每日市场十大成交'
               },
       #证券复权因子
       'restoration':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2406',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
       #多市场交易日报
       'blocktrading':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2426',
               'method':['tdate','TRADEDATE'],
               'rule':'append'
               },
       #公司员工情况表
       'staff':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2107',
               'method':['scode'],
               'rule':'replace'
               },
      #十大流通股东持股情况
       'tencirculation':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2209',
               'method':['scode','rdate'],
               'rule':'append'
               },
      #十大股东持股情况
       'tenshareholder':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2210',
               'method':['scode','rdate'],
               'rule':'append'
               },
       #公司股东人数
       'shareholdernum':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2211',
               #'method':['scode','sdate','edate'],
                'method':['scode','rdate'],
               'rule':'append'
               },
       #股东持股集中度
       'Concentration':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2212',
               'method':['scode','rdate'],
               'rule':'append'
               },
       #公司股东实际控制人
       'actualcon':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2213',
               'method':['scode','rdate'],
               'rule':'append'
               },
       #公司股本变动
       'guben':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2215',
               'method':['scode','rdate'],
               'rule':'append'
               },
       #高管持股变动
       'OwnershipChange':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2218',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
       #股东股份冻结
       'freezing':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2219',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
       #股东股份质押
       'Pledge':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2220',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
       #股东增（减）持情况
       'shareholderchange':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2226',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
       #受限股份实际解禁日期
       'liftingdate':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2227',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
       #受限股份预计解禁日期表
       'liftingpre':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2228',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
       #股东大会召开情况
       'shareholdermeeting':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2222',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
       #股东大会议案
       'smbill':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2223',
               'method':['scode','sdate','edate'],
               'rule':'append',
               'num':20
               },
#股东大会相关事项变动
       'smchange':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2224',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#对外担保
       'guarantee':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2245',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司诉讼
       'litigation':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2246',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司受处罚表
       'punish':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2248',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司资产冻结表
       'freezeasset':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2249',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司仲裁
       'arbitration':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2250',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#大股东资金占用表
       'entrusted':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2260',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#上市公司日常关联交易预计表
       'related':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2261',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#上市公司投资表
       'investment':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2264',
               'method':['scode','sdate','edate'],
               'rule':'append',
                'num':1
               },
#分红转增信息
       'share':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2201',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司增发股票预案
       'seospre':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2229',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司增发股票实施方案
       'seos':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2230',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司配股预案
       'RightsIssuepre':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2231',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司配股实施方案
       'RightsIssue':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2232',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司首发股票
       'ipo':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2233',
               'method':['scode'],
               'rule':'replace'
               },
#募集资金来源
       'sourceoffund':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2234',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#股票发行中介机构及承销情况
       'intermediary':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2235',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#募集资金投资项目计划
       'targetoffund':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2236',
               'method':['scode','sdate','edate'],
               'rule':'append'
               },
#公司首发股票审核信息表
       'ipoexam':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2247',
               'method':['scode','sdate','edate'],
               'rule':'append',
               },
#新股过会情况表
       'newstockpass':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2262',
               'method':'all',
               'rule':'replace',
               'name':'新股过会情况表'
               },
#优先股派息表
       'yxgpx ':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2263',
               'method':['scode','sdate','edate'],
               'rule':'append',
                'name':'优先股派息表'
               },
#新股发行
       'newstock':{'url':'http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1097',
               'method':['timetype'],
               'rule':'append',
               'name':'新股发行'
               },
#股票智能摘要
       'aisummary':{'url':'http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1078',
               'method':['tdate','DECLAREDATE'],
               'rule':'append',
               'name':'股票智能摘要'
               },
#国证指数基本信息
       'csindex':{'url':'http://webapi.cninfo.com.cn/api/index/p_index2903',
               'method':'all',
               'rule':'replace',
               'name':'国证指数基本信息'
               },
       'exchangeindex':{'url':'http://webapi.cninfo.com.cn/api/index/p_index2911',
               'method':'all',
               'rule':'replace',
               'name':'交易所指数基本信息'
               },
       'exchangetrade':{'url':'http://webapi.cninfo.com.cn/api/index/p_index2905',
               'method':['edate','TRADEDATE'],
               'rule':'append',
               'name':'交易所指数日行情'
               },
       'researchsummary':{'url':'http://webapi.cninfo.com.cn/api/info/p_info3029',
               'method':['newid'],
               'rule':'append',
               'name':'研报摘要'
               },
       'coresearch':{'url':'http://webapi.cninfo.com.cn/api/info/p_info3032',
               'method':['edate','DECLAREDATE'],
               'rule':'append',
               'name':'公司研报数据'
               },
       'indresearch':{'url':'http://webapi.cninfo.com.cn/api/info/p_info3033',
               'method':['edate','DECLAREDATE'],
               'rule':'append',
               'name':'行业研报数据'
               },
       'macroresearch':{'url':'http://webapi.cninfo.com.cn/api/info/p_info3034',
               'method':['edate','DECLAREDATE'],
               'rule':'append',
               'name':'宏观研报数据'
               },
#主营业务收入行业分布
       'mainind':{'url':'http://webapi.cninfo.com.cn/api/stock/p_stock2336',
               'method':['scode','rdate'],
               'rule':'append'
               },

       }
marco_dic = {'CPI':{'url':'http://webapi.cninfo.com.cn/api/macro/p_CPI',
               'method':'all',
               'rule':'replace'
               },
'CPIbyCategory':{'url':'http://webapi.cninfo.com.cn/api/macro/p_CPIbyCategory',
               'method':['year'],
               'rule':'replace',
                 'name':'全国居民消费价格指数分类指数'
               },
'CPIbyRegion':{'url':'http://webapi.cninfo.com.cn/api/macro/p_CPIbyRegion',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市居民消费价格指数'
               },
'CPIbyRegionandCategory':{'url':'http://webapi.cninfo.com.cn/api/macro/p_CPIbyRegionandCategory',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市居民消费价格指数分类指数'
               },
'moneysupply':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9034',
               'method':'all',
               'rule':'replace'
               },
'PMI':{'url':'http://webapi.cninfo.com.cn/api/macro/p_PMI',
               'method':'all',
               'rule':'replace'
               },
'PPI':{'url':'http://webapi.cninfo.com.cn/api/macro/p_PPI',
               'method':'all',
               'rule':'replace'
               },
'centerbankBS1':{'url':'http://webapi.cninfo.com.cn/api/macro/p_BalanceSheetMenu?type=1',
               'method':'all',
               'rule':'replace'
               },
'centerbankBS2':{'url':'http://webapi.cninfo.com.cn/api/macro/p_BalanceSheetMenu?type=2',
               'method':'all',
               'rule':'replace'
               },
'countryinfo':{'url':'http://webapi.cninfo.com.cn/api/macro/p_countryinfo',
               'method':'all',
               'rule':'replace'
               },
'marcodate':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9002',
             'method':['sdate','edate'],
             'rule':'append',
             'name':'重要宏观数据日历'
               },
'macrobaseinfo':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macrobaseinfo?type=J',
               'method':'all',
               'rule':'append',
                 'name':'宏观数据基本编码'
               },
'SourcesandUsesofCreditFunds':{'url':'http://webapi.cninfo.com.cn/api/macro/p_SourcesandUsesofCreditFunds?type=1',
               'method':'all',
               'rule':'replace',
                 'name':'信贷收支表科目列表'
               },
'agpriceindex':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9017',
               'method':['year'],
               'rule':'replace',
                 'name':'全国农业价格指数(月度)'
               },

'PPIbyCategory':{'url':'http://webapi.cninfo.com.cn/api/macro/p_PPIbyCategory',
               'method':['year'],
               'rule':'replace',
                 'name':'工业生产者出厂价格行业分类指数(月度)'
               },
'PurchasingPriceIndexbyRegion':{'url':'http://webapi.cninfo.com.cn/api/macro/p_PurchasingPriceIndexbyRegion',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市工业生产者购进价格指数(月度)'
               },
'PurchasingPriceIndexbyRegionandCategory':{'url':'http://webapi.cninfo.com.cn/api/macro/p_PurchasingPriceIndexbyRegionandCategory',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市工业生产者购进价格指数—分类指数(月度)'
               },
'RPI':{'url':'http://webapi.cninfo.com.cn/api/macro/p_RPI',
               'method':'all',
               'rule':'replace',
                 'name':'全国商品零售价格指数(月度)'
               },
'RPIbyCategory':{'url':'http://webapi.cninfo.com.cn/api/macro/p_RPIbyCategory',
               'method':['year'],
               'rule':'replace',
                 'name':'全国商品零售价格指数-分类指数'
               },
'RPIbyRegion':{'url':'http://webapi.cninfo.com.cn/api/macro/p_RPIbyRegion',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市商品零售价格指数(月度)'
               },
'RPIbyRegionandCategory':{'url':'http://webapi.cninfo.com.cn/api/macro/p_RPIbyRegionandCategory',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市商品零售价格指数(月度)--分类指数'
               },
'centerbankbs':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9035',
               'method':['year'],
               'rule':'replace',
                 'name':'中央银行（货币当局）资产负债表'
               },
'reserve':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9036',
               'method':['year'],
               'rule':'replace',
                 'name':'国际储备月度统计表'
               },
'creditstate':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9037',
               'method':['year'],
               'rule':'replace',
                 'name':'金融机构人民币信贷收支表 （按类型）'
               },
'CategoryforInduValAdded':{'url':'http://webapi.cninfo.com.cn/api/macro/p_CategoryforInduValAdded',
               'method':'all',
               'rule':'replace',
                 'name':'工业增加值增长速度细分行业列表'
               },
'CCI':{'url':'http://webapi.cninfo.com.cn/api/macro/p_CCI',
               'method':'all',
               'rule':'replace',
                 'name':'消费者信心指数月度统计'
               },
'GrowthRateofInduValAdded':{'url':'http://webapi.cninfo.com.cn/api/macro/p_GrowthRateofInduValAdded',
               'method':['year'],
               'rule':'replace',
                 'name':'全国工业增加值增长速度（月度）'
               },
'GrowthRateofInduValAddedbyCategory':{'url':'http://webapi.cninfo.com.cn/api/macro/p_GrowthRateofInduValAddedbyCategory',
               'method':['year'],
               'rule':'replace',
                 'name':'全国细分行业工业增加值增长速度（月度）'
               },
'GrowthRateofInduValAddedbyRegion':{'url':'http://webapi.cninfo.com.cn/api/macro/p_GrowthRateofInduValAddedbyRegion',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市工业增加值增长速度（月度）'
               },
'GrowthRateofInduValAddedbyRegionandCategory':{'url':'http://webapi.cninfo.com.cn/api/macro/p_GrowthRateofInduValAddedbyRegionandCategory',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市细分行业工业增加值增长速度（月度）'
               },
'mainproduct':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9025',
               'method':['year'],
               'rule':'replace',
                 'name':'全国工业主要产品产量及增长速度'
               },
'benefits':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9028',
               'method':['year'],
               'rule':'replace',
                 'name':'全国工业企业经济效益综合数据(月度)'
               },
'indearns':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9029',
               'method':['year'],
               'rule':'replace',
                 'name':'全国各行业工业企业利润额(月度)'
               },
'bankindex':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9031',
               'method':['year'],
               'rule':'replace',
                 'name':'银行业景气指数(季度)'
               },
'productbyregion':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9032',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市工业主要产品产量及增长速度'
               },
'sales':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9047',
               'method':['year'],
               'rule':'replace',
                 'name':'全国消费品零售总额综合数据(月度)'
               },
'MajorIndustrialProducts':{'url':'http://webapi.cninfo.com.cn/api/macro/p_MajorIndustrialProducts',
               'method':'all',
               'rule':'replace',
                 'name':'工业主要产品列表'
               },
'fixedasset':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9019',
               'method':['year'],
               'rule':'replace',
                 'name':'全国固定资产投资价格指数(季度)'
               },
'fixedassetbyarea':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9024',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市固定资产投资价格指数(季度)'
               },
'realestate':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9042',
               'method':['year'],
               'rule':'replace',
                 'name':'全国房地产建设与销售'
               },
'realestatebyregion':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9043',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市房地产建设与销售'
               },
'fixedinvest':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9046',
               'method':['year'],
               'rule':'replace',
                 'name':'全国城镇固定资产投资情况(月度)'
               },
'fixedinvestbyarea':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9050',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市城镇固定资产投资情况(月度)'
               },
'govermentbs':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9054',
               'method':['year'],
               'rule':'replace',
                 'name':'全国政府财政收支情况(月度)'
               },
'familybs':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9055',
               'method':['year'],
               'rule':'replace',
                 'name':'全国家庭收入及支出统计（季度)'
               },
'gdp':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9056',
               'method':['year'],
               'rule':'replace',
                 'name':'生产法国民生产总值表'
               },
'gdpbyseason':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9057',
               'method':['year'],
               'rule':'replace',
                 'name':'生产法国内生产总值分季度统计表'
               },
'gdpbyregion':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9058',
               'method':['year'],
               'rule':'replace',
                 'name':'生产法国内生产总值各地区分季度统计表'
               },
'importexport':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9048',
               'method':['year'],
               'rule':'replace',
                 'name':'全国进出口贸易数据(月度)'
               },
'countryinvest':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9049',
               'method':['year'],
               'rule':'replace',
                 'name':'各国对华直接投资月度统计'
               },
'porttradebyregion':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9052',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市进出口贸易数据(月度)'
               },
'indforeigninvest':{'url':'http://webapi.cninfo.com.cn/api/macro/p_macro9053',
               'method':['year'],
               'rule':'replace',
                 'name':'各省市进出口贸易数据(月度)'
               }
             }
#获取已存在的天数
def get_day(dbname,sym):
    db = pymysql.connect(host="93.179.125.162",
                         port=3306,
                         database="stockinfo",
                         user="root",
                         password="Zzl33818!",
                         charset="utf8")
    conn = db.cursor()
    if 'DATE' in sym:
        symbol_sql_1 = "SELECT "+sym+" FROM " + dbname + " ORDER BY "+sym+" DESC LIMIT 1"
    else:
        symbol_sql_1 = "SELECT UPDATE_DATE FROM " + dbname + " where SECCODE = '" + sym + "' ORDER BY UPDATE_DATE DESC LIMIT 1"
    try:
        print(symbol_sql_1)
        conn.execute(symbol_sql_1)
        dats = conn.fetchall()
        if len(dats) > 0:
            print(str(type(dats[0][0])))
            if str(type(dats[0][0])) == "<class 'datetime.datetime'>":
                last_date = dats[0][0]
                print(dats[0][0])
            elif dats[0] != None:
                try:
                    last_date = datetime.datetime.strptime(dats[0][0], '%Y-%m-%d %H:%M:%S')
                except:
                    last_date = datetime.datetime.strptime(dats[0][0], '%Y-%m-%d')
            else:
                last_date = ''
        else:
            last_date = ''
    except:
        last_date = datetime.datetime.strptime('2017-01-01','%Y-%m-%d')
    print(last_date)
    today = datetime.date.today()
    if len(str(last_date))>11:
        #begin = datetime.datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S').date()
        last_date = last_date.date()
        begin = last_date
    elif len(str(last_date))<=11 and len(str(last_date))>0:
        begin = last_date
        #begin = datetime.datetime.strptime(last_date, '%Y-%m-%d').date()
    else:
        begin = datetime.datetime.strptime('2017-01-01', '%Y-%m-%d').date()
    days = []
    print(begin)
    for i in range((today - begin).days + 1):
        day = begin + datetime.timedelta(days=i)
        date= str(day).split('-')
        days.append(date)
    return days[1:],str(last_date)

#获取已存在的报告期
def get_season(dbname, datenm, sym):
    db = pymysql.connect(host="93.179.125.162",
                         port=3306,
                         database="stockinfo",
                         user="root",
                         password="Zzl33818!",
                         charset="utf8")
    conn = db.cursor()
    symbol_sql_1 = "SELECT " + datenm + " FROM " + dbname + " where SECCODE = '" + sym+"'"
    print(symbol_sql_1)
    try:
        conn.execute(symbol_sql_1)
        dats = conn.fetchall()
        all_dates = []
        for i in dats:
            try:
                rdate = datetime.datetime.strptime(str(i[0]), '%Y-%m-%d %H:%M:%S')
            except:
                rdate = datetime.datetime.strptime(str(i[0]), '%Y-%m-%d')
            #print(rdate)
            rdate = str(rdate).split(' ')[0].replace('-', '')
            all_dates.append(rdate)
    except Exception as e:
        print(e)
        all_dates = []
    #print(all_dates)
    dates = []
    for year in range(2000, 2022):
        ss = ['0331', '0630', '0930', '1231']
        for season in ss:

            y = str(year) + season
            if y not in all_dates:
                print(year, season)
                dates.append(y)
    db.close()
    return dates
#获取已存在的年
def get_year(dbname):
    db = pymysql.connect(host="93.179.125.162",
                         port=3306,
                         database="stockinfo",
                         user="root",
                         password="Zzl33818!",
                         charset="utf8")
    conn = db.cursor()
    symbol_sql_1 = "SELECT year FROM " + dbname
    print(symbol_sql_1)
    try:
        conn.execute(symbol_sql_1)
        dats = conn.fetchall()
        all_dates = []
        for i in dats:
            rdate = str(i[0])
            all_dates.append(rdate)
    except Exception as e:
        print(e)
        all_dates = []
    #print(all_dates)
    dates = []
    for year in range(2000, 2022):
        y = str(year)
        if y not in all_dates:
            dates.append(y)
    db.close()
    return dates

def get_mcode():
    # 关键步骤 1：下面两行代码是用来设置特性，获取request的信息前提步骤。

    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    options.add_experimental_option('w3c', False)
    d = DesiredCapabilities.CHROME
    d['loggingPrefs'] = {'performance': 'ALL'}
    try:
        driver = webdriver.Chrome(chrome_options=options, desired_capabilities=d)
        driver.get('http://webapi.cninfo.com.cn/#/dataBrowse')
    except:
        time.sleep(2)
        driver = webdriver.Chrome(chrome_options=options, desired_capabilities=d)
        driver.get('http://webapi.cninfo.com.cn/#/dataBrowse')

    # 关键步骤2：获取 request 信息。
    info = driver.get_log('performance')  # 这里的参数 'performance' 是步骤1中添加的。获取到的是一个列表。

    # 用 for循环读取列表中的每一项元素。每个元素都是 json 格式。
    for i in info:
        dic_info = json.loads(i["message"])  # 把json格式转成字典。
        info = dic_info["message"]['params']  # request 信息，在字典的 键 ["message"]['params'] 中。
        if 'request' in info:  # 如果找到了 request 信息，就终断循环。
            if info['request'].get('headers').get('mcode')!=None:
                mcode = info['request'].get('headers').get('mcode')
                break
    driver.quit()
    return mcode

def get_sym(num):
    if num==None:
        num =50
    db = pymysql.connect(host="93.179.125.162",
                         port=3306,
                         database="stockinfo",
                         user="root",
                         password="Zzl33818!",
                         charset="utf8")
    conn = db.cursor()
    #symbol_sql_1 = '''select SYMBOL from STOCKLIST order by list_date'''
    symbol_sql_1 = '''select SECCODE from ea_stocklist where F006D is not null order by F006D;'''
    conn.execute(symbol_sql_1)
    symbol_1 = conn.fetchall()
    sym = []
    for i in symbol_1:
        sym.append(i[0])
    syms = []
    for i in range(0, len(sym), num):
        try:
            b = sym[i:i + num]
        except:
            b = sym[i:]
        ss = ''
        for x in b:
            ss += x + ','
        syms.append(ss[:-1])
    db.close()
    return syms

def craw(section,url,method,rule,num):
    db = pymysql.connect(host="93.179.125.162",
                         port=3306,
                         database="stockinfo",
                         user="root",
                         password="Zzl33818!",
                         charset="utf8")
    conn = db.cursor()
    engine = create_engine("mysql+pymysql://root:Zzl33818!@93.179.125.162:3306/stockinfo", echo=False)
    mcode = get_mcode() #获取mode
    headers.update({'mcode': mcode})
    if method == 'all':
        res = requests.post(url, headers=headers).content.decode('utf-8')
        if json.loads(res).get('resultcode') != 200:
            print('过期了哦')
            time.sleep(1)
            mcode = get_mcode()
            headers.update({'mcode': mcode})
            res = requests.post(url, headers=headers).content.decode('utf-8')
            print(res)
        list_1 = json.loads(res).get('records')
        if list_1 != None and rule == 'replace':
            try:
                sql = 'truncate table ' + 'ea_' + section + ';'
                engine.execute(sql)
            except:
                pass
        for i in list_1:
            d = json.dumps(i, sort_keys=True)
            detail = json.loads(d)
            del d
            df = pd.DataFrame.from_dict(data=detail, orient='index').T
            # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
            dtypedict = mapping_df_types(df)
            df.to_sql(name='ea_'+section, con=engine, chunksize=1000, if_exists='append', index=None,
                      dtype=dtypedict)
    if 'scode' in method:
        if rule == 'replace':
            try:
                sql = 'truncate table ' + 'ea_' + section + ';'
                engine.execute(sql)
            except:
                pass
        syms = get_sym(num)
        for s in syms:
            urls = url+'?scode='+s
            if 'sdate' in method:
                days = get_day('ea_'+section, s[:6])
                urls = url+'?scode='+s+'&sdate='+days[1]+'&edate='+str(datetime.date.today())
                try:
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                except:
                    time.sleep(2)
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                print(urls)
                if json.loads(res).get('resultcode') != 200:
                    print('过期了哦')
                    print(res)
                    time.sleep(1)
                    mcode = get_mcode()
                    headers.update({'mcode': mcode})
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                list_1 = json.loads(res).get('records')
                # print(res)
                for i in list_1:
                    d = json.dumps(i, sort_keys=True)
                    detail = json.loads(d)
                    del d
                    df = pd.DataFrame.from_dict(data=detail, orient='index').T
                    # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
                    df['UPDATE_DATE'] = [str(datetime.datetime.today())]
                    dtypedict = mapping_df_types(df)
                    df.to_sql(name='ea_' + section, con=engine, chunksize=1000, if_exists='append', index=None,
                              dtype=dtypedict)

            elif 'rdate' in method:
                rdate = get_season('ea_'+section, 'ENDDATE', s[:6])
                for r in rdate:
                    print(r)
                    urls = url +'?scode='+s+ '&rdate='+r
                    #print(urls)
                    try:
                        res = requests.post(urls, headers=headers).content.decode('utf-8')
                    except:
                        time.sleep(2)
                        res = requests.post(urls, headers=headers).content.decode('utf-8')
                    if json.loads(res).get('resultcode') != 200:
                        print('过期了哦')
                        print(res)
                        time.sleep(1)
                        mcode = get_mcode()
                        headers.update({'mcode': mcode})
                        res = requests.post(urls, headers=headers).content.decode('utf-8')
                    list_1 = json.loads(res).get('records')
                    # print(res)
                    for i in list_1:
                        d = json.dumps(i, sort_keys=True)
                        detail = json.loads(d)
                        del d
                        df = pd.DataFrame.from_dict(data=detail, orient='index').T
                        # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
                        dtypedict = mapping_df_types(df)
                        df.to_sql(name='ea_' + section, con=engine, chunksize=1000, if_exists='append', index=None,
                                  dtype=dtypedict)
        #
            else:
                print('只有scode')
                print(urls)
                try:
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                except:
                    time.sleep(2)
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                if json.loads(res).get('resultcode') != 200:
                    print('过期了哦')
                    print(res)
                    time.sleep(1)
                    mcode = get_mcode()
                    headers.update({'mcode': mcode})
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                list_1 = json.loads(res).get('records')
                # print(res)
                for i in list_1:
                    d = json.dumps(i, sort_keys=True)
                    detail = json.loads(d)
                    del d
                    df = pd.DataFrame.from_dict(data=detail, orient='index').T
                    # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
                    dtypedict = mapping_df_types(df)
                    df.to_sql(name='ea_' + section, con=engine, chunksize=1000, if_exists='append', index=None,
                              dtype=dtypedict)


    elif 'edate' in method or 'tdate' in method and len(method) ==2:
        if rule == 'replace':
            try:
                sql = 'truncate table ' + 'ea_' + section + ';'
                engine.execute(sql)
            except:
                pass
        days = get_day('ea_' + section, method[1])
        for d in days[0]:
            print(d[0]+d[1]+d[2])
            if 'tdate' in method and '?' in url:
                urls = url + "&tdate=" + d[0]+d[1]+d[2]
            else:
                urls = url + "?"+method[0]+"=" + d[0]+d[1]+d[2]
            try:
                res = requests.post(urls, headers=headers).content.decode('utf-8')
            except:
                time.sleep(2)
                res = requests.post(urls, headers=headers).content.decode('utf-8')
            print(urls)
            if json.loads(res).get('resultcode') != 200:
                print('过期了哦')
                print(res)
                time.sleep(1)
                mcode = get_mcode()
                headers.update({'mcode': mcode})
                res = requests.post(urls, headers=headers).content.decode('utf-8')
            list_1 = json.loads(res).get('records')
            #print(res)
            for i in list_1:
                d = json.dumps(i, sort_keys=True)
                detail = json.loads(d)
                del d
                df = pd.DataFrame.from_dict(data=detail, orient='index').T
                # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
                df['UPDATE_DATE'] = [str(datetime.datetime.today())]
                dtypedict = mapping_df_types(df)
                df.to_sql(name='ea_' + section, con=engine, chunksize=1000, if_exists='append', index=None,
                          dtype=dtypedict)

    elif 'newid' in method:
        query = "select ID from ea_indresearch ORDER BY DECLAREDATE desc;"
        conn.execute(query)
        ids = conn.fetchall()
        idss = []
        for id in ids:
            idss.append(id[0])
        query = "select newid from ea_researchsummary;"
        conn.execute(query)
        hadids = []
        hadid = conn.fetchall()
        for hi in hadid:
            hadids.append(hi[0])
        for newid in ids:
            if newid in hadids:
                pass
            else:
                urls = url + "?newid=" + newid[0]
                try:
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                except:
                    time.sleep(2)
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                print(urls)
                if json.loads(res).get('resultcode') != 200:
                    print('过期了哦')
                    print(res)
                    time.sleep(1)
                    mcode = get_mcode()
                    headers.update({'mcode': mcode})
                    res = requests.post(urls, headers=headers).content.decode('utf-8')
                list_1 = json.loads(res).get('records')
                # print(res)
                for i in list_1:
                    d = json.dumps(i, sort_keys=True)
                    detail = json.loads(d)
                    del d
                    df = pd.DataFrame.from_dict(data=detail, orient='index').T
                    # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
                    df['UPDATE_DATE'] = [str(datetime.datetime.today())]

                    dtypedict = mapping_df_types(df)
                    df.to_sql(name='ea_' + section, con=engine, chunksize=1000, if_exists='append', index=None,
                              dtype=dtypedict)

def craw_marco(section,url,method,rule,num):
    db = pymysql.connect(host="93.179.125.162",
                         port=3306,
                         database="stockinfo",
                         user="root",
                         password="Zzl33818!",
                         charset="utf8")
    conn = db.cursor()
    engine = create_engine("mysql+pymysql://root:Zzl33818!@93.179.125.162:3306/stockinfo", echo=False)
    mcode = get_mcode() #获取mode
    headers.update({'mcode': mcode})
    if method == 'all':
        res = requests.post(url, headers=headers).content.decode('utf-8')
        if json.loads(res).get('resultcode') != 200:
            print('过期了哦')
            time.sleep(1)
            mcode = get_mcode()
            headers.update({'mcode': mcode})
            res = requests.post(url, headers=headers).content.decode('utf-8')
            print(res)
        list_1 = json.loads(res).get('records')
        if list_1 != None and rule == 'replace':
            try:
                sql = 'truncate table ' + 'ea_' + section + ';'
                engine.execute(sql)
            except:
                pass
        for i in list_1:
            d = json.dumps(i, sort_keys=True)
            detail = json.loads(d)
            del d
            df = pd.DataFrame.from_dict(data=detail, orient='index').T
            # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
            dtypedict = mapping_df_types(df)
            df.to_sql(name='ea_'+section, con=engine, chunksize=1000, if_exists='append', index=None,
                      dtype=dtypedict)
    elif 'year' in method:
        years = get_year('ea_'+section)
        for y in years:
            res = requests.post(url+'?year='+str(y), headers=headers).content.decode('utf-8')
            print(url+'?year='+str(y))
            if json.loads(res).get('resultcode') != 200:
                print('过期了哦')
                time.sleep(1)
                mcode = get_mcode()
                headers.update({'mcode': mcode})
                res = requests.post(url+'?year='+str(y), headers=headers).content.decode('utf-8')
                print(res)
            list_1 = json.loads(res).get('records')
            for i in list_1:
                d = json.dumps(i, sort_keys=True)
                detail = json.loads(d)
                del d
                df = pd.DataFrame.from_dict(data=detail, orient='index').T
                # df = pd.DataFrame(list_1).T.reset_index().drop('index', 1)
                dtypedict = mapping_df_types(df)
                df.to_sql(name='ea_' + section, con=engine, chunksize=1000, if_exists='append', index=None,
                          dtype=dtypedict)

if __name__ == '__main__':
    # name = 'macrobaseinfo'
    # craw_marco(name, marco_dic.get(name).get('url'), marco_dic.get(name).get('method'), marco_dic.get(name).get('rule'),
    #      marco_dic.get(name).get('num'))

    # all = list(marco_dic.keys())
    # for name in all:
    #     print(name)
    #     craw_marco(name,marco_dic.get(name).get('url'),marco_dic.get(name).get('method'),marco_dic.get(name).get('rule'),marco_dic.get(name).get('num'))
    all = list(dic.keys())
    for name in all:
        print(name)
        craw(name,dic.get(name).get('url'),dic.get(name).get('method'),dic.get(name).get('rule'),dic.get(name).get('num'))