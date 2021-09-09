import requests
import json
import cx_Oracle
import pandas as pd
import datetime
import time
from sqlalchemy import create_engine
from sqlalchemy.dialects.oracle import \
            BFILE, BLOB, CHAR, CLOB, DATE, \
            DOUBLE_PRECISION, FLOAT, INTERVAL, LONG, NCLOB, \
            NUMBER, NVARCHAR, NVARCHAR2, RAW, TIMESTAMP, VARCHAR, \
            VARCHAR2

def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: NVARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: FLOAT(precision=2, asdecimal=True)})
        #if "float" in str(j):
            #dtypedict.update({i: Numeric(precision=18,scale=4, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: NUMBER()})
    return dtypedict
def get_day(y,m,d):
    today = datetime.date.today()
    begin = datetime.date(y,m,d)
    days = []
    for i in range((today - begin).days + 1):
        day = begin + datetime.timedelta(days=i)
        date= str(day).replace('-','')
        days.append(date)
    return days
def base():
    db = cx_Oracle.connect('lcgs709999/Abcd1234@10.0.19.92:1521/snyx')
    engine = create_engine('oracle://lcgs709999:Abcd1234@10.0.19.92:1521/snyx')
    conn = db.cursor()
    query = 'SELECT PRO_TIME FROM "ODS_SHFE_LWG" order BY PRO_TIME DESC'
    conn.execute(query)
    ymd = conn.fetchall()[0][0]
    print(int(ymd[:4]), int(ymd[4:6]), int(ymd[-2:]))
    days = get_day(int(ymd[:4]), int(ymd[4:6]), int(ymd[-2:]))
    # days = get_day(2019,3,1)
    for day in days:
        time.sleep(2)
        try:
            url = 'http://www.shfe.com.cn/data/dailydata/kx/kx' + day + '.dat'
            lwg = requests.get(url).text
            type_dic = json.loads(lwg)
            all_type = type_dic.get('o_curinstrument')
        except:
            all_type = []

        for product in all_type:
            if 'rb_f' in product.get('PRODUCTID'):
                df = pd.DataFrame(product, index=[0])
                df['pro_time'] = day
                print(df)
                try:
                    dtypedict = mapping_df_types(df)
                    df.to_sql(name='ods_shfe_lwg', con=engine, chunksize=1000, if_exists='append', index=None,
                              dtype=dtypedict)
                except Exception as e:
                    print(e)
    conn.close()
    db.close()
if __name__ == '__main__':
    base()

