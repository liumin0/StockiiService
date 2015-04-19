#encoding: utf-8
import os

def run(args):
    '''
    args: 参数信息，类型为字典
    return: 返回一个元组，元组的第一个元素表示成功与否，第二元素为返回值
    '''
    conn = None
    cursor = None
    try:
        import MySQLdb
        conn=MySQLdb.connect(host="localhost",user="root",passwd="root",db="stock",charset="utf8")  
        cursor = conn.cursor()    
        startD = args['starttime']
        endD = args['endtime']
        ids = None
        if 'stockid' in args:
            ids = args['stockid']
        optName = args['sumname']

        filter = None
        if ids is not None:
            filter = "stock_id in (%s)" %ids 
        ret = {'listndayssumresponse':{}}
        ret['listndayssumresponse']['stockndayssum'] = []
        sql = 'select stock_day_info.stock_id as stock_id,sum(stock_day_info.%s) as s from stock_day_info \
            where stock_day_info.created >="%s" and stock_day_info.created <="%s"' %(optName, startD,  endD)
        if filter is not None:
            sql += ' and stock_day_info.' + filter
        sql += ' group by stock_day_info.stock_id'
        cursor.execute(sql)
        count = 0
        for item in cursor.fetchall(): 
            row = {}
            count += 1
            row['stockid'] = (item[0])
            row[optName] = (item[1])
            ret['listndayssumresponse']['stockndayssum'].append(row)
        ret['listndayssumresponse']['count'] = count
   
        conn.close()
        cursor.close()
        return True, ret
    except:
        if conn is not None:
            conn.close()
        if cursor is not None:
            cursor.close()
        import traceback
        traceback.print_exc()
        return False, "Error"
 
if __name__ == '__main__':
# 'stockid':'000001',  'starttime':'2000-01-01',  'endtime':'2014-01-01',  'sortname':'bull_profit'
    import time
    start = time.time()
    run( {'starttime':'2001-01-01',  'endtime':'2013-01-01', 'sumname':'avg_price'})
    print time.time()-start