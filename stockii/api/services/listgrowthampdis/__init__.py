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
        ids = None
        if 'stockid' in args:
            ids = args['stockid']
        filter = None
        startD = args['starttime']
        endD = args['endtime']
        if ids is not None:
            filter = "stock_id in (%s)" %ids 
        
        sql = 'select stock_day_info.stock_id, count(stock_day_info.growth_ratio), count(stock_day_info.amplitude_ratio)'
        sql = appendCountSql(sql, 'growth_ratio', None,  -9)
        sql = appendCountSql(sql, 'growth_ratio', -9,  -8)
        sql = appendCountSql(sql, 'growth_ratio', -8,  -7)
        sql = appendCountSql(sql, 'growth_ratio', -7,  -6)
        sql = appendCountSql(sql, 'growth_ratio', -6,  -5)
        sql = appendCountSql(sql, 'growth_ratio', -5,  -4)
        sql = appendCountSql(sql, 'growth_ratio', -4,  -3)
        sql = appendCountSql(sql, 'growth_ratio', -3,  -2)
        sql = appendCountSql(sql, 'growth_ratio', -2,  -1)
        sql = appendCountSql(sql, 'growth_ratio', -1,  0)
        sql = appendCountSql(sql, 'growth_ratio', 0,  1)
        sql = appendCountSql(sql, 'growth_ratio', 1,  2)
        sql = appendCountSql(sql, 'growth_ratio', 2,  3)
        sql = appendCountSql(sql, 'growth_ratio', 3,  4)
        sql = appendCountSql(sql, 'growth_ratio', 4,  5)
        sql = appendCountSql(sql, 'growth_ratio', 5,  6)
        sql = appendCountSql(sql, 'growth_ratio', 6,  7)
        sql = appendCountSql(sql, 'growth_ratio', 7,  8)
        sql = appendCountSql(sql, 'growth_ratio', 8,  9)
        sql = appendCountSql(sql, 'growth_ratio', 9,  None)
        sql = appendCountSql(sql, 'amplitude_ratio', 0,  1)
        sql = appendCountSql(sql, 'amplitude_ratio', 1,  2)
        sql = appendCountSql(sql, 'amplitude_ratio', 2,  3)
        sql = appendCountSql(sql, 'amplitude_ratio', 3,  4)
        sql = appendCountSql(sql, 'amplitude_ratio', 4,  5)
        sql = appendCountSql(sql, 'amplitude_ratio', 5,  6)
        sql = appendCountSql(sql, 'amplitude_ratio', 6,  7)
        sql = appendCountSql(sql, 'amplitude_ratio', 7,  8)
        sql = appendCountSql(sql, 'amplitude_ratio', 8,  9)
        sql = appendCountSql(sql, 'amplitude_ratio', 9,  10)
        sql = appendCountSql(sql, 'amplitude_ratio', 10,  11)
        sql = appendCountSql(sql, 'amplitude_ratio', 11,  12)
        sql = appendCountSql(sql, 'amplitude_ratio', 12,  13)
        sql = appendCountSql(sql, 'amplitude_ratio', 13,  14)
        sql = appendCountSql(sql, 'amplitude_ratio', 14,  15)
        sql = appendCountSql(sql, 'amplitude_ratio', 15,  16)
        sql = appendCountSql(sql, 'amplitude_ratio', 16,  17)
        sql = appendCountSql(sql, 'amplitude_ratio', 17,  18)
        sql = appendCountSql(sql, 'amplitude_ratio', 18,  19)
        sql = appendCountSql(sql, 'amplitude_ratio', 19, None)

        if filter is not None:
            filter = ' from stock_day_info where stock_day_info.' + filter + ' and stock_day_info.created >= "%s" and stock_day_info.created <= "%s"' %(startD,  endD)
        else:
            filter = ' from stock_day_info where stock_day_info.created >= "%s" and stock_day_info.created <= "%s"' %(startD,  endD)
        sql += filter
        sql +=' group by stock_day_info.stock_id'
        cursor.execute(sql)
        ret = {'listgrowthampdisresponse':{}}
        ret['listgrowthampdisresponse']['growthamp'] = []
        count = 0
        for item in cursor.fetchall():  
            row = {}
            tmp = 0
            row['stockid'] = item[0]
            row['growthcount']  = int(item[1])
            row['ampcount']  = int(item[2])
            tmp = 3
            row['g0'] = item[tmp]
            tmp += 1
            row['g1'] = item[tmp]
            tmp += 1
            row['g2'] = item[tmp]
            tmp += 1
            row['g3'] = item[tmp]
            tmp += 1
            row['g4'] = item[tmp]
            tmp += 1
            row['g5'] = item[tmp]
            tmp += 1
            row['g6'] = item[tmp]
            tmp += 1
            row['g7'] = item[tmp]
            tmp += 1
            row['g8'] = item[tmp]
            tmp += 1
            row['g9'] = item[tmp]
            tmp += 1
            row['g10'] = item[tmp]
            tmp += 1
            row['g11'] = item[tmp]
            tmp += 1
            row['g12'] = item[tmp]
            tmp += 1
            row['g13'] = item[tmp]
            tmp += 1
            row['g14'] = item[tmp]
            tmp += 1
            row['g15'] = item[tmp]
            tmp += 1
            row['g16'] = item[tmp]
            tmp += 1
            row['g17'] = item[tmp]
            tmp += 1
            row['g18'] = item[tmp]
            tmp += 1
            row['g19'] = item[tmp]
            tmp += 1
            row['a0'] = item[tmp]
            tmp += 1
            row['a1'] = item[tmp]
            tmp += 1
            row['a2'] = item[tmp]
            tmp += 1
            row['a3'] = item[tmp]
            tmp += 1
            row['a4'] = item[tmp]
            tmp += 1
            row['a5'] = item[tmp]
            tmp += 1
            row['a6'] = item[tmp]
            tmp += 1
            row['a7'] = item[tmp]
            tmp += 1
            row['a8'] = item[tmp]
            tmp += 1
            row['a9'] = item[tmp]
            tmp += 1
            row['a10'] = item[tmp]
            tmp += 1
            row['a11'] = item[tmp]
            tmp += 1
            row['a12'] = item[tmp]
            tmp += 1
            row['a13'] = item[tmp]
            tmp += 1
            row['a14'] = item[tmp]
            tmp += 1
            row['a15'] = item[tmp]
            tmp += 1
            row['a16'] = item[tmp]
            tmp += 1
            row['a17'] = item[tmp]
            tmp += 1
            row['a18'] = item[tmp]
            tmp += 1
            row['a19'] = item[tmp]
            ret['listgrowthampdisresponse']['growthamp'].append(row)
            count += 1
        ret['listgrowthampdisresponse']['count'] = count
   
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
        
def appendCountSql(sql, name, left, right):
    if left == None:
        sql += ', count(stock_day_info.%s < %d or null)' %(name, right)
    elif right == None:
        sql += ', count(stock_day_info.%s >= %d or null)' %(name, left)
    else:     
        sql += ', count(stock_day_info.%s >= %d and stock_day_info.%s < %d or null)' %(name, left, name, right)
    return sql
 
if __name__ == '__main__':
# 'stockid':'000001',  'starttime':'2000-01-01',  'endtime':'2014-01-01',  'sortname':'bull_profit'
    import time
    start = time.time()
    print run( {'starttime':'2011-01-01',  'endtime':'2012-01-01', 'stockid':'000001'})
    print time.time()-start