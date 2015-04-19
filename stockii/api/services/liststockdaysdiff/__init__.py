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
        optName = args['optname']
        opt = args['opt']
        filter = None
        if ids is not None:
            filter = "stock_id in (%s)" %ids 
        
        ret = {'liststockdaysdiffresponse':{}}
        ret['liststockdaysdiffresponse']['details'] = []

        sortName = None
        if 'sortname' in args:
            sortName = args['sortname']
            if sortName == 'name' or sortName == 'stockname':
                sortName = 'stock_name'
            elif sortName == 'stockid':
                sortName = 'stock_id'
        
        if opt == 'plus' or opt == 'minus' or opt == 'divide':
            optType = {'plus':'+', 'minus':'-', 'divide':'/'}[opt]
            sql = 'select a.stock_id as stock_id, a.%s, b.%s, b.%s%sa.%s as op from (select stock_id,%s from stock_day_info where created = "%s") a,\
                (select stock_id,%s from stock_day_info where created = "%s") b where a.stock_id = b.stock_id'\
                %(optName, optName, optName, optType, optName, optName, startD, optName, endD)
            if filter is not None:
                sql += ' and a.' + filter
            if sortName != 'stock_id':
                sortName = 'op'
        elif opt == 'maxmin':
            sql = 'select stock_day_info.stock_id, max(stock_day_info.%s), min(stock_day_info.%s), max(stock_day_info.%s) - min(stock_day_info.%s) as maxmin from stock_day_info \
                where stock_day_info.created >="%s" and stock_day_info.created <="%s" and %s > 0' %(optName,  optName,  optName, optName, startD, endD, optName)
            if filter is not None:
                sql += ' and stock_day_info.' + filter
            sql += ' group by stock_day_info.stock_id'
            if sortName != 'stock_id':
                sortName = 'maxmin'   
        elif opt == 'maxmindivide':
            sql = 'select stock_day_info.stock_id, max(stock_day_info.%s), min(stock_day_info.%s), max(stock_day_info.%s) / min(stock_day_info.%s) as maxmin from stock_day_info \
                where stock_day_info.created >="%s" and stock_day_info.created <="%s" and %s > 0' %(optName,  optName,  optName, optName, startD, endD, optName)
            if filter is not None:
                sql += ' and stock_day_info.' + filter
            sql += ' group by stock_day_info.stock_id'
            if sortName != 'stock_id':
                sortName = 'maxmindivide'  
        elif opt == 'sum':
            sql = 'select stock_day_info.stock_id as stock_id as stock_name,sum(stock_day_info.%s) as s from stock_day_info \
                where stock_day_info.created >="%s" and stock_day_info.created <="%s"' %(optName, startD,  endD)
            if filter is not None:
                sql += ' and stock_day_info.' + filter
            sql += ' group by stock_day_info.stock_id'
            if sortName != 'stock_id':
                sortName = 's'   
        cursor.execute(sql)
        count = 0
        for item in cursor.fetchall():   
            row = {}
            row['stockid'] = (item[0])
            if opt == 'plus' or opt == 'minus' or opt == 'divide':
                row['startvalue'] = (item[1])
                row['endvalue'] = (item[2])
                row[optName] = (item[3])
            elif opt == 'maxmin' or opt == 'maxmindivide':
                row['maxvalue'] = (item[1])
                row['minvalue'] = (item[2])
                row[optName] = (item[3])
                row['maxdate']  = ''
                row['mindate']  = ''
                if (optName == 'current_price' or optName == 'volume_ratio' or optName == 'amplitude_ratio' or optName == 'turnover_ratio' or optName == 'total_money') and float(row['minvalue']) == 0:
                    continue
            else:
                row[optName] = (item[1])
            ret['liststockdaysdiffresponse']['details'].append(row)
            count += 1
        ret['liststockdaysdiffresponse']['count'] = count
        
        if opt == 'maxmin' or opt == 'maxmindivide':
            sql = 'select a.stock_id, a.created from (select stock_id, created, %s from stock_day_info where created >="%s" and created <="%s" and %s > 0 order by %s desc) as a' %(optName, startD, endD, optName, optName)
            if filter is not None:
                sql += ' where a.' + filter
            sql += ' group by a.stock_id'
            cursor.execute(sql)
            i = 0
            for item in cursor.fetchall():   
                if ret['liststockdaysdiffresponse']['details'][i]['stockid'] == (item[0]):
                    if ret['liststockdaysdiffresponse']['details'][i]['maxdate'] == '':
                        ret['liststockdaysdiffresponse']['details'][i]['maxdate'] = (item[1])
                    i += 1
                if i >= count:
                    break

            sql = 'select a.stock_id, a.created from (select stock_id, created, %s from stock_day_info where created >="%s" and created <="%s" and %s > 0 order by %s asc) as a' %(optName, startD, endD, optName, optName)
            if filter is not None:
                sql += ' where a.' + filter
            
            sql += ' group by a.stock_id'
            n = cursor.execute(sql)
            i = 0
            for item in cursor.fetchall(): 
#                    while i < count:
                if ret['liststockdaysdiffresponse']['details'][i]['stockid'] == (item[0]):
                    if ret['liststockdaysdiffresponse']['details'][i]['mindate']  == '':
                        ret['liststockdaysdiffresponse']['details'][i]['mindate'] = (item[1])
                    i += 1
                if i >= count:
                    break
        
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
    print run( {'starttime':'2012-01-01',  'stockid':'000001, 000002, 000006', 'endtime':'2013-01-01',  'optname':'growth_ratio',  'opt':'maxmin'})