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
        sql = 'select distinct created from stock_day_info ORDER BY created asc'
        cursor.execute(sql)
        ret = {'listtradedateresponse':{}}
        ret['listtradedateresponse']['tradedate'] = []
        count = 0
        for item in cursor.fetchall():  
            row = {}
            row['listdate'] = item[0]
            ret['listtradedateresponse']['tradedate'].append(row)
            count += 1
        ret['listtradedateresponse']['count'] = count
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
    print run(123)