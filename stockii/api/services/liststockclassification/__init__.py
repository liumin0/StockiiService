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
        sql = 'SELECT stock_basic_info.stock_id,stock_basic_info.stock_name,industry_info.industry_name,area_info.area_name FROM stock_basic_info, stock_classification LEFT JOIN industry_info ON stock_classification.industry_id=industry_info.industry_id LEFT JOIN area_info ON stock_classification.area_id = area_info.area_id where stock_basic_info.stock_id=stock_classification.stock_id'
        cursor.execute(sql)
        ret = {'liststockclassificationresponse':{}}
        ret['liststockclassificationresponse']['stockclassification'] = []
        count = 0
        for item in cursor.fetchall():    
            row = {}
            row['stockid'] = (item[0])
            row['stockname'] = (item[1])
            row['industryname'] = (item[2])
            row['areaname'] = (item[3])
            ret['liststockclassificationresponse']['stockclassification'].append(row)
            count += 1
        ret['liststockclassificationresponse']['count'] = count
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