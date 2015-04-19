#encoding: utf-8
import os, math

def run(args):
    '''
    args: 参数信息，类型为字典
    return: 返回一个元组，元组的第一个元素表示成功与否，第二元素为返回值
    '''
    conn = None
    cursor = None
    try:
        import MySQLdb, datetime
        conn=MySQLdb.connect(host="localhost",user="root",passwd="root",db="stock",charset="utf8")  
        cursor = conn.cursor()    
        
        startD = args['starttime']
        endD = args['endtime']
        weight = args['weight']
        optname = args['optname']
        startD = datetime.datetime.strptime(startD,"%Y-%m-%d").date()
        endD = datetime.datetime.strptime(endD,"%Y-%m-%d").date()
        
        sql = 'SELECT stock_id, list_date FROM stock_basic_info'
        cursor.execute(sql)
        listDate = {}
        item = cursor.fetchone()
        while item:
            listDate[item[0]] = item[1].date()
            item = cursor.fetchone()
        
        sql = 'SELECT stock_id, created , %s \
                                FROM stock_day_info \
                                WHERE created = "%s" OR created = "%s" \
                                ORDER BY stock_id , created' %( optname, startD, endD)
        cursor.execute(sql)
        startEndDict = {}
        item = cursor.fetchone()
        while item:
            stockId = item[0]
            if stockId not in startEndDict:
                startEndDict[stockId] = [item[2]]
            else:
                startEndDict[stockId] += [item[2]]
            item = cursor.fetchone()
        
        sql = 'SELECT stock_id,avg(%s) FROM stock_day_info WHERE created BETWEEN "%s" AND "%s" GROUP BY stock_id HAVING COUNT(created) >= 30' %(optname, startD, endD)
        cursor.execute(sql)
        positiveList = []
        negativeList = []
        defaultList = []
        item = cursor.fetchone()
        while item:
            stockId = item[0]
            avg = item[1]
            if stockId in startEndDict and len(startEndDict[stockId]) == 2:
                tempItem = DataItem(startEndDict[stockId][0], startEndDict[stockId][1], stockId, avg, weight)
                if tempItem.difference >= 0 :
                    positiveList.append( tempItem )
                else :
                    negativeList.append( tempItem )
            item = cursor.fetchone()
        positiveList.sort( key = sortKey )
        negativeList.sort( key = sortKey )
        if len( positiveList ) > 100 :
            defaultList += positiveList[-100:]
            defaultList.sort( key = sortKey )
            del positiveList[100:]
        else :
            defaultList += positiveList[:]
            defaultList.sort( key = sortKey )
        if len( negativeList ) > 100 :
            del negativeList[:-100]
        if len( defaultList ) > 100 :
            del defaultList[:-100]
            
        ret = {'listcrossinforesponse':{}}
        ret['listcrossinforesponse']['crossinfo'] = []
        count = 0
        border = min( 100 , len( positiveList ) )
        for i in range( 0 , border ) :
            row = {}
            row['stockid'] = positiveList[i].stockId
            row['startdate'] = str(startD)
            row['startvalue'] = positiveList[i].startPrice
            row['enddate'] = str(endD)
            row['endvalue'] = positiveList[i].endPrice
            row['avg'] = positiveList[i].avg
            row['difference'] = positiveList[i].difference
            row['crosstype'] = 'positive'
#                    print type(endD), type(listDate[positiveList[i].stockId])
#                    print type(endD-listDate[positiveList[i].stockId])
            row['endlistdate'] = (endD-listDate[positiveList[i].stockId]).days
            row['startlistdate'] = (startD-listDate[positiveList[i].stockId]).days
            ret['listcrossinforesponse']['crossinfo'].append(row)
            count += 1
        border = max( 0 , len( defaultList ) - 100 )
        for i in range( border , len( defaultList ) )[::-1] :
            row = {}
            row['stockid'] = defaultList[i].stockId
            row['startdate'] = str(startD)
            row['startvalue'] = defaultList[i].startPrice
            row['enddate'] = str(endD)
            row['endvalue'] = defaultList[i].endPrice
            row['avg'] = defaultList[i].avg
            row['difference'] = defaultList[i].difference
            row['crosstype'] = 'default'
            row['endlistdate'] = (endD-listDate[positiveList[i].stockId]).days
            row['startlistdate'] = (startD-listDate[positiveList[i].stockId]).days
            ret['listcrossinforesponse']['crossinfo'].append(row)
            count += 1
        border = max( 0 , len( negativeList ) - 100 )
        for i in range( border , len( negativeList ) )[::-1] :
            row = {}
            row['stockid'] = negativeList[i].stockId
            row['startdate'] = str(startD)
            row['startvalue'] = negativeList[i].startPrice
            row['enddate'] = str(endD)
            row['endvalue'] = negativeList[i].endPrice
            row['avg'] = negativeList[i].avg
            row['difference'] = negativeList[i].difference
            row['crosstype'] = 'negative'
            row['endlistdate'] = (endD-listDate[positiveList[i].stockId]).days
            row['startlistdate'] = (startD-listDate[positiveList[i].stockId]).days
            ret['listcrossinforesponse']['crossinfo'].append(row)
            count += 1
        ret['listcrossinforesponse']['count'] = count
        
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
 
class DataItem :
    def __init__( self, start_price, end_price, stock_id, avg ,weight):
        avg = float(avg)
        weight = float(weight) / 100
        self.startPrice = float(start_price)
        self.endPrice = float(end_price)
        self.stockId = stock_id
        self.avg = round( avg , 2 )
        
        value1 = math.log( self.startPrice / avg , 1 + weight )
        value2 = math.log( self.endPrice / avg , 1 + weight )
        if value1 > 0 :
            n1 = math.ceil( value1 )
        else :
            n1 = math.floor( value1 )
        if value2 > 0 :
            n2 = math.ceil( value2 )
        else :
            n2 = math.floor( value2 )
        self.difference = n2 - n1

def sortKey( x ) :
    return -x.difference
    
if __name__ == '__main__':
    print run({'starttime':'2011-10-10', 'endtime':'2012-10-10', 'weight': '2', 'optname':'avg_price'})