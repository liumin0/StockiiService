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
        sumType = args['sumtype']
        sumName = args['sumname']
        tableName = ''
        num = 1

        num = args['days']
        if sumType == 'all':
            tableName = '%s_sum' %sumName
        else:
            tableName = '%s_%s_sum' %(sumType, sumName)
       
        filter = 'x.created >= "%s" and x.created <= "%s"' %(startD,  endD)
        if ids is not None:
            filter += " and x.stock_id in (%s)" %ids 
        
        innerFilterType = None
        if 'filter' in args:
            innerFilterType = args['filter']
            tableFilter, condition = parseInnerFilter(innerFilterType)
            filter += ' and x.stock_id=y.stock_id and x.created=y.created and y.stock_id=z.stock_id and y.created=z.created %s %s' %(tableFilter, condition)
            countSql = 'select count(*) from %s x, stock_day_info z, twodaysdifference y where ' %tableName + filter
        else:
            countSql = 'select count(*) from %s x where ' %tableName + filter
            
        cursor.execute(countSql)
        ret = {'listdaysumresponse':{}}
        ret['listdaysumresponse']['count'] = int(cursor.fetchall()[0][0])
        ret['listdaysumresponse']['daysuminfo'] = []
        response = ret['listdaysumresponse']['daysuminfo']
        columnNum = {1:'one',2:'two',3:'three',4:'four',5:'five',6:'six',7:'seven',8:'eight',9:'nine',10:'ten',\
                  11:'eleven',12:'twelve',13:'thirteen',14:'fourteen',15:'fifteen',16:'sixteen',17:'seventeen',18:'eighteen',\
                  19:'nineteen',20:'twenty',21:'twenty1',22:'twenty2',23:'twenty3',24:'twenty4',25:'twenty5',26:'twenty7',\
                  27:'twenty7',28:'twenty8',29:'twenty9',30:'thirty'}[int(num)]
        columnName = columnNum + '_sum'
        retColumnName = columnNum + 'sum'
        
        if innerFilterType is not None:
            sql = 'select x.stock_id,x.created,x.%s from %s x, stock_day_info z, twodaysdifference y where ' %(columnName, tableName) + filter
        else:
            sql = 'select x.stock_id,x.created,x.%s from %s x where ' %(columnName, tableName) + filter
                
        if 'sortname' in args:
            sortName = args['sortname']
            if sortName == 'name' or sortName == 'stockname':
                sortName = 'stock_name'
            elif sortName == 'stockid':
                sortName = 'stock_id'
            sortName = sortName.replace('week',  '_week')
            sortName = sortName.replace('month',  '_month')
            sortName = sortName.replace('sum',  '_sum')
            if 'asc' in args and args['asc'] == 'false':
                sql += ' order by %s desc' %sortName
            else:
                sql += ' order by %s asc' %sortName
        
        if 'page' in args and 'pagesize' in args:
            sql += ' limit %d,%d' %((int(args['page']) - 1)*int(args['pagesize']),  int(args['pagesize']))
        
        cursor.execute(sql)
        for item in cursor.fetchall():  
            row = {}
            row['stockid'] = (item[0])
            row['created'] = (item[1])
            row[retColumnName] = (item[2])
            response.append(row)
        
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
 
def parseInnerFilter(innerFilterType):
    ret = None
    condition = ''
    tableFilter = ''
    try:
        flag, type = innerFilterType.split('#')
        clz, switchType, subType = type.split('_')
        if switchType == '0':
            condition = { 
             '0': u' and z.today_begin_price=z.current_price and z.max=z.min and z.current_price=z.max',     #开盘=收盘=最高=最低
            }[subType]
        elif switchType == '1':
            condition = {
             '0': u' and z.amplitude_ratio<2',                                       #振幅 < 2%
             '1': u' and amplitude_ratio=0'                                          #振幅 = 0
            }[subType]
        elif switchType == '2':
            condition = {
             '0': u' and z.sold_price=0 and z.growth_ratio>4.9',                        #u'卖价 = 0, 涨幅 > 4.9%'
             '1': u' and z.bought_price=0 and z.growth_ratio<-4.9'                      #u'买价 = 0, 涨幅 < -4.9%'
            }[subType]
        elif switchType == '3':
            condition = {
             '0': u' and z.total_money<99990000',                                    #u'总金额 < 0.9999亿'
             '1': u' and z.total_money<199990000 and z.total_money>100000000',       #u'总金额 1 ~ 1.9999亿'
             '2': u' and z.total_money<499990000 and z.total_money>200000000',       #u'总金额 2 ~ 4.9999亿'
             '3': u' and z.total_money>=500000000',                                  #u'总金额 >= 5亿', 
             '4': u' and z.total_money>900000000',                                   #u'总金额 >  9亿', 
             '5': u' and z.total_money<1500000000',                                  #u'总金额 >  15亿'
             '6': u' and z.total_money>2000000000',                                  #u'总金额 >  20亿'
             '7': u' and z.total_money>2500000000',                                  #u'总金额 >  25亿'
             '8': u' and z.total_money>3000000000',                                  #u'总金额 >  30亿'
             '9': u' and z.total_money>4000000000'                                   #u'总金额 >  40亿'  
            }[subType]
        elif switchType == '4':
            condition = {
             '0': u' and z.turnover_ratio<0.9999',                                    #u'换手率 < 0.9999%'
             '1': u' and z.turnover_ratio<2.9999 and z.turnover_ratio>=1',            #u'换手率 1% ~ 2.9999%'
             '2': u' and z.turnover_ratio<4.9999 and z.turnover_ratio>=3',            #u'换手率 3% ~ 4.9999%'
             '3': u' and z.turnover_ratio<6.9999 and z.turnover_ratio>=5',            #u'换手率 5% ~ 6.9999%'
             '4': u' and z.turnover_ratio<9.9999 and z.turnover_ratio>=7',            #u'换手率 7% ~ 9.9999%'
             '5': u' and z.turnover_ratio>=10',                                       #u'换手率 >= 10%'
             '6': u' and z.turnover_ratio>15',                                        #u'换手率 > 15%'
             '7': u' and z.turnover_ratio>20',                                        #u'换手率 > 20%'
             '8': u' and z.turnover_ratio>25',                                        #u'换手率 > 25%'
             '9': u' and z.turnover_ratio>30',                                        #u'换手率 > 30%'
             '10': u' and z.turnover_ratio>35',                                       #u'换手率 > 35%'
             '11': u' and z.turnover_ratio>40',                                       #u'换手率 > 40%'
             '12': u' and z.turnover_ratio>45',                                       #u'换手率 > 45%'
             '13': u' and z.turnover_ratio>50',                                       #u'换手率 > 50%'
             '14': u' and z.turnover_ratio>60',                                       #u'换手率 > 60%'
            }[subType]
        elif switchType == '5':
            subTypeL = subType.split('.')
            subType = subTypeL[0]
            condition = {
             '0': u' and MONTH(z.created)<4',                                         #u'第 1 季度'
             '1': u' and MONTH(z.created)<7 and MONTH(z.created)>=4',                 #u'第 2 季度'
             '2': u' and MONTH(z.created)<10 and MONTH(z.created)>=7',                #u'第 3 季度'
             '3': u' and MONTH(z.created)<=12 and MONTH(z.created)>=10',              #u'第 4 季度'
             '4': u' and MONTH(z.created)=1',                                         #u'1 月'
             '5': u' and MONTH(z.created)=2',                                         #u'2 月'
             '6': u' and MONTH(z.created)=3',                                         #u'3 月'
             '7': u' and MONTH(z.created)=4',                                         #u'4 月'
             '8': u' and MONTH(z.created)=5',                                         #u'5 月'
             '9': u' and MONTH(z.created)=6',                                         #u'6 月'
             '10': u' and MONTH(z.created)=7',                                        #u'7 月'
             '11': u' and MONTH(z.created)=8',                                        #u'8 月'
             '12': u' and MONTH(z.created)=9',                                        #u'9 月'
             '13': u' and MONTH(z.created)=10',                                       #u'10 月'
             '14': u' and MONTH(z.created)=11',                                       #u'11 月'
             '14': u' and MONTH(z.created)=12',                                       #u'12 月'
            }[subType]
        elif switchType == '5':
            condition = ''
            
        if flag == 'FLAG_UP':
            tableFilter = u' and y.ty_minus>0' 
        else:
            tableFilter = u' and y.yt_minus>0' 
    except:
        pass
    
    return tableFilter, condition

if __name__ == '__main__':
# 'stockid':'000001',  'starttime':'2000-01-01',  'endtime':'2014-01-01',  'sortname':'bull_profit'
    print run( {'starttime':'2000-01-01',  'endtime':'2014-01-01',  'sumtype':'all',  'sumname':'avg_price',  'days':'3',  'page':'1',  'pagesize':1000,  'sortname':'threesum'})