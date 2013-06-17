# -*- coding: utf-8 -*-
import re
from createindex import METADATA
from errortype import SqlError
oneKeywords = ["SELECT","FROM","WHERE",
	"DESC","ASC",
	"DATE","DAY","INT","CHAR","VARCHAR","DECIMAL",
	"SUM","AVG","COUNT","AS","TOP","AND","OR"]
twoKeywords = ["GROUP BY","ORDER BY"]
stmtTag = ["SELECT","FROM","WHERE","GROUP BY","ORDER BY",";"]

def rmNoUseChar(sql):
    sql = sql.replace(";"," ;")
    while sql.find("'") != -1:
        sql = sql.replace("'","")
    while sql.find('"') != -1:
        sql = sql.replace('"','')
    while sql.find('\t') != -1:
        sql = sql.replace("\t"," ")
    while sql.find('\n') != -1:
        sql = sql.replace("\n"," ")
    statements = sql.split(" ")
    while "" in statements:
        statements.remove("")
    sql=""
    for stmt in statements:
        sql += stmt+ " "
    return sql[0:-1]#最后一个空格删掉
            
def upperKeywords(sql):
    for key in twoKeywords:
        lKey = key.lower()
        if lKey in sql:
            sql = sql.replace(lKey,key)
    stmts = sql.split(" ")
    for key in oneKeywords:
        lKey = key.lower()
        for i in range(len(stmts)):
            stmt = stmts[i]
            if stmt == lKey or (lKey+",") == stmt or(lKey in stmt and "(" in stmt):
                stmts[i] = stmt.replace(lKey,key)
    sql = ""
    for stmt in stmts:
        sql += stmt+ " "
    return sql
def rmListSpace(li):
    while "" in li:
        li.remove("")
    return li
def nextStmtTag(sql,currentTag):
    index = sql.find(currentTag,0)
    for tag in stmtTag:
        if sql.find(tag,index+len(currentTag)) != -1:
            return tag
def rmStrSpace(string):
    li = string.split(" ")
    li = rmListSpace(li)
    result = ''
    for word in li:
        result += word+" "
    return result[0:-1]
def parseSelect(sql):
    selectAttr = []
    reg = "SELECT (.+) FROM"
    select = re.compile(reg).findall(sql)[0]
    selectList = select.split(",")
    for eachAttr in selectList:
        attrAggre = eachAttr.split(" ")
        attrAggre = rmListSpace(attrAggre)
        if len(attrAggre) <= 2:
            selectAttr.append([attrAggre[0].upper(),None,None])
        elif "(" in attrAggre[0]:#有聚集函数
            reg = "^(.+)\("
            aggre = re.compile(reg).findall(attrAggre[0])[0]
            reg = "\((.+)\)"
            attrName = re.compile(reg).findall(attrAggre[0])[0]            
            selectAttr.append([attrName.upper(),aggre,attrAggre[2]])
        elif "TOP" in attrAggre[0]:#有TOP N
            attrs = attrAggre[2].split(",")
            attrs = rmListSpace(attrs)
            for each in attrs:
                selectAttr.append([each.upper(),attrAggre[0],attrAggre[1]])
                
        else:
            selectAttr.append([attrAggre[0].upper(),None,attrAggre[2]])
    return selectAttr                     
                        
def parseFrom(sql):
    nextKey = nextStmtTag(sql,"FROM")
    reg = "FROM (.+) "+nextKey
    froms = re.compile(reg).findall(sql)[0]
    table = froms.split(",")
    table = rmListSpace(table)
    for i in range(len(table)):
        table[i] = table[i].upper().replace(" ","")
    return table

def parseWhere(sql):
    conditions = []
    if "WHERE " not in sql:
        return None
    nextKey = nextStmtTag(sql,"WHERE")
    reg = "WHERE (.+) "+nextKey
    where = re.compile(reg).findall(sql)[0]
    whereStmt = where.split("AND")
    for stmt in whereStmt:
        if "<=" in stmt:
            compare = "<="
        elif ">=" in stmt:
            compare = ">="
        elif "=" in stmt:
            compare = "="
        elif "<" in stmt:
            compare = "<"
        elif ">" in stmt:
            compare = ">"       
        reg = "^(.+)\s*"+compare        
        attr = re.compile(reg).findall(stmt)[0]
        attr = rmStrSpace(attr).upper()
        reg = compare+"\s*(.+)$"
        value = re.compile(reg).findall(stmt)[0]
        value = rmStrSpace(value)
        conditions.append([attr,compare,value])
    return conditions
def parseGroup(sql):
    groupby = []
    if "GROUP BY" not in sql:
        return None
    nextKey = nextStmtTag(sql,"GROUP BY")
    reg = "GROUP BY (.+) "+nextKey
    print 'reg',reg
    group = re.compile(reg).findall(sql)[0]
    groupStmt = group.split(",")
    groupStmt = rmListSpace(groupStmt)
    for each in groupStmt:
        groupby.append(rmStrSpace(each).upper())
    return groupby

def parseOrder(sql):
    orderby = []
    if "ORDER BY" not in sql:
        return None
    reg = "ORDER BY (.+);"
    order = re.compile(reg).findall(sql)[0]
    order = rmStrSpace(order)
    orders = order.split(",")
    for each in orders:
        each = rmStrSpace(each)
        ascDesc = each.split(" ")
        ascDesc = rmListSpace(ascDesc)
        if len(ascDesc) == 1:
            orderby.append([ascDesc[0].upper(),"ASC"])
        else:
            orderby.append([ascDesc[0].upper(),ascDesc[1].upper()])
    return orderby
def checkTable(tables,meta):
    for table in tables:
        if table not in meta:
            from errortype  import NO_SUCH_TABLE
            error = 'Error code:'+str(NO_SUCH_TABLE)+" No such table:"+table
            raise SqlError(error)
    return True

def isInTable(attr,tables,meta):
    for table in tables:            
        tableDesc = meta[table]
        if attr in tableDesc:
            return table
    return None

def checkSelect(selects,tables,meta):
    for i in range(len(selects)):
        select = selects[i]
        if select[0] == "*":
            continue
        findTable = isInTable(select[0],tables,meta)                
        if findTable == None:
            from errortype  import NO_SUCH_ATTR
            error =  'Error code:'+str(NO_SUCH_ATTR)+" No such attribute:"+select[0]
            raise SqlError(error)
        selects[i][0] =findTable+"."+select[0].upper()
    return selects

def checkWhere(wheres,tables,meta):
    if wheres == None:
        return None
    length = len(wheres)
    i = 0
    while i < length:   
        where = wheres[i]
        findTable = isInTable(where[0],tables,meta)
        if findTable == None:
            from errortype  import NO_SUCH_ATTR
            error =  'Error code:'+str(NO_SUCH_ATTR)+" No such attribute:"+where[0]
            raise SqlError(error)
        wheres[i][0] =findTable+"."+where[0].upper()        
        findTable = isInTable(where[2].upper(),tables,meta)#比较符号的右边是否为属性
        if findTable != None:
            where[2] =findTable+"."+where[2].upper()
            wheres.pop(i)
            wheres.append(where)
            i -= 1#后面的条件前移了
            length = length - 1
        i += 1
    return wheres

def checkGroup(groups,tables,meta):
    if groups == None:
        return None
    for i in range(len(groups)):
        findTable = isInTable(groups[i],tables,meta)
        if findTable == None:
            from errortype  import NO_SUCH_ATTR
            error =  'Error code:'+str(NO_SUCH_ATTR)+" No such attribute:"+groups[i]
            raise SqlError(error)
        groups[i] = findTable+"."+groups[i]
    return groups
def checkOrder(orders,tables,meta):
    if orders ==None:
        return None
    for i in range(len(orders)):
        findTable = isInTable(orders[i][0],tables,meta)
        if findTable == None:
            from errortype  import NO_SUCH_ATTR
            error =  'Error code:'+str(NO_SUCH_ATTR)+" No such attribute:"+orders[i][0]
            raise SqlError(error)
        orders[i][0] = findTable+"."+orders[i][0]
    return orders
        
def checkAndAddTableName(sqlDic,meta):
    tables = sqlDic["FROM"]
    selects = sqlDic["SELECT"]
    wheres = sqlDic["WHERE"]
    groups = sqlDic["GROUP"]
    orders = sqlDic["ORDER"]
    checkTable(tables,meta)#检查表名是否存在
    sqlDic["SELECT"] = checkSelect(selects,tables,meta)
    sqlDic["WHERE"] = checkWhere(wheres,tables,meta)
    sqlDic["GROUP"] = checkGroup(groups,tables,meta)
    sqlDic["ORDER"] = checkOrder(orders,tables,meta)
    return sqlDic
    
def parseSql(sql,meta):
    #sql=inputSql()
    sql = rmNoUseChar(sql)
    sql = upperKeywords(sql)
    sqlDic = {}
    sqlDic["SELECT"]= parseSelect(sql)
    sqlDic["FROM"]=  parseFrom(sql)
    sqlDic["WHERE"]= parseWhere(sql)
    sqlDic["GROUP"]= parseGroup(sql)
    sqlDic["ORDER"]= parseOrder(sql)
    return checkAndAddTableName(sqlDic,meta)
    
def inputSql():
    sql=""
    print 'input sql:'
    while ";" not in sql:
        sql += raw_input()+" "
    return sql
if __name__ == "__main__":
    #sql=inputSql()
    sql = '''
select
l_orderkey,
o_orderdate,
o_shippriority
from
customer,
orders,
lineitem
where	
c_mktsegment = '[SEGMENT]'
and c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate < date '[DATE]'
and l_shipdate > date '[DATE]'
group by
l_orderkey,
o_orderdate,
o_shippriority
order by
o_shippriority desc,
o_orderdate;


'''
    sql=inputSql()
    try:
        print parseSql(sql,METADATA)
    except SqlError as error:
        print error.value 

