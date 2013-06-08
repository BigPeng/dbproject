# -*- coding: utf-8 -*-

import os
import collections
import operator
import gzip
import time

DATABASE = "minidb"
SPLITTAG = "|"
BLOCKSIZE = 16384
def ceateDataBase(dbName):
    print 
'''
获取数据库名字
db.desc文件一共两行，一行为所有数据库名字，第二行为当前使用的数据名,如：
    minidb|dbproject
    minidb
'''
def useDataBase(dbName):
    dbdescName = "db.desc"
    if os.path.isfile(dbdescName) == False:
        print "No any database!"
        return False
    else:
        dbdescFile = open(dbdescName,"a+")
        line = dbdescFile.readline()
        if len(line) < 1:
            print "No such database:",dbName
            return False
        database = line.split(SPLITTAG)
        if dbName in database:#修改数据库使用状态
            countfile.seek(0)
            countfile.truncate()#清空文件
            dbdescFile.write(line+"\n"+dbName)
            return True
'''
建立一级索引
'''
def lineIndex(attrs,tableName,fileName):
    print 'create index on ',tableName
    rf = open(fileName,"r")
    wfs = []
    path = os.path.join(DATABASE,"index")
    if os.path.isdir(path) == False:
        os.mkdir(path)
    for each in attrs:
        wf = open(os.path.join(path,tableName+"_"+each),"w")
        wfs.append(wf)   
    loc = rf.tell()
    line = rf.readline()
    lineCount = 0#从第0行开始
    countText = ""
    while line != None and len(line) > 2:
        values = line.split("|")
        values.pop()#del the emputy char       
        for i in range(len(wfs)):
            wfs[i].write(values[i]+"\t"+str(lineCount)+"\n")
        countText += str(loc)+"\n"        
        loc = rf.tell()
        lineCount += 1
        line = rf.readline()
    path = os.path.join(DATABASE,"line2loc")#写字节与行对于文件
    if os.path.isdir(path) == False:
        os.mkdir(path)
    filename = os.path.join(path,tableName+"_LINE2LOC.gz")
    if os.path.isfile(filename) == False:#这个文件只需为一个表创建一次
        print time.time()
        with gzip.open(filename,'wb',compresslevel = 4) as output:
            output.write(countText)
            output.flush()
            output.close()
        print time.time()    
    for eachwf in wfs:
        eachwf.flush()
        eachwf.close()
    rf.close()

def supplier(attrs=None):
    if attrs == None:
        attrs = ["S_SUPPKEY","S_NAME","S_ADDRESS",
             "S_NATIONKEY","S_PHONE","S_ACCTBAL",
             "S_COMMENT"]
    tableName = "SUPPLIER"
    fileName = os.path.join(DATABASE,"supplier.tbl")
    lineIndex(attrs,tableName,fileName)
def part(attrs=None):
    if attrs == None:
        attrs = ["P_PARTKEY","P_NAME","P_MFGR",
             "P_BRAND","P_TYPE","P_SIZE",
             "P_CONTAINER","P_RETAILPRICE","P_COMMENT"]
    tableName = "PART"
    fileName = os.path.join(DATABASE,"part.tbl")
    lineIndex(attrs,tableName,fileName)
def partsupp(attrs=None):
    if attrs == None:
        attrs = ["PS_PARTKEY","PS_SUPPKEY","PS_AVAILQTY",
             "PS_SUPPLYCOST","PS_COMMENT"]
    tableName = "PARTSUPP"
    fileName = os.path.join(DATABASE,"partsupp.tbl")
    lineIndex(attrs,tableName,fileName)
def customer(attrs=None):
    if attrs == None:
        attrs = ["C_CUSTKEY","C_NAME","C_ADDRESS",
             "C_NATIONKEY","C_PHONE","C_ACCTBAL",
             "C_MKTSEGMENT","C_COMMENT"]
    tableName = "CUSTOMER"
    fileName = os.path.join(DATABASE,"customer.tbl")
    lineIndex(attrs,tableName,fileName)
def orders(attrs=None):
    if attrs == None:
        attrs = ["O_ORDERKEY","O_CUSTKEY","O_ORDERSTATUS",
             "O_TOTALPRICE","O_ORDERDATE","O_ORDERPRIORITY",
             "O_CLERK","O_SHIPPRIORITY","O_COMMENT"]
    tableName = "ORDERS"
    fileName = os.path.join(DATABASE,"orders.tbl")
    lineIndex(attrs,tableName,fileName)
def lineitem(attrs=None):
    if attrs == None:
        attrs = ["L_ORDERKEY","L_PARTKEY",
             "L_SUPPKEY","L_LINENUMBER",
             "L_QUANTITY","L_EXTENDEDPRICE",
             "L_DISCOUNT","L_TAX","L_RETURNFLAG",
             "L_LINESTATUS","L_SHIPDATE","L_COMMITDATE",
             "L_RECEIPTDATE","L_SHIPINSTRUCT","L_SHIPMODE",
             "L_COMMENT"]
    tableName = "LINEITEM"
    return
    fileName = os.path.join(DATABASE,"lineitem.tbl")
    lineIndex(attrs,tableName,fileName)
def nation(attrs=None):
    if attrs == None:
        attrs = ["N_NATIONKEY","N_NAME","N_REGIONKEY",
             "N_COMMENT"]
    tableName = "NATION"
    fileName =os.path.join(DATABASE, "nation.tbl")
    lineIndex(attrs,tableName,fileName)
def region(attrs=None):
    if attrs == None:
        attrs = ["R_REGIONKEY","R_NAME","R_COMMENT"]
    tableName = "REGION"
    fileName = os.path.join(DATABASE,"region.tbl")
    lineIndex(attrs,tableName,fileName)
    
#制定表和属性建立索引
    
def firstIndex(table,attr):
    attrs = []
    attrs.append(attr)
    if table == "region".upper():
        region(attrs)
    elif table == "nation".upper():
        nation(attrs)
    elif table == "lineitem".upper():
        lineitem(attrs)
    elif table == "nation".upper():
        nation(attrs)
    elif table == "orders".upper():
        orders(attrs)
    elif table == "customer".upper():
        customer(attrs)
    elif table == "partsupp".upper():
        partsupp(attrs)
    elif table == "part".upper():
        part(attrs)
    elif table == "supplier".upper():
        supplier(attrs)
    
#对一级索引排序并建立二级索引
def sort(fileName,attrType):
    dic = collections.defaultdict(list)
    path = os.path.join(DATABASE,"index")
    rf = open(os.path.join(path,fileName),"r")
    line = rf.readline()
    line = line[0:-1]
    while line != None and len(line) > 2:
        line = line[0:-1]#del '\n'
        vkpair = line.split("\t")
        key = vkpair[0]
        if attrType == "INT":
            key = int(key)
        elif attrType == "DECIMAL":
            key = float(key)        
        dic[key].append(vkpair[1])            
        line = rf.readline()
    li = sorted(dic.items(),key=operator.itemgetter(0))   
    #dic.clear()
    rf.close()
    scddir = os.path.join(DATABASE,"secondindex")#二级索引目录
    if os.path.isdir(scddir) == False:
        os.mkdir(scddir)
    scwf = open(os.path.join(scddir,fileName),"w")
    sortdir = os.path.join(DATABASE,"sorted")
    if os.path.isdir(sortdir) == False:
        os.mkdir(sortdir)    
    wf = gzip.open(os.path.join(sortdir,fileName+".gz"),'wb',compresslevel = 4)
    block = ""
    newblock = True#新块标志
    for k in li:
        if newblock == True:
            blockattr = str(k[0])#块首属性值
            newblock = False
        line = str(k[0])+"|"
        for loc in k[1]:
            line += loc+"|"
        line = line[0:-1]#delete the last " "
        block += line+"\n"
        if len(block) > BLOCKSIZE:
            secondloc = wf.tell()
            scwf.write(blockattr+SPLITTAG+str(secondloc)+'\n')
            wf.write(block)
            block = ""#块清空
            newblock = True#新块，下次循环记住新块首部属性值
    scwf.flush()  
    scwf.close()
    wf.flush()  
    wf.close()
#对每个表的每个属性进行排序
def sortAllIndex():
    descDic = loadMetaData()
    for table,desc in descDic.items():
        for i in range(1,len(desc)):
            fileName = table+"_"+desc[i][0]
            attrType = desc[i][1].split("(")[0]#对INT分割也成立哦
            print fileName,attrType
            sort(fileName,attrType)
    
#读入元数据表
def loadMetaData():
    meta = "meta.table"
    fileName = os.path.join(DATABASE,meta)
    mdFile = open(fileName,"r")
    line = mdFile.readline()
    tableNames = line.split(SPLITTAG)
    tableNames.pop()#删除最后一个空字符
    descDic = {}
    line = mdFile.readline()
    while line != None and len(line) > 2:#最后空行不处理        
        desc = []
        temp = line.split(SPLITTAG)
        primary = temp[1].split(" ")[0].split(",")
        desc.append(primary)#primary key
        for i in range(2,len(temp)):
            attrDesc = temp[i].split(' ')
            while "" in attrDesc:#删掉空格，只留下属性名和限制
                attrDesc.remove("")
            desc.append(attrDesc)
        descDic[temp[0]] = desc
        line = mdFile.readline()
    return descDic
#制定表和属性创建索引
def createIndex(table,attr):
    table = table.upper()#转为大写，方便处理
    attr = attr.upper()
    descDic = loadMetaData()
    if table not in descDic:
        from errortype import NO_SUCH_TABKE
        print 'Error code:',NO_SUCH_TABKE,'No such table:',table
        exit(NO_SUCH_TABKE)
    desc = descDic[table]
    attrType=None
    for i in range(2,len(desc)):#找属性的类型
        if attr == desc[i][0]:
            attrType = desc[i][1]
            break
    if attrType == None:#属性输入错误
        from errortype import NO_SUCH_ATTR
        print 'Error code:',NO_SUCH_ATTR,'No such attribute:',attr,' in:',table
        exit(NO_SUCH_TABKE)
    firstIndex(table,attr)#建立一级索引
    sort(table+"_"+attr,attrType)#建立二级索引    

    
if __name__=="__main__":
    #sort("LINEITEM_L_SUPPKEY","INT")
    #sortAllIndex()
    #createIndex("CUSTOMER","C_NATIONKEY")
    
'''
    supplier()
    part()
    partsupp()
    customer()
    orders()
    lineitem()
    nation()
    region()
    #sort()
    #loadMetaData()
    sortAllIndex()'''
    
    
   
  
