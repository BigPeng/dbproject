# -*- coding: utf-8 -*-

import os
import collections
import operator
import gzip
import time

DATABASE = "minidb"
SPLITTAG = "|"
BLOCKSIZE = 32768#16384
METADATA = None


'''
获取数据库名字
db.desc文件一共两行，一行为所有数据库名字，第二行为当前使用的数据名,如：
    minidb|dbproject
    minidb
'''
#暂未使用本方法
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
def splitCloum(tableName,attrName):
    tableName = tableName.upper()
    attrName = attrName.upper()
    fileName = tableName +"_" + attrName
    path = os.path.join(DATABASE,"cloums")#写字节与行对于文件
    if os.path.isdir(path) == False:
        os.mkdir(path)
    cloumFile = open(os.path.join(path,fileName),'w')
    tableFile = open(os.path.join(DATABASE,tableName.lower()+".tbl"),'r')
    cloum = getAttrOder(tableName,attrName)
    line  = tableFile.readline()
    while line != None and len(line) > 0:
        record = line.split(SPLITTAG)
        cloumFile.write(record[cloum]+"\n")
        line  = tableFile.readline()
    cloumFile.flush()
    cloumFile.close()
    tableFile.close()
    
'''
建立一级索引
'''
def lineIndex(attrs,tableName,fileName,newAll = False):
    print 'create index on ',tableName,attrs,fileName,
    rf = open(fileName,"r")
    wfs = []
    path = os.path.join(DATABASE,"index")
    if os.path.isdir(path) == False:
        os.mkdir(path)
    attrOrders = []#属性序号
    for each in attrs:
        attrOrders.append(getAttrOder(tableName,each))
        wf = open(os.path.join(path,tableName+"_"+each),"w")
        wfs.append(wf)
    print attrOrders
    loc = rf.tell()
    line = rf.readline()
    lineCount = 0#从第0行开始
    countText = ""
    while line != None and len(line) > 2:
        values = line.split("|")
        values.pop()#del the emputy char       
        for i in range(len(wfs)):
            wfs[i].write(values[attrOrders[i]]+"\t"+str(lineCount)+"\n")
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
    pathfile = os.path.join(path,fileName)
    if os.path.isfile(pathfile) == False:#还没有抽取该属性
        ta = fileName.split("_")
        firstIndex(ta[0],ta[1])
    rf = open(pathfile,"r")
    line = rf.readline()
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
    endloc=0
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
            startloc = endloc
            wf.write(block)
            endloc = wf.tell()
            size = endloc - startloc
            scwf.write(blockattr+SPLITTAG+str(startloc)+SPLITTAG+str(size)+'\n')
            block = ""#块清空
            newblock = True#新块，下次循环记住新块首部属性值
    startloc = endloc
    wf.write(block)
    endloc = wf.tell()
    size = endloc - startloc
    scwf.write(blockattr+SPLITTAG+str(startloc)+SPLITTAG+str(size)+'\n')#不足块大小部分写入
    
    scwf.flush()  
    scwf.close()
    wf.flush()  
    wf.close()
#对每个表的每个属性进行排序,并建立二级索引
def sortAllIndex():
    meta = loadMetaData()
    for table,desc in meta.items():
        desc.pop('primary')#忽略主键
        for attr,domain in desc.items():
            fileName = table+"_"+attr
            attrType = domain.split("(")[0]#对INT分割也成立哦
            print fileName,attrType
            sort(fileName,attrType)

#获取表中某个表的属性序号
#
def getAttrOder(table,attr):
    meta = loadMetaData()
    tableDesc = meta[table]
    count = -1
    for k,v in tableDesc.items():
        count += 1
        if k == attr:
            return count
#读入元数据表
#元数据返回格式为：{"tableName":{"primary":[attr1,attr2,...],"attr1":"desc1",...},...}
#其中的内层字典是有序字典，可供定位属性值
def loadMetaData():
    fileName = os.path.join(DATABASE,"meta.table")
    mdFile = open(fileName,"r")
    line = mdFile.readline()
    tableNames = line.split(SPLITTAG)
    tableNames.pop()#删除最后一个空字符
    descDic = {}
    line = mdFile.readline()
    while line != None and len(line) > 2:#最后空行不处理        
        desc =collections.OrderedDict() 
        temp = line.split(SPLITTAG)
        primary = temp[1].split(" ")[0].split(",")        
        for i in range(2,len(temp)):
            attrDesc = temp[i].split(' ')
            while "" in attrDesc:#删掉空格，只留下属性名和限制
                attrDesc.remove("")
            desc[attrDesc[0]]=attrDesc[1]
        desc["primary"]=primary#primary key加在有序字典最后
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
    attrType=desc[attr].split("(")[0]
    if attrType == None:#属性输入错误
        from errortype import NO_SUCH_ATTR
        print 'Error code:',NO_SUCH_ATTR,'No such attribute:',attr,' in:',table
        exit(NO_SUCH_TABKE)
    firstIndex(table,attr)#建立一级索引
    sort(table+"_"+attr,attrType)#建立二级索引    
'''
指定表压缩
'''
def condense(table):
    table = table.lower()
    filename = os.path.join(DATABASE,table+".tbl")
    tablefile = open(filename,"r")
    filename = os.path.join(DATABASE,table+".tb")
    cdsfile = gzip.open(filename,'wb',compresslevel = 4)
    readlength = 16*1024
    while True:
        text = tablefile.read(readlength)
        cdsfile.write(text)
        if len(text) < readlength:
            break
    cdsfile.flush()
    cdsfile.close()
    tablefile.close()
def condenseAll():
    print
    
def testread():
    table = "orders"
    filename = os.path.join(DATABASE,table+".tbl")
    tablefile = open(filename,"r")
    filename = os.path.join(DATABASE,table+".tb")
    cdsfile = gzip.open(filename,'rb')
    loc = 10234
    tablefile.seek(loc)
    print tablefile.read(100)
    print 80*"*"
    cdsfile.seek(loc)
    print cdsfile.read(100)
    #cdsfile.colse()
    tablefile.close()
def testUseCondense():
    filename = "minidb\\sorted\\CUSTOMER_C_ADDRESS.gz"
    cdsfile = gzip.open(filename,'rb')
    cdsfile.seek(442759)
    print cdsfile.read(100)  
if not METADATA:
    #print 'Init meta data'
    METADATA = loadMetaData()  
if __name__=="__main__":
    
    #sort("CUSTOMER_C_ADDRESS","VARCHAR")
    #sortAllIndex()
    #createIndex("NATION","N_NATIONKEY")
    #condense("orders")
    #testread()
    #testUseCondense()
    #sortAllIndex()
    splitCloum('LINEITEM','L_ORDERKEY')
    
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
    
    
   
  
