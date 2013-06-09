# -*- coding: utf-8 -*-
import os
import gzip
from createindex import DATABASE,loadMetaData,SPLITTAG

'''
读取压缩二级索引的一个块
'''
def readIndexBlock(fileName,loc,size):
    filepath = os.path.join(DATABASE,"sorted")
    fileName = fileName+".gz"
    indexFile = gzip.open(os.path.join(filepath,fileName),'rb')
    indexFile.seek(int(loc))
    return indexFile.read(int(size))
'''
比较两个数大小
'''
def compareValue(valueA,compare,valueB):
    if compare == "=":
        return valueA==valueB
    elif compare == "<=":
        return valueA <= valueB
    elif compare == ">=":
        return valueA >= valueB
    elif compare == "<":
        return valueA < valueB
    elif compare == ">":
        return valueA > valueB    
'''
二叉查找blockIn = False查找块，blockIn = True快内查找
查找块时，即便是等值查找也只是找到满足<=value的块
    二级索引格式如下：
    ["1|0|16392","1862|16392|16390"...]
    value要与属性值比较，需进行分割
    返回scIndex对应的下标
    
块内查找时，等值查找必须相等
4,lzz5Uyym0WUT9HaMN|126466
4,m1Kkd8WdalsDyzQPCF|80784
4,m5twD7nF7,rlta0p|102107
'''
def binarySearch(scIndex,compare,value,domain,blockIn = False):
    length = len(scIndex)
    high =length -1
    low = 0
    while low <= high:
        middle = (low + high) / 2
        valueLoc = scIndex[middle].split(SPLITTAG)
        attrValue = valueLoc[0]
        if domain == "INT":
            attrValue = int(attrValue)
        elif domain == "decimal":
            attrValue = float(attrValue)
        if compareValue(attrValue,compare,value):
            if "=" in compare:#"=","<=",">="，满足比较条件即找到
                return middle
            else:
                if middle <length:
                    print 

def binaryEqualSearch(scIndex,compare,value,domain,blockIn = False):
    length = len(scIndex)
    high =length -1
    low = 0
    while low <= high:
        middle = (low + high) / 2
        valueLoc = scIndex[middle].split(SPLITTAG)
        attrValue = valueLoc[0]
        if domain == "INT":
            attrValue = int(attrValue)
        elif domain == "decimal":
            attrValue = float(attrValue)
        if compareValue(attrValue,compare,value):           
            return middle
            
        
    

'''
单表小于查询
'''
def lessFind(attr,compare,value):
    METADATA = loadMetaData()
    attr = attr.upper()
    ta = attr.split(".")#table.attr
    desc = METADATA[ta[0]]
    domain = desc[ta[1]].split("(")[0]#值域,decimal(15,2)
    fileName = ta[0]+"_"+ta[1]    
    path = os.path.join(DATABASE,"secondindex")
    scIndexFile = open(os.path.join(path,fileName),"r")#读索引文件
    scIndex = scIndexFile.read().split("\n")
    if len(scIndex[-1]) == 0:
        scIndex.pop()#删除最后一个无用字符
    satisfyList = []
    satisfyList.append(ta[0])#第一步添加表名，以备以后查找
    for i in range(len(scIndex)):        
        blockAttr = scIndex[i].split(SPLITTAG)
        attrValue = blockAttr[0]
        if domain == "INT":
            attrValue = int(attrValue)
        if domain == "DECIMAL":
            attrValue = float(attrValue)
        if compareValue(attrValue,compare,value):#找到了所需要的压缩二级索引块头
            locsize = [blockAttr[1],blockAttr[2]]
            block = readIndexBlock(fileName,locsize[0],locsize[1])
            blockLines = block.split("\n")
            #print blockLines[0]          
            for each in blockLines:
                blockAttr = each.split(SPLITTAG)
                attrValue = blockAttr[0]
                if domain == "INT":
                    attrValue = int(attrValue)
                if domain == "DECIMAL":
                    attrValue = float(attrValue)
                if compareValue(attrValue,compare,value):
                    blockAttr.pop(0)
                    satisfyList += blockAttr
                else :
                    break#出现大于，终止
                    break
        else:
            break#整块大于,终止
        #print 'block:',i,len(satisfyList)
    return satisfyList
   
        
            
    
    
'''
单表查询
'''
def findInTable(attr,compare,value):
    if compare == "=":
        return equalFind(attr,compare,value)    
    elif compare == ">":
        return moreFind(attr,compare,value)
    elif compare == "<":
        return lessFind(attr,compare,value)
    elif compare == "<=":
        return lEqualFind(attr,compare,value)
    elif compare == ">=":
        return mEqualFind(attr,compare,value)
    else:
        print "Unsupport compare:",compara
        return None

'''
根据比较条件join两个表
'''
def joinTable(attr,compare,para):
    print '还没实现呢'
    
'''
判断操作数是不是属性，是属性必须满足table.attr格式
'''
def isAttr(para):
    if '.' not in para:
        return False
    else:
        para = para.split('.')
        table = para[0]
        attr = para[1]
        meta = METADATA
        if table in meta:
            desc = meta[table]
            for attrdesc in desc:
                if attr in attrdesc:
                    return True
                else:#属性名不对
                    return False
        else:#表名不对
            return False
'''
conditions = [(table.attr,compareTag,table.attr|value),(...)...]
找出满足条件的记录，输出格式如：
satisfy = [(table1,table2,...),(1,5,...)...]
第一个元组表示
'''

def getSatisfy(conditions):
    for attr,compare,para in conditions:
        if isAttr(para) == True:
            return joinTable(attr,compare,para)
        else:
            return findInTable(attr,compare,para)
##def binarySeach(value,candidate):
##    length = len(candidate)
##    high = lenght -1
##    low = 0
##    while low < high:
##        mid = (low + high) / 2
##        if 
        
def topK(k,orderAttrs,candidate):
    order = "DESC"
    orderAttr = orderAttrs[0]
    orderAttr = orderAttr.upper()
    ta = orderAttr.split('.')
    fileName = ta[0]+"_"+ta[1]    
    path = os.path.join(DATABASE,"secondindex")
    scIndexFile = open(os.path.join(path,fileName),"r")#读索引文件
    scIndex = scIndexFile.read().split("\n")
    if len(scIndex[-1]) == 0:
        scIndex.pop()#删除最后一个无用字符
    selectedNum = 0;
    topList = []
    topList.append(ta[0])#添加表名
    if order == "DESC":#降序
        blockVisiteList = range(len(scIndex)-1,-1,-1)
    else:
        blockVisiteList = range(len(scIndex))
    for i in blockVisiteList:
        print 'block:',i
        blockAttr = scIndex[i].split(SPLITTAG)
        block = readIndexBlock(fileName,blockAttr[1],blockAttr[2])
        print block
        blockLines = block.split("\n")
        if order == "DESC":#降序
            visiteList = range(len(blockLines)-1,-1,-1)
        else:
            visiteList = range(len(blockLines))
        for j in visiteList:            
            lineAttr = blockLines[j].split(SPLITTAG)
            for index in range(1,len(lineAttr)):
                if lineAttr[index] in candidate:
                    topList.append(lineAttr[index])
                    print lineAttr[0],lineAttr[index]
                    selectedNum += 1
                    if selectedNum >= k:
                        return topList
                    

#行号到字节地址的转换
def line2loc(satisfy):
    length = len(satisfy)
    table = satisfy[0].upper()
    fileName = table + "_LINE2LOC.gz"
    satisfy.pop(0)#表名删掉
    path = os.path.join(DATABASE,"line2loc")    
    transFile = gzip.open(os.path.join(path,fileName),'rb')
    locations = transFile.read().split("\n")
    satisLoc = []
    for each in satisfy:
        satisLoc.append(locations[int(each)])
    return satisLoc,table
            
'''
首先从表中将行号转为字节地址，再从表中读取记录
'''
        
def readRecord(resultAttr,satisfy):
    satisLoc,tableName = line2loc(satisfy)
    fileName = tableName+".tbl"
    tableFile = open(os.path.join(DATABASE,fileName),"r")
    result = []
    for recordLoc in satisLoc:
        tableFile.seek(int(recordLoc))
        record = tableFile.readline().split("|")
        record.pop()#最后一个空字符
        result.append(record)
    return result
          
    
'''求topN
table=[table1,table2,...]
resultAttr = [table.attr1,table.attr2,...]
conditions的比较条件在上游方法确定，本方法按下标增序依次执行
conditions = [(table.attr,compareTag,table.attr|value),(...)...]
orderAttrs = [table.attr,table.attr...]
首先根据条件选出满足条件的记录，再对记录按指定属性排序，最后投影出输出属性
'''
def selectTopK(tables,k,resultAttr,conditions,orderAttrs):
    satisfy = getSatisfy(conditions)
    print len(satisfy)
    kItems = topK(k,orderAttrs,satisfy)
    #kItems = sorted(kItems)
    result = readRecord(resultAttr,kItems)
    print "result",result
    
   


if __name__ == "__main__":
    tables = ['ORDERS']
    resultAttr = ['*']
    conditions = [('ORDERS.O_ORDERDATE','<','1995-03-08')]
    orderAttrs = ['ORDERS.O_TOTALPRICE']
    k=20
    import time
    t1 = time.time()
    print t1
    selectTopK(tables,k,resultAttr,conditions,orderAttrs)
    print time.time(),time.time()-t1
