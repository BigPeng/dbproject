# -*- coding: utf-8 -*-
import os
import gzip
from createindex import DATABASE,METADATA,SPLITTAG
from parsesql import parseSql,inputSql

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
在二级索引中折半查找大于等于的块头
'''
def mBinarySearch(scIndex,compare,value,domain):
    compare = compare.replace('>','<')
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
        if middle == 0 and compareValue(attrValue,'>',value):
            return 0
        if compareValue(attrValue,compare,value):           
            if middle == length-1:
                return middle
            else:
                if compareValue(attrValue,'>',value):
                    return middle
                else:
                    low = middle + 1#还在右半部分
        else:
            high = middle - 1#还在左半部分
    return -1

#折半范围查找
#若查找成功，返回最大的下标，满足data[i]<value或data[i]<=value
#若不存在制定的范围，则返回-1
#blockFind=False 表示在块内查找，blockFind = True表示在块的索引上查找

def binarySearch(scIndex,compare,value,domain,blockFind = False):
    length = len(scIndex)
    low = 0
    high = length - 1
    while low <= high:#等值折半查找value
        middle = (high-low) / 2 + low
        valueLoc = scIndex[middle].split(SPLITTAG)
        attrValue = valueLoc[0]
        if domain == "INT":
            attrValue = int(attrValue)
        elif domain == "decimal":
            attrValue = float(attrValue)
        if attrValue == value:
            break#找到value，跳出循环
        if attrValue < value:
            low = middle + 1
        else:
            high = middle - 1
    fmt = '%10s   %10s   %10s'
    print fmt % ('Seach',value,compare)
    print fmt % ('low','middle','high')
    print fmt % (low,middle,high)
    #print fmt % (scIndex[low],scIndex[middle],scIndex[high])
    if low <= high:#value在data中
        if compare == '<=' or compare == '>=' or compare == '=':#包含‘=’的比较，直接返回值value的下标
            return middle
        elif compare == '<':#'<'需要返回前面一个下标，有可能为-1
            return middle - 1
        else:
            #(compare == '>')    '>'需要返回后面一个下标
            if middle == length -1:#middle等于有序表长度时，表示不存在data[i]>value，返回-1
                return -1
            else:
                return middle + 1#返回后面一个下标
    else:#value不在data中，此时high = low - 1,如果查找成功，value 在(high,low)的开区间中
        if compare == '=':
            return -1#等值查找，返回-1，查找失败
        elif compare == '<' or compare == '<=':#返回区间左边端点，有可能返回-1
            return high
        else:
            #compare == '>' or compare == '>='
            if low >= length:#不存在区间，返回-1
                return -1
            else:
                if blockFind == False:
                    return low#返回区间右端点
                else:
                    return high#查找块应该返回左端点
                
def equalFind(attr,compare,value):
    attr = attr.upper()
    ta = attr.split(".")#table.attr
    desc = METADATA[ta[0]]
    domain = desc[ta[1]].split("(")[0]#值域,decimal(15,2)
    if domain == "INT":
        value = int(value)
    if domain == "DECIMAL":
        value = float(value)
    fileName = ta[0]+"_"+ta[1]    
    path = os.path.join(DATABASE,"secondindex")
    scIndexFile = open(os.path.join(path,fileName),"r")#读索引文件
    scIndex = scIndexFile.read().split("\n")
    if len(scIndex[-1]) == 0:
        scIndex.pop()#删除最后一个无用字符
    satisfyList = []    
    index = binarySearch(scIndex,'<=',value,domain,blockFind=True)
    if index == -1:#没找到满足条件的
        return satisfyList
    print 'index:',index
    string = ''
    if type(scIndex[index]) == type(string):
            scIndex[index] = scIndex[index].split(SPLITTAG)
    block = readIndexBlock(fileName,scIndex[index][1],scIndex[index][2])
    blockLines = block.split("\n")
    if len(blockLines[-1]) == 0:
        blockLines.pop()#删除最后一个无用字符
    lineIndex = binarySearch(blockLines,compare,value,domain)
    if lineIndex == -1:#没找到满足条件的
        return satisfyList
    blockAttr = blockLines[lineIndex].split(SPLITTAG)
    blockAttr.pop(0)
    satisfyList += blockAttr
    satisfyList.append(ta[0])
    return satisfyList
    
'''
单表大于等于查询
'''
def moreFind(attr,compare,value):
    attr = attr.upper()
    ta = attr.split(".")#table.attr
    desc = METADATA[ta[0]]
    domain = desc[ta[1]].split("(")[0]#值域,decimal(15,2)
    if domain == "INT":
        value = int(value)
    if domain == "DECIMAL":
        value = float(value)
    fileName = ta[0]+"_"+ta[1]    
    path = os.path.join(DATABASE,"secondindex")
    scIndexFile = open(os.path.join(path,fileName),"r")#读索引文件
    scIndex = scIndexFile.read().split("\n")
    if len(scIndex[-1]) == 0:
        scIndex.pop()#删除最后一个无用字符
    satisfyList = []    
    index = binarySearch(scIndex,compare,value,domain,blockFind=True)
    if index == -1:#没找到满足条件的
        return satisfyList
    print 'index:',index
    string = ''
    if type(scIndex[index]) == type(string):
            scIndex[index] = scIndex[index].split(SPLITTAG)
    block = readIndexBlock(fileName,scIndex[index][1],scIndex[index][2])
    blockLines = block.split("\n")
    if len(blockLines[-1]) == 0:
        blockLines.pop()#删除最后一个无用字符
    lineIndex = binarySearch(blockLines,compare,value,domain)
    print 'lineIndex',lineIndex
    for i in range(lineIndex,len(blockLines)):#读取块内满足条件的部分
        blockAttr = blockLines[i].split(SPLITTAG)
        print blockAttr.pop(0)
        satisfyList += blockAttr    
    for i in range(index+1,len(scIndex)):#依次读取整块
        if type(scIndex[i]) == type(string):
            scIndex[i] = scIndex[i].split(SPLITTAG)
        block = readIndexBlock(fileName,scIndex[i][1],scIndex[i][2])
        blockLines = block.split("\n")
        if len(blockLines[-1]) == 0:
            blockLines.pop()#删除最后一个无用字符
        for each in blockLines:
            blockAttr = each.split(SPLITTAG)
            blockAttr.pop(0)
            satisfyList += blockAttr
    satisfyList.append(ta[0])
    return satisfyList
    
    
    

'''
单表小于查询
'''
def lessFind(attr,compare,value):   
    attr = attr.upper()
    ta = attr.split(".")#table.attr
    desc = METADATA[ta[0]]
    domain = desc[ta[1]].split("(")[0]#值域,decimal(15,2)
    if domain == "INT":
        value = int(value)
    if domain == "DECIMAL":
        value = float(value)
    fileName = ta[0]+"_"+ta[1]    
    path = os.path.join(DATABASE,"secondindex")
    scIndexFile = open(os.path.join(path,fileName),"r")#读索引文件
    scIndex = scIndexFile.read().split("\n")
    if len(scIndex[-1]) == 0:
        scIndex.pop()#删除最后一个无用字符
    satisfyList = []
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
                    
        else:
            break#整块大于,终止
        #print 'block:',i,len(satisfyList)
    satisfyList.append(ta[0])#添加表名，以备以后查找
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
        return lessFind(attr,compare,value)
    elif compare == ">=":
        return moreFind(attr,compare,value)
    else:
        print "Unsupport compare:",compara
        return None

def binarySeach(value,li):
    low = 0
    high = len(li)-1
    while low <= high:
        mid = (high - low) / 2 + low
        if li[mid] == value:
            return mid
        elif value > li[mid] :#在右边
            low = mid + 1
        else:
            high = mid - 1
    return -1
def binaryGet(line,satisfy):
    if satisfy == None:
        return line
    inSatisfy = []
    line.pop(0)#第一个是属性值    
    for eachLoc in line:
        if binarySeach(eachLoc,satisfy) != -1:
            inSatisfy.append(eachLoc)
    return inSatisfy
'''
根据比较条件join两个表，只考虑等值连接
在单表查询的基础上进行连接
'''
def joinTable(attr,compare,para,leftSatisfy,rightSatisfy):
    ta = attr.split(".")#table.attr
    desc = METADATA[ta[0]]
    domain = desc[ta[1]].split("(")[0]#值域,decimal(15,2)
    if leftSatisfy != None:#先排序
        leftSatisfy.pop()
        leftSatisfy = sorted(leftSatisfy)
    if rightSatisfy != None:
        rightSatisfy.pop()
        rightSatisfy = sorted(rightSatisfy)
    print len(leftSatisfy)
    print len(rightSatisfy)
    path = os.path.join(DATABASE,"secondindex")
    leftFileName = attr.replace(".","_")
    rightFileName = para.replace(".","_")
    path = os.path.join(DATABASE,"secondindex")
    leftScIndexFile = open(os.path.join(path,leftFileName),"r")#读索引文件
    leftScIndex = leftScIndexFile.read().split("\n")
    if len(leftScIndex[-1]) == 0:
        leftScIndex.pop()#删除最后一个无用字符
    rightScIndexFile = open(os.path.join(path,rightFileName),"r")#读索引文件
    rightScIndex = rightScIndexFile.read().split("\n")
    if len(rightScIndex[-1]) == 0:
        rightScIndex.pop()#删除最后一个无用字符
    i = j = 0
    joinResult = []
    leftAttr = leftScIndex[i].split(SPLITTAG)
    rightAttr = rightScIndex[i].split(SPLITTAG)
    leftBlock = readIndexBlock(leftFileName,leftAttr[1],leftAttr[2])
    rightBlock = readIndexBlock(rightFileName,rightAttr[1],rightAttr[2])
    i += 1
    j += 1
    leftBlockLines = leftBlock.split("\n")
    rightBlockLines = rightBlock.split("\n")
    if len(leftBlockLines[-1])==0:
        leftBlockLines.pop()
    if len(rightBlockLines[-1])==0:
        rightBlockLines.pop()
    lNum = rNum = 0
    while i < len(leftScIndex) and j < len(rightScIndex):
        leftLine = leftBlockLines[lNum].split(SPLITTAG)
        rightLine = rightBlockLines[rNum].split(SPLITTAG)
        if len(leftLine[-1])==0:
            leftLine.pop()
        if len(rightLine[-1])==0:
            rightLine.pop()
        leftValue = leftLine[0]
        rightValue = rightLine[0]
        if domain == "INT":
            leftValue = int(leftValue)
            rightValue = int(rightValue)
        if domain == "DECIMAL":
            leftValue = float(leftValue)
            rightValue = float(rightValue)
        #print leftValue,  rightValue  
        if leftValue < rightValue:            
            lNum += 1#左表下标后移
        elif leftValue > rightValue:
            rNum += 1
        else:#相等
            inLeftSatisfy = binaryGet(leftLine,leftSatisfy)
            inRightSatisfy = binaryGet(rightLine,rightSatisfy)
            for eachLeft in inLeftSatisfy:
                for eachRight in inRightSatisfy:
                    joinResult.append([leftValue,eachLeft,eachRight])
            lNum += 1
            rNum += 1
        if lNum >= len(leftBlockLines):
            leftAttr = leftScIndex[i].split(SPLITTAG)
            leftBlock = readIndexBlock(leftFileName,leftAttr[1],leftAttr[2])
            leftBlockLines = leftBlock.split("\n")
            if len(leftBlockLines[-1])==0:
                leftBlockLines.pop()
            i += 1
            lNum = 0#块内下标重置0
        if rNum >= len(rightBlockLines):
            rightAttr = rightScIndex[j].split(SPLITTAG)
            rightBlock = readIndexBlock(rightFileName,rightAttr[1],rightAttr[2])
            rightBlockLines = rightBlock.split("\n")
            if len(rightBlockLines[-1])==0:
                rightBlockLines.pop()
            j += 1
            rNum = 0
    print i,j
    joinResult.append([attr.split(".")[0],para.split(".")[0]])
    print len(joinResult)
    return joinResult    
    
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
            if attr in desc:
                return True
            else:#属性名不对
                return False
        else:#表名不对
            return False
def joinResultNum(firstResult):
    count = 0
    for each in firstResult:
        if type(each[-1]) == type(each):
            count += 1
    return count
'''
conditions = [(table.attr,compareTag,table.attr|value),(...)...]
找出满足条件的记录，输出格式如：
satisfy = [(table1,table2,...),(1,5,...)...]
第一个元组表示
'''

def getSatisfy(conditions):
    firstResult = []
    i = 0
    for attr,compare,para in conditions:
        print attr,compare,para
        if isAttr(para) == True:
            leftSatisfy = rightSatisfy = None
            for tempResult in firstResult:#找出单表的查询结果
                if len(tempResult) > 1:
                    if tempResult[-1] == attr.split(".")[0]:
                        leftSatisfy = tempResult
                    elif tempResult[-1] == para.split(".")[0]:
                        rightSatisfy = tempResult
            joinResult = joinTable(attr,compare,para,leftSatisfy,rightSatisfy)
            firstResult.append(joinResult)
        else:#先处理单表查询
            result = findInTable(attr,compare,para)
            print len(result)
            firstResult.append(result)
    while joinResultNum(firstResult) > 1:
        print
        break
        
    
            
    return firstResult
##def binarySeach(value,candidate):
##    length = len(candidate)
##    high = lenght -1
##    low = 0
##    while low < high:
##        mid = (low + high) / 2
##        if 

def list2set(candidate):
    canSet = set()
    for each in candidate:
        canSet.add(each)
    return canSet
def topK(k,orderAttrs,candidate):
    #candidate = list2set(candidate)
    order = orderAttrs[0][1]
    orderAttr = orderAttrs[0][0]
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
        print 'block',i
        blockAttr = scIndex[i].split(SPLITTAG)
        block = readIndexBlock(fileName,blockAttr[1],blockAttr[2])
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
                    selectedNum += 1
                    print k,selectedNum
                    if selectedNum >= int(k):
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
    #if resultAttr[0] == "*":
    return result

          
    
'''求topN
table=[table1,table2,...]
resultAttr = [table.attr1,table.attr2,...]
conditions的比较条件在上游方法确定，本方法按下标增序依次执行
conditions = [(table.attr,compareTag,table.attr|value),(...)...]
orderAttrs = [table.attr,table.attr...]
首先根据条件选出满足条件的记录，再对记录按指定属性排序，最后投影出输出属性
'''
def selectTopK(k,resultAttr,conditions,orders):
    satisfy = getSatisfy(conditions)
    print 'satisfy',len(satisfy[0])
    if len(satisfy[0]) <= 1:
        print '查询结果为空'
        return
    kItems = topK(k,orders,satisfy[0])
    #kItems = sorted(kItems)
    result = readRecord(resultAttr,kItems)
    print "result",result  
   

def despathSelect(sqlDic):
    tables = sqlDic['FROM']
    select = sqlDic['SELECT']
    conditions = sqlDic['WHERE']
    groups = sqlDic["GROUP"]
    orders = sqlDic['ORDER']
    if "TOP" in select[0]:
        k = select[0][2]
        resultAttr = []
        for each in select:
            resultAttr.append(each[0])
        selectTopK(k,resultAttr,conditions,orders)
    

if __name__ == "__main__":
##    sql = inputSql()
##    sqlDic = parseSql(sql,METADATA)
##    print sqlDic
##    for k,v in sqlDic.items():
##        print k,v
    import time
    t1 = time.time()        
    condition = [['CUSTOMER.C_MKTSEGMENT', '=', 'MACHINERY'],
                 ['ORDERS.O_ORDERDATE', '<', '1995-03-11'],
                 ['CUSTOMER.C_CUSTKEY', '=', 'ORDERS.O_CUSTKEY']]
    getSatisfy(condition)
    print time.time()-t1
##
##    import time
##    t1 = time.time()
##    print t1
##    despathSelect(sqlDic)
##    print time.time(),time.time()-t1

