# -*- coding: utf-8 -*-
import re
from createindex import DATABASE
#import createindex
import os
def getAttrCons(attrsCons):
    attrs = attrsCons.split(",")
    i=0
    length = len(attrs)
    while i < length-1:
        if "(" in attrs[i] and ")" not in attrs[i] and ")" in attrs[i+1] and "(" not in attrs[i+1]:       
            attrs[i] +=","+ attrs[i+1]
            attrs.remove(attrs[i+1])
        i += 1
        length = len(attrs)
    return attrs
def createMeta(sql):
    keywords = ["TABLE","CHAR","INT","VARCHAR","DECIMAL","DATA"]
    sql = sql.replace("table","TABLE").replace("varchar","VARCHAR")\
          .replace("char","CHAR").replace("int","INT")\
          .replace("decimal","DECIMAL").replace("date","DATA")\
          .replace("\n","").replace("\t","")\
          .replace("primary","PRIMARY").replace("key","KEY")
    reg = "TABLE\s*(\w*)\s*\("   
    tableName = re.compile(reg).findall(sql)
    if len(tableName) == 0:
        print 'Create statement must contain "table"'
        return
    tableName = tableName[0]
    metaData = ""
    primary = ""
    reg = "\((.*)\)"
    attrstr = re.compile(reg).findall(sql)
    attrs = getAttrCons(attrstr[0])
    for attr in attrs:
        reg = "PRIMARY\s*KEY.*\((.*)\)+"
        prikey = re.compile(reg).findall(attr)
        #print attr,prikey
        if len(prikey) > 0:
            print prikey
            primary = prikey[0]
        for keyword in keywords:
            if keyword in attr:
                reg = "(\sPRIMARY\s*KEY)"
                prikey = re.compile(reg).findall(attr)
                if len(prikey) > 0:
                    print prikey
                    attr = attr.replace(prikey[0],"")#remone the "primary key" string
                    primary = attr.replace(keyword,"")#把域也删掉，只保留属性
                    print attr,primary
                metaData += "|"+attr
                break
    fileName = os.path.join(DATABASE,"meta.table")
    wf = open(fileName,"a+")
    tableNames = wf.read()
    if tableNames==None:
        tableNames = "\n"
    tableNames = tableName+"|"+tableNames  
    tableNames += "\n"+tableName +"|" +primary+metaData
    #wf.truncate()
    wf.seek(0)
    wf.truncate()
    reg = "\s(\s\s+)"
    attrstr = re.compile(reg).findall(tableNames)
    for each in attrstr:
        tableNames = tableNames.replace(each,"")#删掉多余空格
    wf.write(tableNames)
    wf.flush()
    wf.close()           
    
    
def part():    
    createSql = '''create table PART'''
    attrs = '''(P_PARTKEY int primary key,P_NAME varchar(55),
                P_MFGR char(25),P_BRAND char(10),P_TYPE varchar(25),P_SIZE int,P_CONTAINER char(10),
                P_RETAILPRICE decimal(15,2),P_COMMENT varchar(23))'''    
    createMeta(createSql+attrs)
    
def supplier():
    createSql = " create table SUPPLIER "
    attrs = ''' (S_SUPPKEY int primary key,S_NAME char(25),
                S_ADDRESS  varchar(40),S_NATIONKEY int,S_PHONE char(15),S_ACCTBAL decimal(15,2),
                S_COMMENT varchar(101),foreign key (S_NATIONKEY) references NATION(N_NATIONKEY))'''
    createMeta(createSql+attrs)
def partsupp():
    fileName = "e:/Study/database/project/data/partsupp.tbl"
    createSql = " create table PARTSUPP "
    attrs = ''' (PS_PARTKEY int,PS_SUPPKEY int,PS_AVAILQTY int,PS_SUPPLYCOST decimal(15,2),
                PS_COMMENT varchar(199),foreign key (PS_PARTKEY) references PART(P_PARTKEY),
                foreign key (PS_SUPPKEY) references SUPPLIER(S_SUPPKEY),
                primary key (PS_PARTKEY,PS_SUPPKEY))'''
    createMeta(createSql+attrs)
    
def customer():
    fileName = "e:/Study/database/project/data/customer.tbl"
    createSql = " create table CUSTOMER "
    attrs = ''' (C_CUSTKEY int PRIMARY KEY,C_NAME varchar(25),C_ADDRESS varchar(40),C_NATIONKEY int,
                C_PHONE char(15),C_ACCTBAL decimal(15,2),C_MKTSEGMENT char(10),C_COMMENT varchar(117), 
                foreign key (C_NATIONKEY) references NATION(N_NATIONKEY))'''
    createMeta(createSql+attrs)
def orders():
    fileName = "e:/Study/database/project/data/orders.tbl"
    createSql = " create table ORDERS "
    attrs =  '''(O_ORDERKEY int PRIMARY KEY,O_CUSTKEY int,O_ORDERSTATUS char(1),
            O_TOTALPRICE decimal(15,2),  O_ORDERDATE date,O_ORDERPRIORITY char(15),
            O_CLERK char(15),O_SHIPPRIORITY int, O_COMMENT varchar(79),
            foreign key (O_CUSTKEY) references CUSTOMER(C_CUSTKEY))'''
    createMeta(createSql+attrs)
           
def lineitem():
    fileName = "e:/Study/database/project/data/lineitem.tbl"
    createSql = " create table LINEITEM "
    attrs = ''' (L_ORDERKEY int,L_PARTKEY int,L_SUPPKEY int,L_LINENUMBER int,L_QUANTITY decimal(15,2),
                L_EXTENDEDPRICE decimal(15,2),L_DISCOUNT decimal(15,2),L_TAX decimal(15,2),
                L_RETURNFLAG char(1),L_LINESTATUS char(1),L_SHIPDATE date,L_COMMITDATE date,
                L_RECEIPTDATE date,L_SHIPINSTRUCT char(25),L_SHIPMODE char(10),L_COMMENT varchar(44),                
               foreign key (L_ORDERKEY) references ORDERS(O_ORDERKEY),
               foreign key (L_PARTKEY) references PART(P_PARTKEY),
               primary key(L_ORDERKEY,L_LINENUMBER))'''
    createMeta(createSql+attrs)
def nation():
    fileName = "e:/Study/database/project/data/nation.tbl"
    createSql = ''' create table NATION '''
    attrs = '''(N_NATIONKEY int primary key,N_NAME varchar(25),
                N_REGIONKEY int,N_COMMENT varchar(152),
                foreign key (N_REGIONKEY) references REGION(R_REGIONKEY));'''
    createMeta(createSql+attrs)
    
def region():
    fileName = "e:/Study/database/project/data/region.tbl"
    createSql = " create table REGION "
    attrs = "(R_REGIONKEY int primary key,R_NAME varchar(25),R_COMMENT varchar(152));"
    createMeta(createSql+attrs)

def createMetaData():
    supplier()
    part()
    partsupp()
    customer()
    orders()
    lineitem()
    nation()
    region()
    
if __name__ == "__main__":
    createMetaData()
