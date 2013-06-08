# -*- coding: utf-8 -*-

form createindex import DATABASE

'''求topN
table=[table1,table2,...]
resultAttr = [table.attr1,table.attr2,...]
conditions的比较条件在上游方法确定，本方法按下标增序依次执行
conditions = [(table.attr,compareTag,table.attr|value),(...)...]
orderAttrs = [table.attr,table.attr...]
'''
def selectTopK(tables,resultAttr,conditions,orderAttrs):
    
