
该项目用python实现了一个简单列式数据库，实现了多表join和top N查询
详细设计可以参考：http://www.cnblogs.com/fengfenggirl/p/minit_db.html
数据在百度网盘：http://pan.baidu.com/share/link?shareid=361303219&uk=3271689851

1、下载数据文件后，将代码放在数据文件的上层目录；
2、修改createindex.py中的DATABASE = "minidb"改为存放数据文件的文件目录
3、运行createtable.py，建立元数据文件meta.table
4、运行createindex.py，将对所有的表建立索引
5、运行join_topk.py，执行sql:
	top N:
	select top 20 * 
	from ORDERS 
	where o_orderdate< "1995-05-20"	
	order by o_totalprice;
	
	多表连接：
	select
	l_orderkey,
	o_orderdate,
	o_shippriority
	from
	customer,
	orders,
	lineitem
	where	
	c_mktsegment = 'MACHINERY'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < "1995-05-20"
	and l_shipdate > "1995-05-18";
	
group by 没有实现。



