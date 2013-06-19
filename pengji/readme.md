				数据库项目描述文档
				
1.2 SQL 
	本课程设计要求学生设计的数据库系统能执行以下的sql语句，并在尽可能短的时间内输出最终结果。
1.2.1 范围查询以及聚集优化
	查询功能定义：
		select
			l_returnflag,
			l_linestatus,
			sum(l_quantity) as sum_qty,
			sum(l_extendedprice) as sum_base_price,
			avg(l_quantity) as avg_qty,
			avg(l_extendedprice) as avg_price,
			avg(l_discount) as avg_disc,
			count(*) as count_order
		from
			lineitem
		where
			l_shipdate <= date '1998-12-01' - '[DELTA]' day 
		group by
			l_returnflag,
			l_linestatus
		order by
			l_returnflag,
			l_linestatus;
	 
	价格摘要报告查询提供了给定日期的运送的所有行的价格摘要报告，这个日期在数据库包含的最大的运送日期的60－120天以内。查询列出了扩展价格、打折的扩展价格、平均数量、平均扩展价格和平均折扣的总和。这些统计值根据RETURNFLAG 和LINESTATUS进行分组，并按照RETURNFLAG 和LINESTATUS的升序排列。每一组都给出所包含的行数。
	替换参数：
	替换参数（这里是[DELTA]）的值必须被产生以用来形成可执行查询文本：	DELTA在区间[60, 120]内随机选择。
	
1.2.2 Join操作优化
	查询功能定义：
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
			l_orderkey desc,
			o_orderdate;

	替换参数：
	日期（[DATE]）在[1995-03-01, 1995-03-31]中随机选择。
	[SEGMENT]在 [AUTOMOBILE，BUILDING，FURNITURE，MACHINERY，HOUSEHOLD]中随机选择	



1.2.3	TopN SQL优化
	查询功能定义：
	select
		top n * 
	from 
		ORDERS 
	where 
		o_orderdate< date '[DATE]'
	order by 
		o_totalprice

	替换参数：
	日期（[DATE]）在[1995-03-01, 1995-03-31]中随机选择。

2 检验标准
		课程设计的评分标准划分为5块 
	1.	simpleDB的存储系统（有压缩算法额外加分）        20%
	2.	Join 的处理                                      20%
	3.	范围查询问题                                    20%
	4.	top N问题                                       20%
	5.	代码规范+文档描述                               20%

Appendix
	A数据类型定义
	下列数据类型将应用于每个表的列的清单中：
	•Identifier
	注释：一个默认的数据类型是整形。然而在SF大于300时，一些值将超出所支持的4个字节的整数。用户必须使用一些其他的数据类型比如8个字节的整数、小数或者字符串类型来实现。
	•Integer意思是必须为整数（比如值的增长为1），取值范围为-2,147,483,646到2,147,483,647。
	•Decimal意思是必须能够描述取值范围从-9,999,999,999.99到+9,999,999,999.99内、数值增长为0.01的所有有理数。
	•Big Decimal是扩展的Decimal数据类型，它具有的附加特性是它必须足够大以至于能够描述存放在临时表中创建的查询变量的总数。
	•Fixed Text，size N是用来存储一个固定长度为N的字符串类型。
	注释：如果字符串本身小于长度N，那么剩余的空间必须被存储在数据库中，或者数据库自动加上一些空间使得CHAR_LENGTH()函数的返回值为N。
	•Variable text, size N是该列可以存储变量长度最长为N的字符串变量。被定义为“Variable text, size N”的列可以和定义为“fixed text, size N”一样执行。
	•Date是一个可以被描述为YYYY-MM-DD的值，它所有的字符均为数字。一个日期必须能够描述连续的14年里的每一天，但是对日期的内部描述没有特殊的要求。
	B 表的规划
	以下的列表定义了每个表所需要的结构（列的清单）。主键的注释及外键引用仅仅只是为了说明，而不是指定实现要求，例如完整性约束。
	PART表的规划
	列名                  数据类型需求               注释
	P_PARTKEY             identifier                 SF*200,000
	P_NAME                variable text, size 55
	P_MFGR                fixed text, size 25
	P_BRAND               fixed text, size 10
	P_TYPE                variable text, size 25
	P_SIZE                integer
	P_CONTAINER           fixed text, size 10
	P_RETAILPRICE         decimal
	P_COMMENT             variable text, size 23
	主键: P_PARTKEY

	SUPPLIER表的规划：
	列名                  数据类型需求               注释
	S_SUPPKEY             identifier                 SF*10,000
	S_NAME                fixed text, size 25
	S_ADDRESS             variable text, size 40
	S_NATIONKEY           identifier                 外键引用N_NATIONKEY
	S_PHONE               fixed text, size 15
	S_ACCTBAL             decimal
	S_COMMENT             variable text, size 101
	主键: S_SUPPKEY

	PARTSUPP表的规划
	列名                  数据类型需求               注释
	PS_PARTKEY            identifier                 外键引用P_PARTKEY
	PS_SUPPKEY            identifier                 外键引用S_SUPPKEY
	PS_AVAILQTY           integer
	PS_SUPPLYCOST         decimal
	PS_COMMENT            variable text, size 199
	主键: PS_PARTKEY, PS_SUPPKEY

	CUSTOMER表的规划
	列名                  数据类型需求               注释
	C_CUSTKEY             identifier                 SF*150,000
	C_NAME                variable text, size 25
	C_ADDRESS             variable text, size 40
	C_NATIONKEY           identifier                 外码参照N_NATIONKEY
	C_PHONE               fixed text, size 15
	C_ACCTBAL             decimal
	C_MKTSEGMENT          fixed text, size 10
	C_COMMENT             variable text, size 117
	主键: C_CUSTKEY

	ORDERS表的规划
	列名                  数据类型需求               注释
	O_ORDERKEY           identifier                  少量的计算SF*1,500,000 
	O_CUSTKEY            identifier                  外键引用C_CUSTKEY
	O_ORDERSTATUS        fixed text, size 1
	O_TOTALPRICE         decimal
	O_ORDERDATE          date
	O_ORDERPRIORITY      fixed text, size 15
	O_CLERK              fixed text, size 15
	O_SHIPPRIORITY       integer
	O_COMMENT            variable text, size 79
	主键: O_ORDERKEY
	注释：并不是所有的顾客都会有订单。事实上，数据库中大约1/3的用户不会有任何订单。这些订单随机的分配给2/3的用户。
	LINEITEM表的规划
	列名                  数据类型需求               注释
	L_ORDERKEY           identifier                  外键引用O_ORDERKEY
	L_PARTKEY            identifier                  外键引用P_PARTKEY，
	和L_SUPPKEY混合引用(PS_PARTKEY, PS_SUPPKEY)
	L_SUPPKEY            identifier                  外键引用S_SUPPKEY,
	和L_PARTKEY混合引用外键(PS_PARTKEY, PS_SUPPKEY)
	L_LINENUMBER         integer
	L_QUANTITY           decimal
	L_EXTENDEDPRICE      decimal
	L_DISCOUNT           decimal
	L_TAX                decimal
	L_RETURNFLAG         fixed text, size 1
	L_LINESTATUS         fixed text, size 1
	L_SHIPDATE           date
	L_COMMITDATE         date
	L_RECEIPTDATE        date
	L_SHIPINSTRUCT       fixed text, size 25
	L_SHIPMODE           fixed text, size 10
	L_COMMENT            variable text size 44
	混合的主键: L_ORDERKEY, L_LINENUMBER

	NATION表的规划：
	列名                 数据类型需求               注释
	N_NATIONKEY          identifier                 组合25 nations
	N_NAME               fixed text, size 25
	N_REGIONKEY          identifier                 外键引用R_REGIONKEY
	N_COMMENT            variable text, size 152
	主键: N_NATIONKEY

	REGION表的规划：
	列名                 数据类型需求               注释
	R_REGIONKEY          identifier                 组装5 regions
	R_NAME               fixed text, size 25
	R_COMMENT            variable text, size 152
	主键: R_REGIONKEY
