# 目标
支持Spark ETL导入流程中的全局字典构建与编码。

# 主要思路
通过新增一个spark job，执行spark sql，完成全局字典的构建与编码。
如果当前的doris olapTable不包含精确去重字段，那么可以跳过全局字典的构建与编码，直接产出一张hive表，作为接下来spark etl流程的输入。
以下是全局字典构建与编码job的详细设计，下文中均称为```当前job```

# 作业输入
1. 作业所需的所有上下文信息
	* 上游hive表名称,String
	* 精确去重字段列表,List<String>
	* Doris olapTable字段列表，需要能和hive表中的字段对应上，List<String>
	* Doris olapTable名称
	* 用户指定filter，sql查询上游表时的过滤条件，String
	* Doris Job Id,long
	* 函数列，必须是spark支持的函数，列名需要和hive表中的列保持一致，List<String>
		* 长度与doris表长度一致，通过数组下标确定字段到函数的对应关系
	* 表达式计算列，必须是spark支持的表达式，表达式中的列需要和hive表中的列保持一致，List<String>
	* doris hive库，所有构建流程中要用到的hive表都在当前库中创建,String
	* 以上信息，可以通过提交spark作业时，以string的形式从main方法中传入参数

2. 上游hive表
	doris账户具备读权限即可

# 作业输出
1. 编码过的hive表
	* 所有字段类型均为string
	* 如果包含精确去重字段，精确去重字段实际存储值为bigint
	* 该表主要用于下一步spark预计算时的输入
	* 该表名称的格式为“固定前缀+doris表名+dorisjobid”
	* 完成表达式计算
	* 完成spark函数计算
	* 完成用户指定条件的过滤

# 功能点支持与实现
1. 支持对于HDFS等文件系统存储数据的数据读取
	* hive本身已经实现了指定数据源以及文件读取格式的功能，因此对于用户文件的导入可以通过新建外部表的方式
	* 关于访问hive metastore的问题
		* 目前sparksql是包含访问hive表的接口的，例如获取表结构。
		* 使用时只需要在提交spark作业时上传一个包含hive metastore地址的hive-site.xml即可
		* 另外还需要上传hive metastore依赖的jar，spark默认不携带这些jar包
		* 如果公司内部版本的spark客户端在提交作业时会自动携带hive包，Doris提交作业时无需指定jar包位置
2. 支持对于导入列的函数计算
	* 函数可以分为两类
		* 一类是spark支持的函数，针对这类函数，可以在```当前job```中通过sparksql完成
		* 另一类是doris独有的，这类需要在spark预计算的job中完成
3. 支持对于用户自定义表达式计算列的导入，例如期望导入c列，但是该列由a+b列计算出来
	* 可以将表达式嵌入sparksql，在```当前job```中完成
4. 支持多个hivemetastore
	* 本次开发暂不实现
5. 字段映射，hive表如何对应到doris的表
	* 目前设计为创建doris的hive表时，字段名称与上游hive表一致即可
	* 在提交```当前job```时就无需再指定映射
6. hadoop账户的权限问题
	* 目前仅支持使用一个doris的hadoop账户提交spark作业
	* 如果是要读取业务方的数据，需要为doris的账户提供hadoop的读权限
7. 失败重试
	* 在现有的设计中，重试的粒度为```当前job```
	* 也就是说```当前job```的任意步骤失败了，都需要从头开始重跑。
	* 这样设计的好处是开发成本低，可以快速落地。缺点就是失败重试的成本会比较高。
	* 暂时可以先按整个job粒度的重试。
	* 后续可以考虑支持按照更细粒度步骤的重试，降低重试的时间成本和资源成本。
	* 例如```当前job```每完成一步，就写一次hdfs，重试时可以从上次失败的地方继续等。
8. 支持对于全局字典的并发访问
	* 当有同一个olaptable的多个spark job同时运行时，可能会对全局字典的写入产生竞争，因此需要加锁
	* 比较直接的思路是，读最新的版本，不加锁。写入时需要加锁，加锁之后建一个副本，写完成后再将该副本更新为最新的版本。
	* 加锁可以是通过fe接口调用加锁。
	* 并发访问问题现阶段暂时不实现。
	* 现阶段可以先通过fe控制当前步骤的串行执行实现。

# 全局字典hive表结构设计
1. distinct_column_group_by(dict_key string) partition by (dict_column string)
	* dict_key，主要保存了字段去重后的值，来源是上游hive表
	* dict_column，保存了去重字段的名称，在多个去重字段的去重值在同一个hive表的前提下，帮助快速访问到需要的分区数据
2. global_dict_hive_table(dict_key string, dict_value bigint)partition by (dict_column string)
	* 该表主要用于保存全局字典
	* dict_key是原始值
	* dict_value是编码后的值
3. hive_intermediate_table
	* 所有字段和doris保持一致
	* 所有字段类型为string，都存成string是为了全局字典编码时比较方便
	* 对于精确去重字段，实际值保存的值是bigint

# 作业流程
1. 提交一个spark作业，以下流程均在这个spark作业中完成
2. 通过spark sql切换到doris的hive db
3. 通过spark sql创建hive_intermediate_table
4. 通过insert overwrite select将数据写入从上游hive表写入到hive_intermediate_table
	* 在select sql中，替换函数计算列，替换表达式计算列
	* where条件添加用户指定的谓词过滤
	* 如果不需要精确去重计算，那么在这一步结束即可，不需要接下来的步骤
5. 从表hive_intermediate_table中读取精确去重字段的值，做group by完成去重，将结果写入表distinct_column_group_by
	* 上述过程可以通过一个sql完成
	* 如果有多个精确去重字段，那么上述sql需要执行多次
6. 构建全局字典
	1. 通过sql查询global_dict_hive_table，获得当前字段的最大字典编码值，记为max_dict_value
		* 如果是第一次导入，那么该值为0
	2. 通过一个sql完成全局字典的编码，该sql逻辑如下
``` insert global_dict_hive_table select
(从global_dict_hive_table读取历史去重值) t1
union all
select dict_key,row_number() over(dict_key) + max_dict_value from
(
读取distinct_column_group_by
left join global_dict_hive_table
在join的结果集中过滤掉已存在于distinct_column_group_by中的dict_key，只保留新增的dict_key
) t2 ```
	3. 如果精确去重字段存在多个，前两步需要执行多次
7. 对hive_intermediate_table表进行编码
	1. 读取hive_intermediate_table的值并与global_dict_hive_table做join，去重字段的值替换为编码后的值
	2. 将第1步的结果写入到hive_intermediate_table中
	3. 如果存在多个精确去重字段，那么前两步需要执行多次

方案分析
1. 方案优点是，实现简单，数据结构也比较简单，可以快速落地。
2. 方案缺点是资源开销可能会比较高，判断依据是join次数会比较多
3. 该方案适用于资源不是很敏感但是性能要求比较高的场景，因此为了配合快速落地可以先如此实现。
	* 后续可以考虑添加资源使用敏感但是瓶颈相对比较明显的方案
		* 比如基于trie树的全局字典的实现，通过三次shuffle完成构建与编码
	* 或者对现有方案（基于hive表的全局字典构建）进行改良，尝试降低资源开销

# 参考
[spark开发文档](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
[kylin使用hive构建全局字典](http://kylin.apache.org/docs30/howto/howto_use_hive_mr_dict.html)
