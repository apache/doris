# 建表

**Q:建表报错：Float or double can't be used as a key, use decimal instead**

A:key列不能用float或者double，要用decimal表示小数

**Q:建表报错：Invalid column order. value should be after key**

A:key列在前，value列在后

**Q:varchar最长多长？**

A:最长65533字节(由于前两个字节用于表示长度，因此比65535少两字节)

**Q:比varchar(65533)更长的字符串，用什么存？**

A:业务层切分为多个字段

**Q:是否支持string类型？**

A:palo有char、varchar两种类型用来处理string类型的数据

**Q:可以设置自增列吗？**

A:不支持自增的列

**Q:palo支持map和struct吗？**

A:不支持

**Q:palo支持text类型的字段吗？**

A:不支持，可以考虑使用char和varchar

**Q:palo的表有timezone属性吗**

A:没有

**Q:如何设置表的字符编码为charset=gbk？**

A:Palo只支持utf8编码，对gbk不支持

# 表的属性和信息查看

**Q:如何看一个表占了多少物理存储？**

A:show data，包括所有副本;

**Q:如何查看某个表是行存，还是列存？**

A:show create table，查看建表语句中PROPERTIES中的storage_type字段

**Q:如何把建表语句描述出来?**

A:show create table;

**Q:如何查看已有的rollup表？**

A:desc table_name all;

**Q:如何看一个表有哪些partition？**

A:show partitions from table_name;

# 修改表

**Q:palo可以对已有的表增加列吗？**

A:可以，具体help alter table

**Q:palo删除列报错：old schema is not deleted**

A:旧的schema可能有之前的查询在查，因此需要等待一段时间，等之前的查询完成，删除旧的schema

**Q:palo表怎么修改字段的默认值？**

A:palo不支持修改默认值

**Q:schema change是否可以中止？**

A:可以，具体可以help cancel alter

**Q:如何添加partition?**

A:比如现有分区 [MIN, 2013-01-01)，增加分区 [2013-01-01, 2014-01-01)，使用默认分桶方式

ALTER TABLE example_db.table_name ADD PARTITION p1 VALUES LESS THAN ("2014-01-01");

具体可以help alter table

**Q:sum类型的字段，如果一直增加超出了存储长度？**

A:需要用户修改列类型

**Q:如何修改varchar的长度？**

A:help alter table, 查看schema change相关

**Q:如何取消bloom filter的建立？**

A:cancel alter table column from table_name

**Q:建表时忘记指明value列，能否通过alter将key列修改为value列，并指明聚合类型**

A:不能

**Q:建表后，表中hash分桶的列，以及指定的bucket的数量，还能更改么？**

A:对于单分区表不能更改，多分区表可以在创建新分区的时候重新指定hash的列和bucket数

**Q:可以修改列名吗？**

A:不能

**Q:一张表同时只能创建一个rollup？**

A:是的 

**Q:如何查看rollup的进度？**

A:show alter table rollup

**Q:建了rollup会影响数据的导入吗？**

A:不会

**Q:建了一个rollup表 ，客户端显示执行成功了，但是 desc table_name all 没有显示上卷表**

A:创建rollup是异步的，show alter table rollup 查看进度

**Q:delete报错：Syntax error at:Encountered: WHERE**

A:需要指定partition，具体help delete

**Q:delete from支持like吗？**

A:不支持

**Q:执行delete后，查询效率明显降低**

A:delete执行之后目前是会影响性能，建议少发delete

**Q:delete语句必须指定分区吗?**

A:是的

# 导入

**Q:show load能按照一些条件查询么，能部分匹配么**

A:可以，部分匹配不带%%，不支持NOT LIKE，具体可以help show load

**Q:导入palo后数据条数少了？**

A:1.检查是否聚合了，确认下导入文件有没有重复的key

  2.检查是否有不符合质量要求的数据被过滤，通过show warning查看数据质量不符合的数据

**Q:导入命令返回后发现数据没有导入进去**

A:导入是异步的，通过show load 查看进度

**Q:同一个表，小批量导入方式能并行操作吗？**

A:可以，不同的label就行，etl阶段是并行的，loading阶段是串行的

**Q:使用curl 向palo导入数据的时候 可以自定义字段之间的分隔符吗？**

A:可以，设置column_separator的值为自定义的字段分隔符

**Q:导入报错ETL_QUALITY_UNSATISFIED; msg:quality not good enough to cancel 是什么原因？**

A:数据质量有问题。把show load的 URL字段的文件下载下来，里面有哪一行有什么样的问题的说明。

常见的错误类型有：

(1) convert csv string to INT failed. 导入文件某列的字符串转化对应类型的时候出错，比如"abc"转化为数字。

(2) the length of input is too long than schema. 导入文件某列长度不正确，比如定长字符串超过建表设置的长度、int类型的字段超过4个字节。

(3) actual column number is less than schema column number. 导入文件某一行按照指定的分隔符切分后列数小于指定的列数，可能是分隔符不正确。

(4) actual column number is more than schema column number. 导入文件某一行按照指定的分隔符切分后列数大于指定的列数。

(5) the frac part length longer than schema scale. 导入文件某decimal列的小数部分超过指定的长度。

(6) the int part length longer than schema precision. 导入文件某decimal列的整数部分超过指定的长度。

(7) the length of decimal value is overflow. 导入文件某decimal列的长度超过指定的长度。

(8) there is no corresponding partition for this key. 导入文件某行的分区列的值不在分区范围内。

举例：

有一张表有4列，类型分别是int, int, char(5), decimal(5, 3)。将第一列作为分区列，第一个分区范围小于5，第二个分区范围小于10。

现在有以下8列数据需要导入到该表。

1	abc	1	1.1

2	2	helloworld	2.2

3	3	3.3

4	4	abc	4	4.4

5	5	abc	5.5555

6	6	abc	123456

7	7	abc	123456.2

20	20	abc	20.2

第一行数据报convert csv string to INT failed错误。因为"abc"不能转换为int类型。

第二行数据报the length of input is too long than schema错误。因为"helloworld"的长度超过了5。

第三行数据报actual column number is less than schema column number错误。因为该行只有3列，少于schema的4列。

第四行数据报actual column number is more than schema column number错误。因为该行有5列，多于schema的4列。

第五行数据报the frac part length longer than schema scale错误。因为5.5555的小数部分超过了schema规定的长度。

第六行数据报the int part length longer than schema precision错误。因为123456的整数部分超过了schema规定的长度。

第七行数据报the length of decimal value is overflow错误。因为123456.2长度是8字节，超过了schema规定的长度。

第八行数据报there is no corresponding partition for this key错误。因为20不在分区范围内。

**Q:文件中一个6.56 导入palo中double型的变成6.5600000000000005，这种要怎么解决才能一样呢？**

A:用decimal类型

**Q:能把一个palo表里的数据导入另一个表吗？**

A:Insert into Table2(field1,field2,...) select value1,value2,... from Table1 where condition;

  使用时可以通过condition限制一次的数据量，保证单批次控制在10G以内

**Q:palo可以用\x01作为分隔符吧？**

A:可以，具体help load，需要转义

**Q:show load展示结果太多了**

A:通过limit限制，如show load order by jobid desc limit 10;

**Q:通过load或者curl导入palo时，导入文件中的列数大于palo内表的列数，怎么解决？**

A:在columns参数中指定表和数据中列的对应关系，并且在需要略过的列的位置指定不存在的列名

**Q:以前向palo表里load很多次数据，为什么刚刚用show load发现是空？**

A:超过7天的历史数据会被清空

# 查询

**Q:查询报错：memory limit exceeded**

A:为了减少各用户相互影响，对查询使用的内存有限制，默认为落在单个节点上使用的内存不能超过2GB，因此需要用户尽量优

化表结构和查询，如果优化后还报这个错通过show variables，查看单个be节点执行查询的内存限制exec_mem_limit的

值，然后通过命令set exec_mem_limit = new_size提高限制，new_size为新内存的大小，单位为字节，set是

session级别的，每次重连则需要重新设置

**Q:palo索引机制是怎样的？ 需要用户建立吗？ 还是自动根据查询 cache一些数据？**

A:palo数据是按全key排序的（建表时指定），这样有些场景下可以进行快速定位

  另外也可以针对一些场景创建物化视图（rollup）

  palo当前没有Cache

**Q:查询报错：Reach limit of connections**

A:当前单台FE单个用户的最大连接数默认为100，总连接数限制默认为1024，建议使用连接池并且大小不要超过这个限制，

如果需要提高限制，通过在fe的配置文件里配置总限qe_max_connection或者单用户限制max_conn_per_user

**Q:palo是否支持json解析函数？**

A:palo支持json解析函数，提供了3个json解析函数，分别是get_json_int（string，string）、get_json_string（string，string）和get_json_double（string，string）。第一个参数为json字符串，第二个参数为json内的路径。

**Q:查询超时**

A:查询超时默认是5分钟，对于多数在线的分析和报表查询我们认为这个限制足够，如果查询超时首先优化查询语句，如果依旧超

时，则通过show variables查看query_timeout变量的值，并通过set query_timeout = new_value提高超时限制，

new_value为新的超时时间，单位为秒，set为session级的修改，每次重连则需要重新设置

**Q:访问palo的视图，可以命中基表的rollup吗？**

A:由create view时的as select决定的，具体可以通过执行explain select ....来看有没有命中rollup

**Q:是否可以通过palo查询mysql？**

A:可以通过palo查询mysql库，palo本身不存储mysql的数据，也不能使用palo往mysql中导入数据

# 其它

**Q:报错：Sql parsing error, check your sql.**

A:可能有非法字符，比如不能识别的不可见字符

**Q:Palo是否支持存储过程？**

A:目前不支持
