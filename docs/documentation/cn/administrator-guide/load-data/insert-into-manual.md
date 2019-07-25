# Insert into

Insert into 主要用来将某几行数据插入某张表，或者将已经存在在 Doris 系统中的部分数据导入到目标表中。

目前所有的 Insert into 导入实现均为同步执行。但是为了兼容旧的 Insert into 导入，不指定 STREAMING 的 Insert into 同步执行完后，依然可以通过查看导入命令查看导入状态和结果。

# 基本操作
## 创建导入

Insert into 创建导入请求需要通过 Mysql 协议提交，创建导入请求会同步返回导入结果。

Insert info 创建导入语句

```
语法：
INSERT INTO table_name [partition_info] [col_list] [plan_hints] [query_stmt] [VALUES] 

示例：
insert into char_4_new values ("qweasdzxcqweasdzxc"), ("a");
insert into char_4_new select * from char_3_new;
```

下面主要介绍创建导入语句中使用到的参数：

+ query\_stmt

	通过一个查询语句，将查询语句的结果导入到 Doris 系统中的其他表。

+ VALUES
	
	用户可以通过 VALUES 语法插入一条或者多条数据。
	
	*注意：VALUES 方式仅适用于导入几条数据作为导入 DEMO 的情况，完全不适用于任何测试和生产环境。Doris 系统本身也不适合单条数据导入的场景。建议使用 Insert into select 的方式进行批量导入。*

+ partition\_info

	导入表的目标分区，如果指定目标分区，则只会导入符合目标分区的数据。如果没有指定，则默认值为这张表的所有分区。所有不属于当前目标分区的数据均属于 ``` num_rows_unselected ```，不参与 ``` max_filter_ratio ``` 的计算。

+ col\_list

	导入表的目标列，可以以任意的顺序存在。如果没有指定目标列，那么默认值是这张表的所有列。如果待表中的某个列没有存在目标列中，那么这个列需要有默认值，否则 INSERT 就会执行失败。

	如果查询语句的结果列类型与目标列的类型不一致，那么会调用隐式类型转化，如果不能够进行转化，那么 INSERT 语句会报语法解析错误。
	
+ plan\_hints

	可指定当前导入的导入方式，目前无论是否指定 hint 导入方式均为同步，用户均可以通过创建导入请求的结果判断导入结果。
	
	唯一不同的是，不指定 STREAMING 的导入方式，由于为了兼容旧的使用习惯，用户依旧可以通过查看导入命令看到导入的结果。但导入实际上已经在创建导入返回结果的时候就已经完成了。
	
## 导入结果

Insert into 的导入结果也就是创建导入命令的返回值。

指定 plan\_hints 为 STREAMING 的 Insert into 导入直接根据 sql 执行的返回码确定导入是否成功，除了 query 返回 OK 以外，其他均为导入失败。

未指定 plan\_hints 的 Insert into 导入如果导入失败，也是返回 sql 执行失败，如果导入成功还会附加返回一个 Label 字段。

+ Label

	导入任务的标识。每个导入任务，都有一个在单 database 内部唯一的 label。Insert into 的 label 则是由系统生成的，用户可以拿着这个 label 通过查询导入命令异步获取导入状态。
	
	*注意：只有不指定 plan\_hints 的 Insert into 才会返回 Label 参数*
	
# Insert into 系统配置
## FE conf 中的配置

+ timeout

	导入任务的超时时间(以秒为单位)，导入任务在设定的 timeout 时间内未完成则会被系统取消，变成 CANCELLED。
	
	目前 Insert into 并不支持自定义导入的 timeout 时间，所有 Insert into 导入的超时时间是统一的，默认的 timeout 时间为1小时。如果导入的源文件无法再规定时间内完成导入，则需要调整 FE 的参数```insert_load_default_timeout_second```。
	
## Session variable 中的配置

+ enable\_insert\_strict

	Insert into 导入本身不能设置 ```max_filter_ratio ```，这个参数用来代替错误率限制的功能，可以通过 ``` session variable ``` 设置，默认值为 false。当该参数设置为 false 时，则和```max_filter_ratio = 1 ``` 意义相同。如果该参数设置为 true 时，则和```max_filter_ratio = 0 ``` 意义相同。
	
# Insert into 最佳实践
## 应用场景
1. 用户希望仅导入几条假数据，验证一下 Doris 系统的功能。此时适合使用 Insert into VALUS 的语法。
2. 用户希望将已经在 Doris 系统中的数据进行 ETL 转换并导入到一个新的 Doris 表中，此时适合使用 Insert into query\_stmt。

除以上两种情况外，其他情况并不适合使用 Insert into

## 数据量
Insert into 对数据量没有限制，大数据量导入也可以支持。但 Insert into 有默认的超时时间，用户预估的导入数据量过大，就需要修改系统的 Insert into 导入超时时间。

```
导入数据量 = 36G 约≤ 3600s * 10M/s 
其中 10M/s 是最大导入限速，用户需要根据当前集群情况计算出平均的导入速度来替换公式中的 10M/s
```

## 完成例子
用户有一张表 store_sales 在数据库 sales 中，用户又创建了一张表叫 bj_store_sales 也在数据库 sales 中，用户希望将 store_sales 中销售记录在 bj 的数据导入到这张新建的表 bj_store_sales 中。导入的数据量约为：10G。

```
store_sales schema：
(id, total, user_id, sale_timestamp, region)

bj_store_sales schema:
(id, total, user_id, sale_timestamp)

```

集群情况：用户当前集群的平均导入速度约为 5M/s

+ Step1: 判断是否要修改 Insert into 的默认超时时间

	```
	计算导入的大概时间
	10G / 5M/s = 2000s
	
	修改 FE 配置
	insert_load_default_timeout_second = 2000
	```
	
+ Step2：创建导入任务

	由于用户是希望将一张表中的数据做 ETL 并导入到目标表中，所以应该使用 Insert into query\_stmt 方式导入。

	```
	insert into bj_store_sales select id, total, user_id, sale_timestamp from store_sales where region = "bj";
	```



