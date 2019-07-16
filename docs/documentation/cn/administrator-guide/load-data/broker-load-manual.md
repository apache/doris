# Broker Load
Broker load 是一个异步的导入方式，主要支持的数据源有 HDFS, BOS, AFS。用户需要通过 Mysql client 创建 Broker load 导入，并通过查看导入命令检查导入结果。

Broker load 主要适用于源数据在文件系统中的，或者是超大文件导入的需求。

# 基本原理

用户在提交导入任务后，FE 会生成对应的 plan 并根据目前 BE 的个数和文件的大小，将 plan 分给 多个 BE 执行，每个 BE 执行一部分导入数据。

BE 在执行的过程中会从 broker 拉取数据，在对数据 transform 之后将数据导入系统。所有 BE 均完成导入，并且事务被 publish 后，导入完成。

```
                 +
                 | user create broker load
                 v
            +----+----+
            |         |
            |   FE    |
            |         |
            +----+----+
                 |
                 |    BE etl and load the data
    +--------------------------+
    |            |             |
+---v---+     +--v----+    +---v---+
|       |     |       |    |       |
|  BE   |     |  BE   |    |   BE  |
|       |     |       |    |       |
+---+---+     +---+---+    +----+--+
    |             |             |
    |             |             | pull data from broker
+---v---+     +---v---+    +----v--+
|       |     |       |    |       |
|Broker |     |Broker |    |Broker |
|       |     |       |    |       |
+-------+--+  +---+---+    ++------+
           |      |         |
           |      |         |
         +-v------v---------v--+
         |HDFS/BOS/AFS cluster |
         |                     |
         +---------------------+

```

# 基本配置
+ min\_bytes\_per\_broker\_scanner，max\_bytes\_per\_broker\_scanner 和 max\_broker\_concurrency
		
	前两个配置限制了单个 BE 处理的数据量的最小和最大值。第三个配置限制了最大的导入并发数。最小处理的数据量，最大并发数，源文件的大小和当前集群 BE 的个数共同决定了本次导入的**并发数**。
	
	```本次导入并发数 = Math.min(源文件大小/最小处理量，最大并发数，当前BE节点个数)```
	```本次导入单个BE的处理量 = 源文件大小/本次导入的并发数```
	
	如果，``` 本次导入单个BE的处理量 > 最大处理的数据量```则导入任务会被取消。一般来说，导入的文件过大可能导致这个问题，通过上述公式，预先计算出单个 BE 的预计处理量并调整最大处理量即可解决问题。
	
	```
	默认配置：
	min_bytes_per_broker_scanner， default 64MB，单位bytes
	max_broker_concurrency， default 10
	max_bytes_per_broker_scanner，default 3G，单位bytes
	
	```	

# 基本操作
## 创建导入
创建导入的详细语法执行 ``` HELP BROKER LOAD；``` 查看语法帮助。各个导入通用的属性在上级 LOAD 文档中已经介绍，这里主要介绍 Broker load 的特有属性和注意事项。

### 多表导入
Broker load 支持一次导入任务涉及多张表，每张表可以声明各自的源数据文件地址和数据变换函数。Broker load 保证了单次导入的多张表之间原子性成功或失败。

### is negative
Broker load 还支持数据取反导入。这个功能主要用于，当用户上次导入已经成功，但是想取消上次导入的数据时，就可以使用相同的源文件并设置导入 nagetive 取反功能。上次导入的数据就可以被撤销了。

该功能仅对聚合类型为 SUM 的列有效。聚合列数值在取反导入后会根据待导入数据值，不断递减。

### strict mode
Broker load 导入可以开启 strict mode模式。开启方式为 ```properties ("strict_mode" = "true")``` 。默认的 strict mode为开启。

*注意：strict mode 功能仅在新版本的broker load中有效，如果导入明确指定 ```"version" = "v1"``` 则没有此功能。*

strict mode模式的意思是：对于导入过程中的列类型转换进行严格过滤。严格过滤的策略如下：

1. 对于列类型转换来说，如果 strict\_mode 为true，则错误的数据将被 filter。这里的错误数据是指：原始数据并不为空值，在参与列类型转换后结果为空值的这一类数据。

2. 对于导入的某列包含函数变换的，导入的值和函数的结果一致，strict 对其不产生影响。（其中 strftime 等 Broker load 支持的函数也属于这类）。

3. 对于导入的某列类型包含范围限制的，如果原始数据能正常通过类型转换，但无法通过范围限制的，strict 对其也不产生影响。
	+ 例如：如果类型是 decimal(1,0), 原始数据为10，则属于可以通过类型转换但不在列声明的范围内。这种数据 strict 对其不产生影响。

### strict mode 与 source data 的导入关系

这里以列类型为 TinyInt 来举例

注：当表中的列允许导入空值时

source data | source data example | string to int   | strict_mode        | load_data
------------|---------------------|-----------------|--------------------|---------
空值        | \N                  | N/A             | true or false      | NULL
not null    | aaa or 2000         | NULL            | true               | filtered
not null    | aaa                 | NULL            | false              | NULL
not null    | 1                   | 1               | true or false      | correct data

这里以列类型为 Decimal(1,0) 举例
 
注：当表中的列允许导入空值时

source data | source data example | string to int   | strict_mode        | load_data
------------|---------------------|-----------------|--------------------|---------
空值        | \N                  | N/A             | true or false      | NULL
not null    | aaa                 | NULL            | true               | filtered
not null    | aaa                 | NULL            | false              | NULL
not null    | 1 or 10             | 1               | true or false      | correct data

*注意：10 虽然是一个超过范围的值，但是因为其类型符合 decimal的要求，所以 strict mode对其不产生影响。10 最后会在其他 ETL 处理流程中被过滤。但不会被 strict mode过滤。*

## 查看导入
查看导入命令帮助可执行 ``` HELP SHOW LOAD ```。查看导入属性在上级 LOAD 文档中已经介绍。

## 取消导入
取消导入命令可执行 ``` HELP CANCEL LOAD ```. 取消导入属性在上级 LOAD 文档中已经介绍。