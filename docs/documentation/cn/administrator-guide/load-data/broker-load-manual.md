# Broker Load
Broker load 是一个异步的导入方式，主要支持的数据源有 HDFS, BOS, AFS。用户需要通过 Mysql client 创建 Broker load 导入，并通过查看导入命令检查导入结果。

Broker load 主要适用于源数据在文件系统中的，或者是超大文件导入的需求。

# 名词解释
1. HDFS: Hadoop 分布式文件系统。用户的待导入数据可能存放的位置。
2. BOS：百度云提供的对象存储系统，百度云上的 Palo 用户的待导入数据可能存放的位置。
3. AFS：百度内部提供的超大规模文件系统，百度厂内的 Palo 用户的待导入数据可能存放的位置。
4. plan：导入执行计划，BE 会执行导入执行计划将数据导入到 Doris 系统中。
5. Broker：Doris 系统中负责读取外部文件系统数据的组件。
6. backend（BE）：Doris 系统的计算和存储节点。在导入流程中主要负责数据的 ETL 和存储。
7. frontend（FE）：Doris 系统的元数据和调度节点。在导入流程中主要负责导入 plan 生成和导入任务的调度工作。 

# 基本原理

用户在提交导入任务后，FE 会生成对应的 plan 并根据目前 BE 的个数和文件的大小，将 plan 分给 多个 BE 执行，每个 BE 执行一部分导入数据。

BE 在执行的过程中会从 Broker 拉取数据，在对数据 transform 之后将数据导入系统。所有 BE 均完成导入，并且事务被 publish 后，导入完成。

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

# 基本操作
## 创建导入

Broker load 创建导入语句

```
语法
LOAD LABEL load_label 
	(data_desc, ...)
	with broker broker_name broker_properties
	[PROPERTIES (key1=value1, ... )]

LABEL: db_name.label_name

data_desc:
	DATA INFILE ('file_path', ...)
	[NEGATIVE]
	INTO TABLE tbl_name
	[PARTITION (p1, p2)]
	[COLUMNS TERMINATED BY separator ]
	[(col1, ...)]
	[SET (k1=f1(xx), k2=f2(xx))]

broker_name: string

broker_properties: (key1=value1, ...)

示例：
load label test.int_table_01
	(DATA INFILE("hdfs://abc.com:8888/user/palo/test/ml/id_name") into table id_name_table columns terminated by "," (tmp_c1,tmp_c2) set (id=tmp_c2, name=tmp_c1))  
	with broker 'broker' ("username"="root", "password"="3trqDWfl") 
	properties ("timeout" = "10000" );

```

创建导入的详细语法执行 ``` HELP BROKER LOAD；``` 查看语法帮助。这里主要介绍 Broker load 的创建导入语法中参数意义和注意事项。

+ label

	导入任务的标识。每个导入任务，都有一个在单 database 内部唯一的 label。label 是用户在导入命令中自定义的名称。通过这个 label，用户可以查看对应导入任务的执行情况。
	
	label 的另一个作用，是防止用户重复导入相同的数据。**强烈推荐用户同一批次数据使用相同的label。这样同一批次数据的重复请求只会被接受一次，保证了 At most once**
	
	当 label 对应的导入作业状态为 CANCELLED 时，该 label 可以再次被使用。

### 数据描述类参数

数据描述类参数主要指的是 Broker load 创建导入语句中的属于 ```data_desc``` 部分的参数。每组 ```data_desc ``` 主要表述了本次导入涉及到的数据源地址，ETL 函数，目标表及分区等信息。

下面主要对数据描述类的部分参数详细解释：

+  多表导入

	Broker load 支持一次导入任务涉及多张表，每个 Broker load 导入任务可在多个 ``` data_desc ``` 声明多张表来实现多表导入。每个单独的 ```data_desc``` 还可以指定属于该表的数据源地址。Broker load 保证了单次导入的多张表之间原子性成功或失败。

+  negative

	```data_desc```中还可以设置数据取反导入。这个功能主要用于，当用户上次导入已经成功，但是想取消上次导入的数据时，就可以使用相同的源文件并设置导入 nagetive 取反功能。上次导入的数据就可以被撤销了。

	该功能仅对聚合类型为 SUM 的列有效。聚合列数值在取反导入后会根据待导入数据值，不断递减。
	
+ Partition

	在 ``` data_desc ``` 中可以指定待导入表的 Partition 信息，如果待导入数据不属于指定的 Partition 则不会被导入。这些数据将计入 ```dpp.abnorm.ALL ```
	
### 导入任务参数

导入任务参数主要指的是 Broker load 创建导入语句中的属于 ```opt_properties```部分你的参数。导入任务参数是作用于整个导入任务的。

下面主要对导入任务参数的部分参数详细解释：

+ timeout
	
	导入任务的超时时间(以秒为单位)，用户可以在```opt_properties```中自行设置每个导入的超时时间。导入任务在设定的 timeout 时间内未完成则会被系统取消，变成 CANCELLED。Broker load 的默认导入超时时间为4小时。
	
	通常情况下，用户不需要手动设置导入任务的超时时间。当在默认超时时间内无法完成导入时，可以手动设置任务的超时时间。
	
	**推荐超时时间**
	
	``` 
	总文件大小（MB） / 用户 Doris 集群最慢导入速度(MB/s)  > timeout > （（总文件大小(MB) * 待导入的表及相关 Roll up 表的个数） / (10 * 导入并发数） ）
	
	导入并发数见文档最后的导入系统配置说明，公式中的 10 为目前的导入限速 10MB/s。
	
	``` 
	
	例如一个 1G 的待导入数据，待导入表涉及了3个 Roll up 表，当前的导入并发数为 3。则 timeout 的 最小值为 ```(1 * 1024 * 3 ) / (10 * 3) = 102 秒```
	
	由于每个 Doris 集群的机器环境不同且集群并发的查询任务也不同，所以用户 Doris 集群的最慢导入速度需要用户自己根据历史的导入任务速度进行推测。
	
	*注意：用户设置导入超时时间最好在上述范围内。如果设置的超时时间过长，可能会导致总导入任务的个数超过 Doris 系统限制，导致后续提交的导入被系统取消*
		
+ max\_filter\_ratio

	导入任务的最大容忍率，默认为0容忍，取值范围是0~1。当导入的 filter ratio 超过该值，则导入失败。计算公式为： ``` (dpp.abnorm.ALL / (dpp.abnorm.ALL + dpp.norm.ALL ) )> max_filter_ratio ```
	
	``` dpp.abnorm.ALL ``` 在代码中也叫 ``` num_rows_filtered``` 指的是导入过程中被过滤的错误数据。比如：列在表中为非空列，但是导入数据为空则为错误数据。可以通过 ``` SHOW LOAD ``` 命令查询导入任务的错误数据量。
	
	``` dpp.norm.ALL ``` 指的是导入过程中正确数据的条数。可以通过 ``` SHOW LOAD ``` 命令查询导入任务的正确数据量。
	
	``` 原始文件的行数 = dpp.abnorm.ALL + dpp.norm.ALL ```
	
+ exec\_mem\_limit

	导入任务的内存使用上限。当导入任务使用的内存超过设定上限时，导入任务会被 CANCEL。系统默认的内存上限为2G。
	
	设定导入任务内存上限，可以保证各个导入任务之间以及导入任务和查询之间相互不影响。导入任务的内存上限不宜过大，过大可能会导致导入占满 be 内存，查询无法快速返回结果。
	
	```导入任务内存使用量 < 文件大小 / 当前执行导入任务的并发数```

+ strict mode

	Broker load 导入可以开启 strict mode模式。开启方式为 ```properties ("strict_mode" = "true")``` 。默认的 strict mode为开启。

	strict mode模式的意思是：对于导入过程中的列类型转换进行严格过滤。严格过滤的策略如下：

	1. 对于列类型转换来说，如果 strict\_mode 为true，则错误的数据将被 filter。这里的错误数据是指：原始数据并不为空值，在参与列类型转换后结果为空值的这一类数据。

	2. 对于导入的某列包含函数变换的，导入的值和函数的结果一致，strict 对其不产生影响。（其中 strftime 等 Broker load 支持的函数也属于这类）。

	3. 对于导入的某列类型包含范围限制的，如果原始数据能正常通过类型转换，但无法通过范围限制的，strict 对其也不产生影响。
		+ 例如：如果类型是 decimal(1,0), 原始数据为10，则属于可以通过类型转换但不在列声明的范围内。这种数据 strict 对其不产生影响。

	#### strict mode 与 source data 的导入关系

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

Broker load 导入方式由于是异步的，所以用户必须将创建导入的 Label 记录，并且在**查看导入命令中使用 Label 来查看导入结果**。查看导入命令在所有导入方式中是通用的，具体语法可执行 ``` HELP SHOW LOAD ```查看。

下面主要介绍了查看导入命令返回结果集中参数意义：

+ JobId

	导入任务的唯一标识，每个导入任务的JobId都不同。与 Label 不同的是，JobId永远不会相同，而 Label 则可以在导入任务失败后被复用。
	
+ Label

	导入任务的标识。
	
+ State

	导入任务当前所处的阶段。在 Broker load 导入过程中主要会出现 PENDING 和 LOADING 这两个导入中的状态。如果 Broker load 处于 PENDING 状态，则说明当前导入任务正在等待被执行， LOADING 状态则表示正在执行中（ transform and loading data）。
	
	导入任务的最终阶段有两个： CANCELLED 和 FINISHED，当 Load job 处于这两个阶段时，导入完成。其中 CANCELLED 为导入失败，FINISHED 为导入成功。
	
+ Progress

	导入任务的进度描述。分为两种进度：ETL 和 LOAD，对应了导入流程的两个阶段 ETL 和 LOADING。目前 Broker load 由于只有 LOADING 阶段，所以 ETL 则会永远显示为 ```N/A ``` 
	
	LOAD 的进度范围为：0~100%。``` LOAD 进度 = 当前完成导入的表个数 / 本次导入任务设计的总表个数 * 100%``` 
	
	**如果所有导入表均完成导入，此时 LOAD 的进度为 99%** 导入进入到最后生效阶段，整个导入完成后，LOAD 的进度才会改为 100%。
	
+ Type

	导入任务的类型。Broker load 的 type 取值只有 BROKER。	
+ EtlInfo

	主要显示了导入的数据量指标 ``` dpp.norm.ALL 和 dpp.abnorm.ALL```。用户可以根据这两个指标验证当前导入任务的 filter ratio 是否超过 max\_filter\_ratio.
	
+ TaskInfo

	主要显示了当前导入任务参数，也就是创建 Broker load 导入任务时用户指定的导入任务参数，包括：cluster，timeout和max\_filter\_ratio。
	
+ ErrorMsg

	在导入任务状态为CANCELLED，会显示失败的原因，显示分两部分：type和msg，如果导入任务成功则显示 ```N/A```。
	
	```
	type的取值意义
	USER_CANCEL: 用户取消的任务
	ETL_RUN_FAIL：在ETL阶段失败的导入任务
	ETL_QUALITY_UNSATISFIED：数据质量不合格，也就是错误数据率超过了 max_filter_ratio
	LOAD_RUN_FAIL：在LOADING阶段失败的导入任务
	TIMEOUT：导入任务没在超时时间内完成
	UNKNOWN：未知的导入错误

	```
		

+ CreateTime/EtlStartTime/EtlFinishTime/LoadStartTime/LoadFinishTime

	这几个值分别代表导入创建的时间，ETL阶段开始的时间，ETL阶段完成的时间，Loading阶段开始的时间和整个导入任务完成的时间。
	
	Broker load 导入由于没有 ETL 阶段，所以其 EtlStartTime, EtlFinishTime, LoadStartTime 被设置为同一个值。
	
	导入任务长时间停留在CreateTime，而 LoadStartTime 为 N/A 则说明目前导入任务堆积严重。用户可减少导入提交的频率。
	
	```
	LoadFinishTime - CreateTime = 整个导入任务所消耗时间
	LoadFinishTime - LoadStartTime = 整个 Broker load 导入任务执行时间 = 整个导入任务所消耗时间 - 导入任务等待的时间
	```
	
+ URL

	导入任务的错误数据样例，访问 URL 地址既可获取本次导入的错误数据样例。当本次导入不存在错误数据时，URL 字段则为 N/A。

## 取消导入

Broker load 可以被用户手动取消，取消时需要指定待取消导入任务的 Label 。取消导入命令语法可执行 ``` HELP CANCEL LOAD ```查看。

# Broker load 导入系统配置
## FE conf 中的配置

下面几个配置属于 Broker load 的系统级别配置，也就是作用于所有 Broker load 导入任务的配置。主要通过修改 ``` fe.conf```来调整配置值。

+ min\_bytes\_per\_broker\_scanner，max\_bytes\_per\_broker\_scanner 和 max\_broker\_concurrency
		
	前两个配置限制了单个 BE 处理的数据量的最小和最大值。第三个配置限制了最大的导入并发数。最小处理的数据量，最大并发数，源文件的大小和当前集群 BE 的个数**共同决定了本次导入的并发数**。
	
	```本次导入并发数 = Math.min(源文件大小/最小处理量，最大并发数，当前BE节点个数)```
	```本次导入单个BE的处理量 = 源文件大小/本次导入的并发数```
	
	如果，``` 本次导入单个BE的处理量 > 最大处理的数据量```则导入任务会被取消。一般来说，导入的文件过大可能导致这个问题，通过上述公式，预先计算出单个 BE 的预计处理量并调整最大处理量即可解决问题。
	
	```
	默认配置：
	参数名：min_bytes_per_broker_scanner， default 64MB，单位bytes
	参数名：max_broker_concurrency， default 10
	参数名：max_bytes_per_broker_scanner，default 3G，单位bytes
	
	```	
	
# Broker load 最佳实践

## 应用场景
使用 Broker load 最适合的场景就是原始数据在文件系统（HDFS，BOS，AFS）中的场景。其次，由于 Broker load 是单次导入中唯一的一种异步导入的方式，所以如果用户在导入大文件中，需要使用异步接入，也可以考虑使用 Broker load。

## 数据量

这里仅讨论单个 BE 的情况，如果用户集群有多个 BE 则下面标题中的数据量应该乘以 BE 个数来计算。比如：如果用户有3个 BE，则 3G 以下（包含）则应该乘以 3，也就是 9G 以下（包含）。

+ 3G 以下（包含）

	用户可以直接提交 Broker load 创建导入请求。
	
+ 3G 以上

	由于单个导入 BE 最大的处理量为 3G，超过 3G 的待导入文件就需要通过调整 Broker load 的导入参数来实现大文件的导入。
	
	1. 根据当前 BE 的个数和原始文件的大小修改单个 BE 的最大扫描量和最大并发数。

		```
		修改 fe.conf 中配置
		
		max_broker_concurrency = BE 个数
		当前导入任务单个 BE 处理的数据量 = 原始文件大小 / max_broker_concurrency
		max_bytes_per_broker_scanner >= 当前导入任务单个 BE 处理的数据量
		
		比如一个 100G 的文件，集群的 BE 个数为 10个
		max_broker_concurrency = 10
		max_bytes_per_broker_scanner >= 10G = 100G / 10
		
		```
		
		修改后，所有的 BE 会并发的处理导入任务，每个 BE 处理原始文件的一部分。
		
		*注意：上述两个 FE 中的配置均为系统配置，也就是说其修改是作用于所有的 Broker load的任务的。*
		
	2. 在创建导入的时候自定义当前导入任务的 timeout 时间

		```
		当前导入任务单个 BE 处理的数据量 / 用户 Doris 集群最慢导入速度(MB/s) >= 当前导入任务的 timeout 时间 >= 当前导入任务单个 BE 处理的数据量 / 10M/s
		
		比如一个 100G 的文件，集群的 BE 个数为 10个
		timeout >= 1000s = 10G / 10M/s
		
		```
		
	3. 当用户发现第二步计算出的 timeout 时间超过系统默认的导入最大超时时间 4小时

		这时候不推荐用户将导入最大超时时间直接改大来解决问题。单个导入时间如果超过默认的导入最大超时时间4小时，最好是通过切分待导入文件并且分多次导入来解决问题。主要原因是：单次导入超过4小时的话，导入失败后重试的时间成本很高。
		
		可以通过如下公式计算出 Doris 集群期望最大导入文件数据量：
		
		```
		期望最大导入文件数据量 = 14400s * 10M/s * BE 个数
		比如：集群的 BE 个数为 10个
		期望最大导入文件数据量 = 14400 * 10M/s * 10 = 1440000M ≈ 1440G
		
		注意：一般用户的环境可能达不到 10M/s 的速度，所以建议超过 500G 的文件都进行文件切分，再导入。
		
		```
		
## 完整例子

数据情况：用户数据在 HDFS 中，文件地址为 hdfs://abc.com:8888/store_sales, hdfs 的认证用户名为 root, 密码为 password, 数据量大小约为 30G，希望导入到数据库 bj_sales 的表 store_sales 中。

集群情况：集群的 BE 个数约为 3 个，集群中有 3 个 Broker 名称均为 broker。

+ step1: 经过上述方法的计算，本次导入的单个 BE 导入量为 10G，则需要先修改 FE 的配置，将单个 BE 导入最大量修改为：

	```
	max_bytes_per_broker_scanner = 100000000000

	```

+ step2: 经计算，本次导入的时间大约为 1000s，并未超过默认超时时间，可不配置导入自定义超时时间。

+ step3：创建导入语句

	```
	load label store_sales_broker_load_01.bj_sales 
    	(DATA INFILE("hdfs://abc.com:8888/store_sales") into table store_sales
    	with broker 'broker' ("username"="root", "password"="password") 
	```
		

