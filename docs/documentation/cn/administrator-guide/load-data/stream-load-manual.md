# Stream load
Stream load 是一个同步的导入方式，用户通过发送 HTTP 请求将本地或内存中文件导入到 Doris 中。Stream load 同步执行导入并返回导入结果。用户可直接通过请求的返回体判断本次导入是否成功。

Stream load 主要适用于待导入文件在发送导入请求端，或可被发送导入请求端获取的情况。

# 基本原理
Stream load 是单机版本的导入，整个导入过程中参与 ETL 的节点只有一个 BE。由于导入后端是流式的所以并不会受单个 BE 的内存限制影响。

下图展示了 Stream load 的主要流程，省略了一些导入细节。

```
^                  +
|                  |
|                  |   user creates stream load
| 5. BE returns    |
| the result of    v
| stream load  +---+-----+
|              |         |
|              |FE       |
|              |         |
|              ++-+------+
| 1.FE redirects| ^  2. BE fetches the plan
| request to be | |  of stream load
|               | |   ^
|               | |   | 4. BE wants to effect
|               v |   |    the load data
|              ++-+---+--+
|              |         +------+
+--------------+BE       |      | 3. BE executes plan
               |         +<-----+
               +---------+
```

# 基本操作
## 创建导入
Stream load 创建导入语句

```
语法：
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load

Header 中支持如下属性：
label， column_separator， columns， where， max_filter_ratio， partitions
格式为: -H "key1:value1"

示例：

curl --location-trusted -u root -T date -H "label:123" http://abc.com:8888/api/test/date/_stream_load

```
创建导入的详细语法帮助执行 ``` HELP STREAM LOAD；``` 查看, 下面主要介绍创建 Stream load 的部分参数意义。

### 签名参数

+ user/passwd

	Stream load 由于创建导入的协议使用的是 HTTP 协议，所以需要通过 Basic access authentication 进行签名。Doris 系统会根据签名验证用户身份和导入权限。
	
### 导入任务参数

Stream load 由于使用的是 HTTP 协议，所以所有导入任务有关的参数均设置在 Header 中。下面主要介绍了 Stream load 导入任务参数的部分参数意义。

+ label

	导入任务的标识。每个导入任务，都有一个在单 database 内部唯一的 label。label 是用户在导入命令中自定义的名称。通过这个 label，用户可以查看对应导入任务的执行情况。
	
	label 的另一个作用，是防止用户重复导入相同的数据。**强烈推荐用户同一批次数据使用相同的label。这样同一批次数据的重复请求只会被接受一次，保证了 At most once**
	
	当 label 对应的导入作业状态为 CANCELLED 时，该 label 可以再次被使用。
	
+ max\_filter\_ratio

	导入任务的最大容忍率，默认为0容忍，取值范围是0~1。当导入的 filter ratio 超过该值，则导入失败。计算公式为： ``` (dpp.abnorm.ALL / (dpp.abnorm.ALL + dpp.norm.ALL ) )> max_filter_ratio ```
	
	``` dpp.abnorm.ALL ``` 在代码中也叫 ``` num_rows_filtered``` 指的是导入过程中被过滤的错误数据。比如：列在表中为非空列，但是导入数据为空则为错误数据。可以通过 ``` SHOW LOAD ``` 命令查询导入任务的错误数据量。
	
	``` dpp.norm.ALL ``` 指的是导入过程中正确数据的条数。可以通过 ``` SHOW LOAD ``` 命令查询导入任务的正确数据量。
	
	``` num_rows_unselected ``` 导入过程中被 where 条件过滤掉的数据量。过滤的数据量不参与容忍率的计算。
	
	``` 原始文件的行数 = dpp.abnorm.ALL + dpp.norm.ALL + num_rows_unselected ```
	
+ where 

	导入任务指定的过滤条件。Stream load 支持对原始数据指定 where 语句进行过滤。被过滤的数据将不会被导入，也不会参数 filter ratio 的计算，但会被计入``` num_rows_unselected ```。 
	
+ partition

	待导入表的 Partition 信息，如果待导入数据不属于指定的 Partition 则不会被导入。这些数据将计入 ```dpp.abnorm.ALL ```
	
+ columns

	待导入数据的函数变换配置，目前 Stream load 支持的函数变换方法包含列的顺序变化以及表达式变换，其中表达式变换的方法与查询语句的一致。
	
	```
	列顺序变换例子：原始数据有两列，目前表也有两列（c1,c2）但是原始文件的第一列对应的是目标表的c2列, 而原始文件的第二列对应的是目标表的c1列，则写法如下：
	columns: c2,c1
	
	表达式变换例子：原始文件有两列，目标表也有两列（c1,c2）但是原始文件的两列均需要经过函数变换才能对应目标表的两列，则写法如下：
	columns: tmp_c1, tmp_c2, c1 = year(tmp_c1), c2 = mouth(tmp_c2)
	其中 tmp_*是一个占位符，代表的是原始文件中的两个原始列。 
	
	```
	
### 创建导入操作返回结果

由于 Stream load 是一种同步的导入方式，所以导入的结果会通过创建导入的返回值直接返回给用户。

下面主要解释了 Stream load 导入结果参数：

+ NumberLoadedRows

	和```dpp.norm.ALL ``` 含义相同，显示的是本次导入成功的条数。
	
+ NumberFilteredRows

	和 ```dpp.abnorm.ALL ``` 含义相同，显示的是本次导入被过滤的错误条数。
	
+ NumberUnselectedRows

	显示的是本地导入被 where 条件过滤的条数。


*注意：由于 Stream load 是同步的导入方式，所以并不会在 Doris 系统中记录导入信息，用户无法异步的通过查看导入命令看到 Stream load。使用时需监听创建导入请求的返回值获取导入结果。

## 取消导入
用户无法手动取消 Stream load，Stream load 在超时或者导入错误后会被系统自动取消。

# Stream load 导入系统配置
## FE conf 中的配置

+ timeout

	导入任务的超时时间(以秒为单位)，导入任务在设定的 timeout 时间内未完成则会被系统取消，变成 CANCELLED。
	
	目前 Stream load 并不支持自定义导入的 timeout 时间，所有 Stream load 导入的超时时间是统一的，默认的 timeout 时间为300秒。如果导入的源文件无法再规定时间内完成导入，则需要调整 FE 的参数```stream_load_default_timeout_second```。
	
## BE conf 中的配置

+ streaming\_load\_max\_mb

	Stream load 的最大导入大小，默认为 10G，单位是 Mb。如果用户的原始文件超过这个值，则需要调整 BE 的参数 ```streaming_load_max_mb```。
	
# Stream load 最佳实践
## 应用场景
使用 stream load 的最合适场景就是原始文件在内存中，或者在磁盘中。其次，由于 Stream load 是一种同步的导入方式，所以用户如果希望用同步方式获取导入结果，也可以使用这种导入。

## 数据量
由于 Stream load 的原理是由 BE 发起的导入，所以 Stream load 都是单机执行的，导入数据量在1 Byte ~ 20G 均可。由于默认的最大 Stream load 导入数据量为 10G，所以如果要导入超过 10G 的文件需要修改 BE 的配置 ```streaming_load_max_mb```

```
比如：待导入文件大小为15G
修改 BE 配置 streaming_load_max_mb 为 16000 即可。
```

+ 数据量过大导致超时

	Stream load 的默认超时为 300秒，按照 Doris 目前最大的导入限速来看，约超过 3G 的文件就需要修改导入任务默认超时时间了。
	
	```
	导入任务超时时间 = 导入数据量 / 10M/s （具体的平均导入速度需要用户根据自己的集群情况计算）
	例如：导入一个 10G 的文件
	timeout = 1000s 等于 10G / 10M/s
	```
	
## 完整例子
数据情况： 数据在发送导入请求端的本地磁盘路径 /home/store_sales 中，导入的数据量约为 15G，希望导入到数据库 bj_sales 的表 store_sales 中。

集群情况：Stream load 的并发数不受集群大小影响。

+ step1: 导入文件大小是否超过默认的最大导入大小10G

	```
	修改 BE conf
	streaming_load_max_mb = 16000
	```
+ step2: 计算大概的导入时间是否超过默认 timeout 值

	```
	导入时间 ≈ 15000 / 10 = 1500s
	超过了默认的 timeout 时间，需要修改 FE 的配置
	stream_load_default_timeout_second = 1500
	
	```
	
+ step3：创建导入任务

	```
	curl --location-trusted -u root:password -T /home/store_sales -H "label:abc" http://abc.com:8000/api/bj_sales/store_sales/_stream_load
	```


# 常见问题

## Label Already Exists

Stream load 的 Label 重复排查步骤如下：

1. 是否和其他导入方式已经存在的导入 Label 冲突：

	由于 Doris 系统中导入的 Label 不区分导入方式，所以存在其他导入方式使用了相同 Label 的问题。
	
	通过 ``` SHOW LOAD WHERE LABEL = “xxx”```，其中 xxx 为重复的 Label 字符串，查看是否已经存在一个 FINISHED 导入的 Label 和用户申请创建的 Label 相同。
	
2. 是否 Stream load 同一个创建任务被重复提交了

	由于 Stream load 是 HTTP 协议提交创建导入任务，一般各个语言的 HTTP Client 均会自带请求重试逻辑。Doris 系统在接受到第一个请求后，已经开始操作 Stream load，但是由于没有及时返回给 Client 端结果， Client 端会发生再次重试创建请求的情况。这时候 Doris 系统由于已经在操作第一个请求，所以第二个请求已经就会被报 Label Already Exists 的情况。
	
	排查上述可能的方法：使用 Label 搜索 FE Master 的日志，看是否存在同一个 Label 出现了两次 ```redirect load action to destination= ``` 的情况。如果有就说明，请求被 Client 端重复提交了。
	
	改进方法：
	+ 建议用户根据当前请求的数据量，计算出大致导入的时间，并根据导入超时时间改大 Client 端的请求超时时间，避免请求被 Client 端多次提交。

# Mini load

Mini load 支持的数据源，导入方式以及支持的 ETL 功能是 Stream load 的子集，且导入的效率和 Stream load 一致。**强烈建议使用 Stream load 替代 Mini load**

唯一不同的是，Mini load 由于为了兼容旧版本的使用习惯，用户不仅可以根据创建请求的返回值查看导入状态，也可以异步的通过查看导入命令查看导入状态。

Mini load的创建语法帮助可执行 ``` HELP MINI LOAD```。




