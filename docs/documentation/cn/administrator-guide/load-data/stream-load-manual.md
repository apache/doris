# Stream load
Stream load 是一个同步的导入方式，用户通过发送 HTTP 请求将本地或内存中文件导入到 Doris 中。Stream load 同步执行导入并返回导入结果。用户可直接通过请求的返回体判断本次导入是否成功。

Stream load 主要适用于待导入文件在发送导入请求端，或可被发送导入请求端获取的情况。

# 基本原理
Stream load 是单机版本的导入，整个导入过程中参与 ETL 的节点只有一个 BE。由于导入后端是流式的所以并不会受单个 BE 的内存限制影响。

下图展示了 Stream load 的主要流程，省略了一些导入细节。

```
^                  +
|                  |
|                  |   user create stream load
| 5. BE return     |
| the result of    v
| stream load  +---+-----+
|              |         |
|              |FE       |
|              |         |
|              ++-+------+
| 1.FE redirect | ^  2. BE fetch the plan
| request to be | |  of stream load
|               | |   ^
|               | |   | 4. BE commit the txn
|               v |   |
|              ++-+---+--+
|              |         +------+
+--------------+BE       |      | 3. BE execute plan
               |         +<-----+
               +---------+
```

# 基本操作
## 创建导入
下面主要介绍创建 Stream load 的部分参数意义。创建导入的详细语法帮助执行 ``` HELP STREAM LOAD；``` 查看。

### 签名参数

+ user/passwd

	Stream load 由于创建导入的协议使用的是 HTTP 协议，所以需要通过 Basic access authentication 进行签名。Doris 系统会根据签名验证用户身份和导入权限。
	
###  导入任务参数

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

# Mini load

Mini load 支持的数据源，导入方式以及支持的 ETL 功能是 Stream load 的子集，且导入的效率和 Stream load 一致。**强烈建议使用 Stream load 替代 Mini load**

唯一不同的是，Mini load 由于为了兼容旧版本的使用习惯，用户不仅可以根据创建请求的返回值查看导入状态，也可以异步的通过查看导入命令查看导入状态。

Mini load的创建语法帮助可执行 ``` HELP MINI LOAD```。




