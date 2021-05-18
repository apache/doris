---
{
    "title": "Stream load",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Stream load

Stream load 是一个同步的导入方式，用户通过发送 HTTP 协议发送请求将本地文件或数据流导入到 Doris 中。Stream load 同步执行导入并返回导入结果。用户可直接通过请求的返回体判断本次导入是否成功。

Stream load 主要适用于导入本地文件，或通过程序导入数据流中的数据。

## 基本原理

下图展示了 Stream load 的主要流程，省略了一些导入细节。

```
                         ^      +
                         |      |
                         |      | 1A. User submit load to FE
                         |      |
                         |   +--v-----------+
                         |   | FE           |
5. Return result to user |   +--+-----------+
                         |      |
                         |      | 2. Redirect to BE
                         |      |
                         |   +--v-----------+
                         +---+Coordinator BE| 1B. User submit load to BE
                             +-+-----+----+-+
                               |     |    |
                         +-----+     |    +-----+
                         |           |          | 3. Distrbute data
                         |           |          |
                       +-v-+       +-v-+      +-v-+
                       |BE |       |BE |      |BE |
                       +---+       +---+      +---+
```

Stream load 中，Doris 会选定一个节点作为 Coordinator 节点。该节点负责接数据并分发数据到其他数据节点。

用户通过 HTTP 协议提交导入命令。如果提交到 FE，则 FE 会通过 HTTP redirect 指令将请求转发给某一个 BE。用户也可以直接提交导入命令给某一指定 BE。

导入的最终结果由 Coordinator BE 返回给用户。

## 基本操作
### 创建导入

Stream load 通过 HTTP 协议提交和传输数据。这里通过 `curl` 命令展示如何提交导入。

用户也可以通过其他 HTTP client 进行操作。

```
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load

Header 中支持属性见下面的 ‘导入任务参数’ 说明 
格式为: -H "key1:value1"
```

示例：

```
curl --location-trusted -u root -T date -H "label:123" http://abc.com:8030/api/test/date/_stream_load
```
创建导入的详细语法帮助执行 ```HELP STREAM LOAD``` 查看, 下面主要介绍创建 Stream load 的部分参数意义。

#### 签名参数

+ user/passwd

    Stream load 由于创建导入的协议使用的是 HTTP 协议，通过 Basic access authentication 进行签名。Doris 系统会根据签名验证用户身份和导入权限。
    
#### 导入任务参数

Stream load 由于使用的是 HTTP 协议，所以所有导入任务有关的参数均设置在 Header 中。下面主要介绍了 Stream load 导入任务参数的部分参数意义。

+ label

    导入任务的标识。每个导入任务，都有一个在单 database 内部唯一的 label。label 是用户在导入命令中自定义的名称。通过这个 label，用户可以查看对应导入任务的执行情况。
    
    label 的另一个作用，是防止用户重复导入相同的数据。**强烈推荐用户同一批次数据使用相同的 label。这样同一批次数据的重复请求只会被接受一次，保证了 At-Most-Once**
    
    当 label 对应的导入作业状态为 CANCELLED 时，该 label 可以再次被使用。

+ column_separator
    
    用于指定导入文件中的列分隔符，默认为\t。如果是不可见字符，则需要加\x作为前缀，使用十六进制来表示分隔符。
    
    如hive文件的分隔符\x01，需要指定为-H "column_separator:\x01"。

    可以使用多个字符的组合作为列分隔符。

+ line_delimiter
   
   用于指定导入文件中的换行符，默认为\n。

   可以使用做多个字符的组合作为换行符。
    
+ max\_filter\_ratio

    导入任务的最大容忍率，默认为0容忍，取值范围是0~1。当导入的错误率超过该值，则导入失败。
    
    如果用户希望忽略错误的行，可以通过设置这个参数大于 0，来保证导入可以成功。
    
    计算公式为：
    
    ``` (dpp.abnorm.ALL / (dpp.abnorm.ALL + dpp.norm.ALL ) ) > max_filter_ratio ```
    
    ```dpp.abnorm.ALL``` 表示数据质量不合格的行数。如类型不匹配，列数不匹配，长度不匹配等等。
    
    ```dpp.norm.ALL``` 指的是导入过程中正确数据的条数。可以通过 ```SHOW LOAD``` 命令查询导入任务的正确数据量。
    
    原始文件的行数 = `dpp.abnorm.ALL + dpp.norm.ALL`
    
+ where 

    导入任务指定的过滤条件。Stream load 支持对原始数据指定 where 语句进行过滤。被过滤的数据将不会被导入，也不会参与 filter ratio 的计算，但会被计入```num_rows_unselected```。 
    
+ partition

    待导入表的 Partition 信息，如果待导入数据不属于指定的 Partition 则不会被导入。这些数据将计入 ```dpp.abnorm.ALL ```
    
+ columns

    待导入数据的函数变换配置，目前 Stream load 支持的函数变换方法包含列的顺序变化以及表达式变换，其中表达式变换的方法与查询语句的一致。
    
    ```
    列顺序变换例子：原始数据有两列，目前表也有两列（c1,c2）但是原始文件的第一列对应的是目标表的c2列, 而原始文件的第二列对应的是目标表的c1列，则写法如下：
    columns: c2,c1
    
    表达式变换例子：原始文件有两列，目标表也有两列（c1,c2）但是原始文件的两列均需要经过函数变换才能对应目标表的两列，则写法如下：
    columns: tmp_c1, tmp_c2, c1 = year(tmp_c1), c2 = month(tmp_c2)
    其中 tmp_*是一个占位符，代表的是原始文件中的两个原始列。
    ```

+ exec\_mem\_limit

    导入内存限制。默认为 2GB，单位为字节。

+ strict\_mode

    Stream load 导入可以开启 strict mode 模式。开启方式为在 HEADER 中声明 ```strict_mode=true``` 。默认的 strict mode 为关闭。

    strict mode 模式的意思是：对于导入过程中的列类型转换进行严格过滤。严格过滤的策略如下：

    1. 对于列类型转换来说，如果 strict mode 为true，则错误的数据将被 filter。这里的错误数据是指：原始数据并不为空值，在参与列类型转换后结果为空值的这一类数据。

    2. 对于导入的某列由函数变换生成时，strict mode 对其不产生影响。
    
    3. 对于导入的某列类型包含范围限制的，如果原始数据能正常通过类型转换，但无法通过范围限制的，strict mode 对其也不产生影响。例如：如果类型是 decimal(1,0), 原始数据为 10，则属于可以通过类型转换但不在列声明的范围内。这种数据 strict 对其不产生影响。
+ merge\_type
    数据的合并类型，一共支持三种类型APPEND、DELETE、MERGE 其中，APPEND是默认值，表示这批数据全部需要追加到现有数据中，DELETE 表示删除与这批数据key相同的所有行，MERGE 语义 需要与delete 条件联合使用，表示满足delete 条件的数据按照DELETE 语义处理其余的按照APPEND 语义处理

#### strict mode 与 source data 的导入关系

这里以列类型为 TinyInt 来举例

>注：当表中的列允许导入空值时

|source data | source data example | string to int   | strict_mode        | result|
|------------|---------------------|-----------------|--------------------|---------|
|空值        | \N                  | N/A             | true or false      | NULL|
|not null    | aaa or 2000         | NULL            | true               | invalid data(filtered)|
|not null    | aaa                 | NULL            | false              | NULL|
|not null    | 1                   | 1               | true or false      | correct data|

这里以列类型为 Decimal(1,0) 举例
 
>注：当表中的列允许导入空值时

|source data | source data example | string to int   | strict_mode        | result|
|------------|---------------------|-----------------|--------------------|--------|
|空值        | \N                  | N/A             | true or false      | NULL|
|not null    | aaa                 | NULL            | true               | invalid data(filtered)|
|not null    | aaa                 | NULL            | false              | NULL|
|not null    | 1 or 10             | 1               | true or false      | correct data|

> 注意：10 虽然是一个超过范围的值，但是因为其类型符合 decimal的要求，所以 strict mode对其不产生影响。10 最后会在其他 ETL 处理流程中被过滤。但不会被 strict mode 过滤。
    

### 返回结果

由于 Stream load 是一种同步的导入方式，所以导入的结果会通过创建导入的返回值直接返回给用户。

示例：

```
{
    "TxnId": 1003,
    "Label": "b6f3bc78-0d2c-45d9-9e4c-faa0a0149bee",
    "Status": "Success",
    "ExistingJobStatus": "FINISHED", // optional
    "Message": "OK",
    "NumberTotalRows": 1000000,
    "NumberLoadedRows": 1000000,
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 40888898,
    "LoadTimeMs": 2144,
    "BeginTxnTimeMs": 1,
    "StreamLoadPutTimeMs": 2,
    "ReadDataTimeMs": 325,
    "WriteDataTimeMs": 1933,
    "CommitAndPublishTimeMs": 106,
    "ErrorURL": "http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005"
}
```

下面主要解释了 Stream load 导入结果参数：

+ TxnId：导入的事务ID。用户可不感知。

+ Label：导入 Label。由用户指定或系统自动生成。
    
+ Status：导入完成状态。
    
    "Success"：表示导入成功。
    
    "Publish Timeout"：该状态也表示导入已经完成，只是数据可能会延迟可见，无需重试。
    
    "Label Already Exists"：Label 重复，需更换 Label。
    
    "Fail"：导入失败。
    
+ ExistingJobStatus：已存在的 Label 对应的导入作业的状态。

    这个字段只有在当 Status 为 "Label Already Exists" 是才会显示。用户可以通过这个状态，知晓已存在 Label 对应的导入作业的状态。"RUNNING" 表示作业还在执行，"FINISHED" 表示作业成功。

+ Message：导入错误信息。

+ NumberTotalRows：导入总处理的行数。

+ NumberLoadedRows：成功导入的行数。

+ NumberFilteredRows：数据质量不合格的行数。

+ NumberUnselectedRows：被 where 条件过滤的行数。

+ LoadBytes：导入的字节数。

+ LoadTimeMs：导入完成时间。单位毫秒。

+ BeginTxnTimeMs：向Fe请求开始一个事务所花费的时间，单位毫秒。

+ StreamLoadPutTimeMs：向Fe请求获取导入数据执行计划所花费的时间，单位毫秒。
  
+ ReadDataTimeMs：读取数据所花费的时间，单位毫秒。

+ WriteDataTimeMs：执行写入数据操作所花费的时间，单位毫秒。

+ CommitAndPublishTimeMs：向Fe请求提交并且发布事务所花费的时间，单位毫秒。

+ ErrorURL：如果有数据质量问题，通过访问这个 URL 查看具体错误行。

> 注意：由于 Stream load 是同步的导入方式，所以并不会在 Doris 系统中记录导入信息，用户无法异步的通过查看导入命令看到 Stream load。使用时需监听创建导入请求的返回值获取导入结果。

### 取消导入

用户无法手动取消 Stream load，Stream load 在超时或者导入错误后会被系统自动取消。

## 相关系统配置

### FE 配置

+ stream\_load\_default\_timeout\_second

    导入任务的超时时间(以秒为单位)，导入任务在设定的 timeout 时间内未完成则会被系统取消，变成 CANCELLED。
    
    默认的 timeout 时间为 600 秒。如果导入的源文件无法在规定时间内完成导入，用户可以在 stream load 请求中设置单独的超时时间。

    或者调整 FE 的参数```stream_load_default_timeout_second``` 来设置全局的默认超时时间。

### BE 配置

+ streaming\_load\_max\_mb

    Stream load 的最大导入大小，默认为 10G，单位是 MB。如果用户的原始文件超过这个值，则需要调整 BE 的参数 ```streaming_load_max_mb```。
    
## 最佳实践

### 应用场景

使用 Stream load 的最合适场景就是原始文件在内存中，或者在磁盘中。其次，由于 Stream load 是一种同步的导入方式，所以用户如果希望用同步方式获取导入结果，也可以使用这种导入。

### 数据量

由于 Stream load 的原理是由 BE 发起的导入并分发数据，建议的导入数据量在 1G 到 10G 之间。由于默认的最大 Stream load 导入数据量为 10G，所以如果要导入超过 10G 的文件需要修改 BE 的配置 ```streaming_load_max_mb```

```
比如：待导入文件大小为15G
修改 BE 配置 streaming_load_max_mb 为 16000 即可。
```

Stream load 的默认超时为 300秒，按照 Doris 目前最大的导入限速来看，约超过 3G 的文件就需要修改导入任务默认超时时间了。
    
```
导入任务超时时间 = 导入数据量 / 10M/s （具体的平均导入速度需要用户根据自己的集群情况计算）
例如：导入一个 10G 的文件
timeout = 1000s 等于 10G / 10M/s
```
    
### 完整例子
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
    curl --location-trusted -u user:password -T /home/store_sales -H "label:abc" http://abc.com:8000/api/bj_sales/store_sales/_stream_load
    ```

## 常见问题

* Label Already Exists

    Stream load 的 Label 重复排查步骤如下：

    1. 是否和其他导入方式已经存在的导入 Label 冲突：

       由于 Doris 系统中导入的 Label 不区分导入方式，所以存在其他导入方式使用了相同 Label 的问题。
    
       通过 ```SHOW LOAD WHERE LABEL = “xxx”```，其中 xxx 为重复的 Label 字符串，查看是否已经存在一个 FINISHED 导入的 Label 和用户申请创建的 Label 相同。
    
    2. 是否 Stream load 同一个作业被重复提交了

       由于 Stream load 是 HTTP 协议提交创建导入任务，一般各个语言的 HTTP Client 均会自带请求重试逻辑。Doris 系统在接受到第一个请求后，已经开始操作 Stream load，但是由于没有及时返回给 Client 端结果， Client 端会发生再次重试创建请求的情况。这时候 Doris 系统由于已经在操作第一个请求，所以第二个请求已经就会被报 Label Already Exists 的情况。
    
       排查上述可能的方法：使用 Label 搜索 FE Master 的日志，看是否存在同一个 Label 出现了两次 ```redirect load action to destination= ``` 的情况。如果有就说明，请求被 Client 端重复提交了。
    
       建议用户根据当前请求的数据量，计算出大致导入的时间，并根据导入超时时间改大 Client 端的请求超时时间，避免请求被 Client 端多次提交。



