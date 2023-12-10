---
{
    "title": "STREAM-LOAD",
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

## STREAM-LOAD

### Name

STREAM LOAD

### Description

stream-load: load data to table in streaming

```
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT http://fe_host:http_port/api/{db}/{table}/_stream_load
```

该语句用于向指定的 table 导入数据，与普通Load区别是，这种导入方式是同步导入。

这种导入方式仍然能够保证一批导入任务的原子性，要么全部数据导入成功，要么全部失败。

该操作会同时更新和此 base table 相关的 rollup table 的数据。

这是一个同步操作，整个数据导入工作完成后返回给用户导入结果。

当前支持HTTP chunked与非chunked上传两种方式，对于非chunked方式，必须要有Content-Length来标示上传内容长度，这样能够保证数据的完整性。

另外，用户最好设置Expect Header字段内容100-continue，这样可以在某些出错场景下避免不必要的数据传输。

参数介绍：
        用户可以通过HTTP的Header部分来传入导入参数

1. label: 一次导入的标签，相同标签的数据无法多次导入。用户可以通过指定Label的方式来避免一份数据重复导入的问题。
   
    当前Doris内部保留30分钟内最近成功的label。
    
2. column_separator：用于指定导入文件中的列分隔符，默认为\t。如果是不可见字符，则需要加\x作为前缀，使用十六进制来表示分隔符。
   
    如hive文件的分隔符\x01，需要指定为-H "column_separator:\x01"。
    
    可以使用多个字符的组合作为列分隔符。
    
3. line_delimiter：用于指定导入文件中的换行符，默认为\n。可以使用做多个字符的组合作为换行符。
   
4. columns：用于指定导入文件中的列和 table 中的列的对应关系。如果源文件中的列正好对应表中的内容，那么是不需要指定这个字段的内容的。
   
    如果源文件与表schema不对应，那么需要这个字段进行一些数据转换。这里有两种形式column，一种是直接对应导入文件中的字段，直接使用字段名表示；
    
    一种是衍生列，语法为 `column_name` = expression。举几个例子帮助理解。
    
    例1: 表中有3个列“c1, c2, c3”，源文件中的三个列一次对应的是"c3,c2,c1"; 那么需要指定-H "columns: c3, c2, c1"
    
    例2: 表中有3个列“c1, c2, c3", 源文件中前三列依次对应，但是有多余1列；那么需要指定-H "columns: c1, c2, c3, xxx";
    
    最后一个列随意指定个名称占位即可
    
    例3: 表中有3个列“year, month, day"三个列，源文件中只有一个时间列，为”2018-06-01 01:02:03“格式；
    
    那么可以指定-H "columns: col, year = year(col), month=month(col), day=day(col)"完成导入
    
5. where: 用于抽取部分数据。用户如果有需要将不需要的数据过滤掉，那么可以通过设定这个选项来达到。
   
    例1: 只导入大于k1列等于20180601的数据，那么可以在导入时候指定-H "where: k1 = 20180601"
    
6. max_filter_ratio：最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。数据不规范不包括通过 where 条件过滤掉的行。

7. partitions: 用于指定这次导入所设计的partition。如果用户能够确定数据对应的partition，推荐指定该项。不满足这些分区的数据将被过滤掉。
   
    比如指定导入到p1, p2分区，-H "partitions: p1, p2"
    
8. timeout: 指定导入的超时时间。单位秒。默认是 600 秒。可设置范围为 1 秒 ~ 259200 秒。

9. strict_mode: 用户指定此次导入是否开启严格模式，默认为关闭。开启方式为 -H "strict_mode: true"。

10. timezone: 指定本次导入所使用的时区。默认为东八区。该参数会影响所有导入涉及的和时区有关的函数结果。

11. exec_mem_limit: 导入内存限制。默认为 2GB。单位为字节。

12. format: 指定导入数据格式，支持csv、json、<version since="1.2" type="inline"> csv_with_names(支持csv文件行首过滤)、csv_with_names_and_types(支持csv文件前两行过滤)、parquet、orc</version>，默认是csv。

13. jsonpaths: 导入json方式分为：简单模式和匹配模式。
    
    简单模式：没有设置jsonpaths参数即为简单模式，这种模式下要求json数据是对象类型，例如：
    
       ```
       {"k1":1, "k2":2, "k3":"hello"}，其中k1，k2，k3是列名字。
       ```
    匹配模式：用于json数据相对复杂，需要通过jsonpaths参数匹配对应的value。
    
14. strip_outer_array: 布尔类型，为true表示json数据以数组对象开始且将数组对象中进行展平，默认值是false。例如：
       ```
           [
            {"k1" : 1, "v1" : 2},
            {"k1" : 3, "v1" : 4}
           ]
           当strip_outer_array为true，最后导入到doris中会生成两行数据。
       ```
    
15. json_root: json_root为合法的jsonpath字符串，用于指定json document的根节点，默认值为""。
    
16. merge_type: 数据的合并类型，一共支持三种类型APPEND、DELETE、MERGE 其中，APPEND是默认值，表示这批数据全部需要追加到现有数据中，DELETE 表示删除与这批数据key相同的所有行，MERGE 语义 需要与delete 条件联合使用，表示满足delete 条件的数据按照DELETE 语义处理其余的按照APPEND 语义处理， 示例：`-H "merge_type: MERGE" -H "delete: flag=1"`

17. delete: 仅在 MERGE下有意义， 表示数据的删除条件

18. function_column.sequence_col: 只适用于UNIQUE_KEYS,相同key列下，保证value列按照source_sequence列进行REPLACE, source_sequence可以是数据源中的列，也可以是表结构中的一列。
    
19. fuzzy_parse: 布尔类型，为true表示json将以第一行为schema 进行解析，开启这个选项可以提高 json 导入效率，但是要求所有json 对象的key的顺序和第一行一致， 默认为false，仅用于json 格式
    
20. num_as_string: 布尔类型，为true表示在解析json数据时会将数字类型转为字符串，然后在确保不会出现精度丢失的情况下进行导入。
    
21. read_json_by_line: 布尔类型，为true表示支持每行读取一个json对象，默认值为false。
    
22. send_batch_parallelism: 整型，用于设置发送批处理数据的并行度，如果并行度的值超过 BE 配置中的 `max_send_batch_parallelism_per_job`，那么作为协调点的 BE 将使用 `max_send_batch_parallelism_per_job` 的值。

23. <version since="1.2" type="inline"> hidden_columns: 用于指定导入数据中包含的隐藏列，在Header中不包含columns时生效，多个hidden column用逗号分割。</version>

      ```
      hidden_columns: __DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__
      系统会使用用户指定的数据导入数据。在上述用例中，导入数据中最后一列数据为__DORIS_SEQUENCE_COL__。
      ```

24. load_to_single_tablet: 布尔类型，为true表示支持一个任务只导入数据到对应分区的一个 tablet，默认值为 false，该参数只允许在对带有 random 分桶的 olap 表导数的时候设置。

25. compress_type: 指定文件的压缩格式。目前只支持 csv 文件的压缩。支持 gz, lzo, bz2, lz4, lzop, deflate 压缩格式。

26. trim_double_quotes: 布尔类型，默认值为 false，为 true 时表示裁剪掉 csv 文件每个字段最外层的双引号。

27. skip_lines: <version since="dev" type="inline"> 整数类型, 默认值为0, 含义为跳过csv文件的前几行. 当设置format设置为 `csv_with_names` 或、`csv_with_names_and_types` 时, 该参数会失效. </version>

28. comment: <version since="1.2.3" type="inline"> 字符串类型, 默认值为空. 给任务增加额外的信息. </version>

29. enclose: <version since="dev" type="inline"> 包围符。当csv数据字段中含有行分隔符或列分隔符时，为防止意外截断，可指定单字节字符作为包围符起到保护作用。例如列分隔符为","，包围符为"'"，数据为"a,'b,c'",则"b,c"会被解析为一个字段。 </version>
  
30. escape <version since="dev" type="inline"> 转义符。用于转义在字段中出现的与包围符相同的字符。例如数据为"a,'b,'c'"，包围符为"'"，希望"b,'c被作为一个字段解析，则需要指定单字节转义符，例如"\"，然后将数据修改为"a,'b,\'c'"。 </version>

### Example

1. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表，使用Label用于去重。指定超时时间为 100 秒
   
   ```
   curl --location-trusted -u root -H "label:123" -H "timeout:100" -T testData http://host:port/api/testDb/testTbl/_stream_load
   ```

2. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表，使用Label用于去重, 并且只导入k1等于20180601的数据
        
   ```
   curl --location-trusted -u root -H "label:123" -H "where: k1=20180601" -T testData http://host:port/api/testDb/testTbl/_stream_load
   ```
    
3. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率（用户是defalut_cluster中的）
        
   ```
   curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -T testData http://host:port/api/testDb/testTbl/_stream_load
   ```
    
4. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率，并且指定文件的列名（用户是defalut_cluster中的）
   
   ```
   curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" -H "columns: k2, k1, v1" -T testData http://host:port/api/testDb/testTbl/_stream_load
   ```
    
5. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表中的p1, p2分区, 允许20%的错误率。
        
   ```
   curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" -H "partitions: p1, p2" -T testData http://host:port/api/testDb/testTbl/_stream_load
   ```
    
6. 使用streaming方式导入（用户是defalut_cluster中的）
        
   ```
   seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - http://host:port/api/testDb/testTbl/_stream_load
   ```
    
7. 导入含有HLL列的表，可以是表中的列或者数据中的列用于生成HLL列，也可使用hll_empty补充数据中没有的列
        
   ```
   curl --location-trusted -u root -H "columns: k1, k2, v1=hll_hash(k1), v2=hll_empty()" -T testData http://host:port/api/testDb/testTbl/_stream_load
   ```
    
8. 导入数据进行严格模式过滤，并设置时区为 Africa/Abidjan
        
    ```
    curl --location-trusted -u root -H "strict_mode: true" -H "timezone: Africa/Abidjan" -T testData http://host:port/api/testDb/testTbl/_stream_load
    ```
    
9. 导入含有BITMAP列的表，可以是表中的列或者数据中的列用于生成BITMAP列，也可以使用bitmap_empty填充空的Bitmap

   ```
   curl --location-trusted -u root -H "columns: k1, k2, v1=to_bitmap(k1), v2=bitmap_empty()" -T testData http://host:port/api/testDb/testTbl/_stream_load
   ```
   
10. 简单模式，导入json数据
    
    表结构：
     ```
     `category` varchar(512) NULL COMMENT "",
     `author` varchar(512) NULL COMMENT "",
     `title` varchar(512) NULL COMMENT "",
     `price` double NULL COMMENT ""
    ```
    json数据格式：
    ```
    {"category":"C++","author":"avc","title":"C++ primer","price":895}
    ```
    导入命令：
    ```
    curl --location-trusted -u root  -H "label:123" -H "format: json" -T testData http://host:port/api/testDb/testTbl/_stream_load
    ```
    为了提升吞吐量，支持一次性导入多条json数据，每行为一个json对象，默认使用\n作为换行符，需要将read_json_by_line设置为true，json数据格式如下：
            
    ```
    {"category":"C++","author":"avc","title":"C++ primer","price":89.5}
    {"category":"Java","author":"avc","title":"Effective Java","price":95}
    {"category":"Linux","author":"avc","title":"Linux kernel","price":195}
    ```
    
11. 匹配模式，导入json数据

    json数据格式：

    ```
    [
    {"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},
    {"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}
    ]
    ```
    通过指定jsonpath进行精准导入，例如只导入category、author、price三个属性
    ```
    curl --location-trusted -u root  -H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" -H "strip_outer_array: true" -T testData http://host:port/api/testDb/testTbl/_stream_load
    ```
    
    说明：
        1）如果json数据是以数组开始，并且数组中每个对象是一条记录，则需要将strip_outer_array设置成true，表示展平数组。
        2）如果json数据是以数组开始，并且数组中每个对象是一条记录，在设置jsonpath时，我们的ROOT节点实际上是数组中对象。
    
12. 用户指定json根节点

    json数据格式:
    ```
    {
     "RECORDS":[
    {"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
    {"category":"22","author":"2avc","price":895,"timestamp":1589191487},
    {"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
    ]
    }
    ```
    通过指定jsonpath进行精准导入，例如只导入category、author、price三个属性 
    ```
    curl --location-trusted -u root  -H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" -H "strip_outer_array: true" -H "json_root: $.RECORDS" -T testData http://host:port/api/testDb/testTbl/_stream_load
    ```
13. 删除与这批导入key 相同的数据
    
    ```
    curl --location-trusted -u root -H "merge_type: DELETE" -T testData http://host:port/api/testDb/testTbl/_stream_load
    ```

14. 将这批数据中与flag 列为ture 的数据相匹配的列删除，其他行正常追加
    
    ```
    curl --location-trusted -u root: -H "column_separator:," -H "columns: siteid, citycode, username, pv, flag" -H "merge_type: MERGE" -H "delete: flag=1"  -T testData http://host:port/api/testDb/testTbl/_stream_load
    ```
15. 导入数据到含有sequence列的UNIQUE_KEYS表中

    ```
    curl --location-trusted -u root -H "columns: k1,k2,source_sequence,v1,v2" -H "function_column.sequence_col: source_sequence" -T testData http://host:port/api/testDb/testTbl/_stream_load
    ```
    
16. csv文件行首过滤导入
   
    文件数据:

    ```
     id,name,age
     1,doris,20
     2,flink,10
    ```
    通过指定`format=csv_with_names`过滤首行导入
    ```
    curl --location-trusted -u root -T test.csv  -H "label:1" -H "format:csv_with_names" -H "column_separator:," http://host:port/api/testDb/testTbl/_stream_load
    ```
17. 导入数据到表字段含有DEFAULT CURRENT_TIMESTAMP的表中

    表结构：
    ```sql
    `id` bigint(30) NOT NULL,
    `order_code` varchar(30) DEFAULT NULL COMMENT '',
    `create_time` datetimev2(3) DEFAULT CURRENT_TIMESTAMP
    ```
    
    json数据格式：
    ```
    {"id":1,"order_Code":"avc"}
    ```

    导入命令：
    ```
    curl --location-trusted -u root -T test.json -H "label:1" -H "format:json" -H 'columns: id, order_code, create_time=CURRENT_TIMESTAMP()' http://host:port/api/testDb/testTbl/_stream_load
    ```
### Keywords

    STREAM, LOAD

### Best Practice

1. 查看导入任务状态

   Stream Load 是一个同步导入过程，语句执行成功即代表数据导入成功。导入的执行结果会通过 HTTP 返回值同步返回。并以 Json 格式展示。示例如下：

   ```json
   {
       "TxnId": 17,
       "Label": "707717c0-271a-44c5-be0b-4e71bfeacaa5",
       "Status": "Success",
       "Message": "OK",
       "NumberTotalRows": 5,
       "NumberLoadedRows": 5,
       "NumberFilteredRows": 0,
       "NumberUnselectedRows": 0,
       "LoadBytes": 28,
       "LoadTimeMs": 27,
       "BeginTxnTimeMs": 0,
       "StreamLoadPutTimeMs": 2,
       "ReadDataTimeMs": 0,
       "WriteDataTimeMs": 3,
       "CommitAndPublishTimeMs": 18
   }
   ```

   下面主要解释了 Stream load 导入结果参数：

    - TxnId：导入的事务ID。用户可不感知。

    - Label：导入 Label。由用户指定或系统自动生成。

    - Status：导入完成状态。

        "Success"：表示导入成功。

        "Publish Timeout"：该状态也表示导入已经完成，只是数据可能会延迟可见，无需重试。

        "Label Already Exists"：Label 重复，需更换 Label。

        "Fail"：导入失败。

    - ExistingJobStatus：已存在的 Label 对应的导入作业的状态。

        这个字段只有在当 Status 为 "Label Already Exists" 时才会显示。用户可以通过这个状态，知晓已存在 Label 对应的导入作业的状态。"RUNNING" 表示作业还在执行，"FINISHED" 表示作业成功。

    - Message：导入错误信息。

    - NumberTotalRows：导入总处理的行数。

    - NumberLoadedRows：成功导入的行数。

    - NumberFilteredRows：数据质量不合格的行数。

    - NumberUnselectedRows：被 where 条件过滤的行数。

    - LoadBytes：导入的字节数。

    - LoadTimeMs：导入完成时间。单位毫秒。

    - BeginTxnTimeMs：向Fe请求开始一个事务所花费的时间，单位毫秒。

    - StreamLoadPutTimeMs：向Fe请求获取导入数据执行计划所花费的时间，单位毫秒。

    - ReadDataTimeMs：读取数据所花费的时间，单位毫秒。

    - WriteDataTimeMs：执行写入数据操作所花费的时间，单位毫秒。

    - CommitAndPublishTimeMs：向Fe请求提交并且发布事务所花费的时间，单位毫秒。

    - ErrorURL：如果有数据质量问题，通过访问这个 URL 查看具体错误行。

    > 注意：由于 Stream load 是同步的导入方式，所以并不会在 Doris 系统中记录导入信息，用户无法异步的通过查看导入命令看到 Stream load。使用时需监听创建导入请求的返回值获取导入结果。

2. 如何正确提交 Stream Load 作业和处理返回结果。

   Stream Load 是同步导入操作，因此用户需同步等待命令的返回结果，并根据返回结果决定下一步处理方式。

   用户首要关注的是返回结果中的 `Status` 字段。

   如果为 `Success`，则一切正常，可以进行之后的其他操作。

   如果返回结果出现大量的 `Publish Timeout`，则可能说明目前集群某些资源（如IO）紧张导致导入的数据无法最终生效。`Publish Timeout` 状态的导入任务已经成功，无需重试，但此时建议减缓或停止新导入任务的提交，并观察集群负载情况。

   如果返回结果为 `Fail`，则说明导入失败，需根据具体原因查看问题。解决后，可以使用相同的 Label 重试。

   在某些情况下，用户的 HTTP 连接可能会异常断开导致无法获取最终的返回结果。此时可以使用相同的 Label 重新提交导入任务，重新提交的任务可能有如下结果：

   1. `Status` 状态为 `Success`，`Fail` 或者 `Publish Timeout`。此时按照正常的流程处理即可。
   2. `Status` 状态为 `Label Already Exists`。则此时需继续查看 `ExistingJobStatus` 字段。如果该字段值为 `FINISHED`，则表示这个 Label 对应的导入任务已经成功，无需在重试。如果为 `RUNNING`，则表示这个 Label 对应的导入任务依然在运行，则此时需每间隔一段时间（如10秒），使用相同的 Label 继续重复提交，直到 `Status` 不为 `Label Already Exists`，或者 `ExistingJobStatus` 字段值为 `FINISHED` 为止。

3. 取消导入任务

   已提交切尚未结束的导入任务可以通过 CANCEL LOAD 命令取消。取消后，已写入的数据也会回滚，不会生效。

4. Label、导入事务、多表原子性

   Doris 中所有导入任务都是原子生效的。并且在同一个导入任务中对多张表的导入也能够保证原子性。同时，Doris 还可以通过 Label 的机制来保证数据导入的不丢不重。具体说明可以参阅 [导入事务和原子性](../../../../data-operate/import/import-scenes/load-atomicity.md) 文档。

5. 列映射、衍生列和过滤

   Doris 可以在导入语句中支持非常丰富的列转换和过滤操作。支持绝大多数内置函数和 UDF。关于如何正确的使用这个功能，可参阅 [列的映射，转换与过滤](../../../../data-operate/import/import-scenes/load-data-convert.md) 文档。

6. 错误数据过滤

   Doris 的导入任务可以容忍一部分格式错误的数据。容忍率通过 `max_filter_ratio` 设置。默认为0，即表示当有一条错误数据时，整个导入任务将会失败。如果用户希望忽略部分有问题的数据行，可以将次参数设置为 0~1 之间的数值，Doris 会自动跳过哪些数据格式不正确的行。

   关于容忍率的一些计算方式，可以参阅 [列的映射，转换与过滤](../../../../data-operate/import/import-scenes/load-data-convert.md) 文档。

7. 严格模式

   `strict_mode` 属性用于设置导入任务是否运行在严格模式下。该属性会对列映射、转换和过滤的结果产生影响，它同时也将控制部分列更新的行为。关于严格模式的具体说明，可参阅 [严格模式](../../../../data-operate/import/import-scenes/load-strict-mode.md) 文档。

8. 超时时间

   Stream Load 的默认超时时间为 10 分钟。从任务提交开始算起。如果在超时时间内没有完成，则任务会失败。

9. 数据量和任务数限制

   Stream Load 适合导入几个GB以内的数据，因为数据为单线程传输处理，因此导入过大的数据性能得不到保证。当有大量本地数据需要导入时，可以并行提交多个导入任务。

   Doris 同时会限制集群内同时运行的导入任务数量，通常在 10-20 个不等。之后提交的导入作业会被拒绝。
