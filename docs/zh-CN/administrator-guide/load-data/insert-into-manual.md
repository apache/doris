---
{
    "title": "Insert Into",
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

# Insert Into

Insert Into 语句的使用方式和 MySQL 等数据库中 Insert Into 语句的使用方式类似。但在 Doris 中，所有的数据写入都是一个独立的导入作业。所以这里将 Insert Into 也作为一种导入方式介绍。

主要的 Insert Into 命令包含以下两种；

* INSERT INTO tbl SELECT ...
* INSERT INTO tbl (col1, col2, ...) VALUES (1, 2, ...), (1,3, ...);

其中第二种命令仅用于 Demo，不要使用在测试或生产环境中。

## 基本操作

### 创建导入

Insert Into 命令需要通过 MySQL 协议提交，创建导入请求会同步返回导入结果。

语法：

```
INSERT INTO table_name [partition_info] [WITH LABEL label] [col_list] [query_stmt] [VALUES];
```

示例：

```
INSERT INTO tbl2 WITH LABEL label1 SELECT * FROM tbl3;
INSERT INTO tbl1 VALUES ("qweasdzxcqweasdzxc"), ("a");
```

**注意**

当需要使用 `CTE(Common Table Expressions)` 作为 insert 操作中的查询部分时，必须指定 `WITH LABEL` 和 column list 部分。示例

```
INSERT INTO tbl1 WITH LABEL label1
WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1;


INSERT INTO tbl1 (k1)
WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1;
```

下面主要介绍创建导入语句中使用到的参数：

+ partition\_info

    导入表的目标分区，如果指定目标分区，则只会导入符合目标分区的数据。如果没有指定，则默认值为这张表的所有分区。
    
+ col\_list

    导入表的目标列，可以以任意的顺序存在。如果没有指定目标列，那么默认值是这张表的所有列。如果待表中的某个列没有存在目标列中，那么这个列需要有默认值，否则 Insert Into 就会执行失败。

    如果查询语句的结果列类型与目标列的类型不一致，那么会调用隐式类型转化，如果不能够进行转化，那么 Insert Into 语句会报语法解析错误。

+ query\_stmt

    通过一个查询语句，将查询语句的结果导入到 Doris 系统中的其他表。查询语句支持任意 Doris 支持的 SQL 查询语法。

+ VALUES
    
    用户可以通过 VALUES 语法插入一条或者多条数据。
    
    *注意：VALUES 方式仅适用于导入几条数据作为导入 DEMO 的情况，完全不适用于任何测试和生产环境。Doris 系统本身也不适合单条数据导入的场景。建议使用 INSERT INTO SELECT 的方式进行批量导入。*
    
* WITH LABEL

    INSERT 操作作为一个导入任务，也可以指定一个 label。如果不指定，则系统会自动指定一个 UUID 作为 label。
    
    该功能需要 0.11+ 版本。
    
    *注意：建议指定 Label 而不是由系统自动分配。如果由系统自动分配，但在 Insert Into 语句执行过程中，因网络错误导致连接断开等，则无法得知 Insert Into 是否成功。而如果指定 Label，则可以再次通过 Label 查看任务结果。*
    
### 导入结果

Insert Into 本身就是一个 SQL 命令，其返回结果会根据执行结果的不同，分为以下几种：

1. 结果集为空

    如果 insert 对应 select 语句的结果集为空，则返回如下：
    
    ```
    mysql> insert into tbl1 select * from empty_tbl;
    Query OK, 0 rows affected (0.02 sec)
    ```
    
    `Query OK` 表示执行成功。`0 rows affected` 表示没有数据被导入。
    
2. 结果集不为空

    在结果集不为空的情况下。返回结果分为如下几种情况：
    
    1. Insert 执行成功并可见：

        ```
        mysql> insert into tbl1 select * from tbl2;
        Query OK, 4 rows affected (0.38 sec)
        {'label':'insert_8510c568-9eda-4173-9e36-6adc7d35291c', 'status':'visible', 'txnId':'4005'}
        
        mysql> insert into tbl1 with label my_label1 select * from tbl2;
        Query OK, 4 rows affected (0.38 sec)
        {'label':'my_label1', 'status':'visible', 'txnId':'4005'}
        
        mysql> insert into tbl1 select * from tbl2;
        Query OK, 2 rows affected, 2 warnings (0.31 sec)
        {'label':'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status':'visible', 'txnId':'4005'}
        
        mysql> insert into tbl1 select * from tbl2;
        Query OK, 2 rows affected, 2 warnings (0.31 sec)
        {'label':'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status':'committed', 'txnId':'4005'}
        ```
        
        `Query OK` 表示执行成功。`4 rows affected` 表示总共有4行数据被导入。`2 warnings` 表示被过滤的行数。
        
        同时会返回一个 json 串：
        
        ```
        {'label':'my_label1', 'status':'visible', 'txnId':'4005'}
        {'label':'insert_f0747f0e-7a35-46e2-affa-13a235f4020d', 'status':'committed', 'txnId':'4005'}
        {'label':'my_label1', 'status':'visible', 'txnId':'4005', 'err':'some other error'}
        ```
        
        `label` 为用户指定的 label 或自动生成的 label。Label 是该 Insert Into 导入作业的标识。每个导入作业，都有一个在单 database 内部唯一的 Label。
        
        `status` 表示导入数据是否可见。如果可见，显示 `visible`，如果不可见，显示 `committed`。
        
        `txnId` 为这个 insert 对应的导入事务的 id。
        
        `err` 字段会显示一些其他非预期错误。
        
        当需要查看被过滤的行时，用户可以通过如下语句
        
        ```
        show load where label="xxx";
        ```
        
        返回结果中的 URL 可以用于查询错误的数据，具体见后面 **查看错误行** 小结。
                
        **数据不可见是一个临时状态，这批数据最终是一定可见的**
        
        可以通过如下语句查看这批数据的可见状态：
        
        ```
        show transaction where id=4005;
        ```
        
        返回结果中的 `TransactionStatus` 列如果为 `visible`，则表述数据可见。
        
    2. Insert 执行失败

        执行失败表示没有任何数据被成功导入，并返回如下：
        
        ```
        mysql> insert into tbl1 select * from tbl2 where k1 = "a";
        ERROR 1064 (HY000): all partitions have no load data. url: http://10.74.167.16:8042/api/_load_error_log?file=__shard_2/error_log_insert_stmt_ba8bb9e158e4879-ae8de8507c0bf8a2_ba8bb9e158e4879_ae8de8507c0bf8a2
        ```
        
        其中 `ERROR 1064 (HY000): all partitions have no load data` 显示失败原因。后面的 url 可以用于查询错误的数据，具体见后面 **查看错误行** 小结。

**综上，对于 insert 操作返回结果的正确处理逻辑应为：**

1. 如果返回结果为 `ERROR 1064 (HY000)`，则表示导入失败。
2. 如果返回结果为 `Query OK`，则表示执行成功。
    1. 如果 `rows affected` 为 0，表示结果集为空，没有数据被导入。
    2. 如果 `rows affected` 大于 0：
        1. 如果 `status` 为 `committed`，表示数据还不可见。需要通过 `show transaction` 语句查看状态直到 `visible`
        2. 如果 `status` 为 `visible`，表示数据导入成功。
    3. 如果 `warnings` 大于 0，表示有数据被过滤，可以通过 `show load` 语句获取 url 查看被过滤的行。

### SHOW LAST INSERT

在上一小节中我们介绍了如何根据 insert 操作的返回结果进行后续处理。但一些语言的mysql类库中很难获取返回结果的中的 json 字符串。因此，Doris 还提供了 `SHOW LAST INSERT` 命令来显式的获取最近一次 insert 操作的结果。

当执行完一个 insert 操作后，可以在同一 session 连接中执行 `SHOW LAST INSERT`。该命令会返回最近一次insert 操作的结果，如：

```
mysql> show last insert\G
*************************** 1. row ***************************
    TransactionId: 64067
            Label: insert_ba8f33aea9544866-8ed77e2844d0cc9b
         Database: default_cluster:db1
            Table: t1
TransactionStatus: VISIBLE
       LoadedRows: 2
     FilteredRows: 0
```

该命令会返回 insert 以及对应事务的详细信息。因此，用户可以在每次执行完 insert 操作后，继续执行 `show last insert` 命令来获取 insert 的结果。

> 注意：该命令只会返回在同一 session 连接中，最近一次 insert 操作的结果。如果连接断开或更换了新的连接，则将返回空集。
        
## 相关系统配置

### FE 配置

+ timeout

    导入任务的超时时间(以秒为单位)，导入任务在设定的 timeout 时间内未完成则会被系统取消，变成 CANCELLED。
    
    目前 Insert Into 并不支持自定义导入的 timeout 时间，所有 Insert Into 导入的超时时间是统一的，默认的 timeout 时间为1小时。如果导入的源文件无法再规定时间内完成导入，则需要调整 FE 的参数```insert_load_default_timeout_second```。
    
    同时 Insert Into 语句收到 Session 变量 `query_timeout` 的限制。可以通过 `SET query_timeout = xxx;` 来增加超时时间，单位是秒。
    
### Session 变量

+ enable\_insert\_strict

    Insert Into 导入本身不能控制导入可容忍的错误率。用户只能通过 `enable_insert_strict` 这个 Session 参数用来控制。

    当该参数设置为 false 时，表示至少有一条数据被正确导入，则返回成功。如果有失败数据，则还会返回一个 Label。

    当该参数设置为 true 时，表示如果有一条数据错误，则导入失败。

    默认为 false。可通过 `SET enable_insert_strict = true;` 来设置。 
        
+ query\_timeout

    Insert Into 本身也是一个 SQL 命令，因此 Insert Into 语句也受到 Session 变量 `query_timeout` 的限制。可以通过 `SET query_timeout = xxx;` 来增加超时时间，单位是秒。
    
## 最佳实践

### 应用场景
1. 用户希望仅导入几条假数据，验证一下 Doris 系统的功能。此时适合使用 INSERT INTO VALUES 的语法。
2. 用户希望将已经在 Doris 表中的数据进行 ETL 转换并导入到一个新的 Doris 表中，此时适合使用 INSERT INTO SELECT 语法。
3. 用户可以创建一种外部表，如 MySQL 外部表映射一张 MySQL 系统中的表。或者创建 Broker 外部表来映射 HDFS 上的数据文件。然后通过 INSERT INTO SELECT 语法将外部表中的数据导入到 Doris 表中存储。

### 数据量
Insert Into 对数据量没有限制，大数据量导入也可以支持。但 Insert Into 有默认的超时时间，用户预估的导入数据量过大，就需要修改系统的 Insert Into 导入超时时间。

```
导入数据量 = 36G 约≤ 3600s * 10M/s 
其中 10M/s 是最大导入限速，用户需要根据当前集群情况计算出平均的导入速度来替换公式中的 10M/s
```

### 完整例子

用户有一张表 store\_sales 在数据库 sales 中，用户又创建了一张表叫 bj\_store\_sales 也在数据库 sales 中，用户希望将 store\_sales 中销售记录在 bj 的数据导入到这张新建的表 bj\_store\_sales 中。导入的数据量约为：10G。

```
store_sales schema：
(id, total, user_id, sale_timestamp, region)

bj_store_sales schema:
(id, total, user_id, sale_timestamp)

```

集群情况：用户当前集群的平均导入速度约为 5M/s

+ Step1: 判断是否要修改 Insert Into 的默认超时时间

    ```
    计算导入的大概时间
    10G / 5M/s = 2000s
    
    修改 FE 配置
    insert_load_default_timeout_second = 2000
    ```
    
+ Step2：创建导入任务

    由于用户是希望将一张表中的数据做 ETL 并导入到目标表中，所以应该使用 Insert into query\_stmt 方式导入。

    ```
    INSERT INTO bj_store_sales WITH LABEL `label` SELECT id, total, user_id, sale_timestamp FROM store_sales where region = "bj";
    ```

## 常见问题

* 查看错误行

    由于 Insert Into 无法控制错误率，只能通过 `enable_insert_strict` 设置为完全容忍错误数据或完全忽略错误数据。因此如果 `enable_insert_strict` 设为 true，则 Insert Into 可能会失败。而如果 `enable_insert_strict` 设为 false，则可能出现仅导入了部分合格数据的情况。

    当返回结果中提供了 url 字段时，可以通过以下命令查看错误行：

    ```SHOW LOAD WARNINGS ON "url";```

    示例：

    ```SHOW LOAD WARNINGS ON "http://ip:port/api/_load_error_log?file=__shard_13/error_log_insert_stmt_d2cac0a0a16d482d-9041c949a4b71605_d2cac0a0a16d482d_9041c949a4b71605";```

    错误的原因通常如：源数据列长度超过目的数据列长度、列类型不匹配、分区不匹配、列顺序不匹配等等。
