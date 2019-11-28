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
INSERT INTO table_name [WITH LABEL label] [partition_info] [col_list] [query_stmt] [VALUES];
```

示例：

```
INSERT INTO tbl2 WITH LABEL label1 SELECT * FROM tbl3;
INSERT INTO tbl1 VALUES ("qweasdzxcqweasdzxc"), ("a");
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

Insert Into 本身就是一个 SQL 命令，所以返回的行为同 SQL 命令的返回行为。

如果导入失败，则返回语句执行失败。示例如下：

```ERROR 1064 (HY000): all partitions have no load data. url: http://ip:port/api/_load_error_log?file=__shard_14/error_log_insert_stmt_f435264d82f342e4-a33764f5f0dfbf00_f435264d82f342e4_a33764f5f0dfbf00```

其中 url 可以用于查询错误的数据，具体见后面 **查看错误行** 小结。

如果导入成功，则返回语句执行成功。示例如下：

```
Query OK, 100 row affected, 0 warning (0.22 sec)
```

如果用户指定了 Label，则会也会返回 Label
```
Query OK, 100 row affected, 0 warning (0.22 sec)
{'label':'user_specified_label'}
```

导入可能部分成功，则会附加 Label 字段。示例如下：

```
Query OK, 100 row affected, 1 warning (0.23 sec)
{'label':'7d66c457-658b-4a3e-bdcf-8beee872ef2c'}
```

```
Query OK, 100 row affected, 1 warning (0.23 sec)
{'label':'user_specified_label'}
```

其中 affected 表示导入的行数。warning 表示失败的行数。用户需要通过 `SHOW LOAD WHERE LABEL="xxx";` 命令，获取 url 查看错误行。

如果没有任何数据，也会返回成功，且 affected 和 warning 都是 0。

Label 是该 Insert Into 导入作业的标识。每个导入作业，都有一个在单 database 内部唯一的 Label。Insert Into 的 Label 则是由系统生成的，用户可以拿着这个 Label 通过查询导入命令异步获取导入状态。
    
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
1. 用户希望仅导入几条假数据，验证一下 Doris 系统的功能。此时适合使用 INSERT INTO VALUS 的语法。
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
