---
{
    "title": "Group Commit",
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

# Group Commit

攒批写入没有引入一种新的导入方式，而是对`INSERT INTO tbl VALUES(...)`、`Stream Load`、`Http Stream`的扩展。

在 Doris 中，所有的数据写入都是一个独立的导入作业，发起一个新的事务，产生一个新的数据版本。在高频写入的场景下，对transaction和compaction都产生了较大的压力。攒批写通过把多个小的写入合成一个写入作业，减少了transaction和compaction的次数，缓解了系统内部的压力，提高了写入的性能。

## 攒批模式

攒批写入有三种模式，分别是：

* `off_mode`

不开启攒批，保持以上三种导入方式的默认行为。

* `sync_mode`

多个客户端发起的导入复用一个内部导入，等内部导入成功或失败后，外部导入才会返回。

如果内部导入成功，数据可以立即查出。

* `async_mode`

多个客户端发起的导入复用一个内部导入，内部导入将处理后的数据写入Write Ahead Log(WAL)后，立即返回。

此时，数据不能立即读出。内部导入默认开启10秒后自动提交，等成功后，数据才能读出。

当内部导入因为BE节点重启或内存不足等原因导入失败后，BE会通过WAL重放机制重新导入数据。

## 攒批使用方式

假如表的结构为：
```sql
CREATE TABLE `dt` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NULL,
    `score` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
```

### INSERT INTO VALUES

* 异步模式
```sql
# 配置session变量开启攒批(默认为off_mode),开启异步模式
mysql> set group_commit = async_mode;

# 这里返回的label是group_commit开头的，是真正消费数据的导入关联的label，可以区分出是否攒批了
mysql> insert into dt values(1, 'Bob', 90), (2, 'Alice', 99);
Query OK, 2 rows affected (0.05 sec)
{'label':'group_commit_a145ce07f1c972fc-bd2c54597052a9ad', 'status':'PREPARE', 'txnId':'181508'}

# 可以看出这个label, txn_id和上一个相同，说明是攒到了同一个导入任务中
mysql> insert into dt(id, name) values(3, 'John');
Query OK, 1 row affected (0.01 sec)
{'label':'group_commit_a145ce07f1c972fc-bd2c54597052a9ad', 'status':'PREPARE', 'txnId':'181508'}

# 不能立刻查询到
mysql> select * from dt;
Empty set (0.01 sec)

# 10秒后可以查询到
mysql> select * from dt;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Bob   |    90 |
|    2 | Alice |    99 |
|    3 | John  |  NULL |
+------+-------+-------+
3 rows in set (0.02 sec)
```

* 同步模式
```sql
# 配置session变量开启攒批(默认为off_mode),开启同步模式
mysql> set group_commit = sync_mode;

# 这里返回的label是group_commit开头的，是真正消费数据的导入关联的label，可以区分出是否攒批了
mysql> insert into dt values(4, 'Bob', 90), (5, 'Alice', 99);
Query OK, 2 rows affected (10.06 sec)
{'label':'group_commit_d84ab96c09b60587_ec455a33cb0e9e87', 'status':'PREPARE', 'txnId':'3007', 'query_id':'fc6b94085d704a94-a69bfc9a202e66e2'}

# 数据可以立刻读出
mysql> select * from dt;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Bob   |    90 |
|    2 | Alice |    99 |
|    3 | John  |  NULL |
|    4 | Bob   |    90 |
|    5 | Alice |    99 |
+------+-------+-------+
5 rows in set (0.03 sec)
```

* 关闭攒批
```sql
mysql> set group_commit = off_mode;
```

### Stream Load

假如`data.csv`的内容为：
```sql
6,Amy,60
7,Ross,98
```

* 异步模式
```sql
# 导入时在header中增加"group_commit:async_mode"配置

curl --location-trusted -u {user}:{passwd} -T data.csv -H "group_commit:async_mode"  -H "column_separator:,"  http://{fe_host}:{http_port}/api/db/dt/_stream_load
{
    "TxnId": 7009,
    "Label": "group_commit_c84d2099208436ab_96e33fda01eddba8",
    "Comment": "",
    "GroupCommit": true,
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 2,
    "NumberLoadedRows": 2,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 19,
    "LoadTimeMs": 35,
    "StreamLoadPutTimeMs": 5,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 26
}

# 返回的GroupCommit为true，说明进入了攒批的流程
# 返回的Label是group_commit开头的，是真正消费数据的导入关联的label
```

* 同步模式
```sql
# 导入时在header中增加"group_commit:sync_mode"配置

curl --location-trusted -u {user}:{passwd} -T data.csv -H "group_commit:sync_mode"  -H "column_separator:,"  http://{fe_host}:{http_port}/api/db/dt/_stream_load
{
    "TxnId": 3009,
    "Label": "group_commit_d941bf17f6efcc80_ccf4afdde9881293",
    "Comment": "",
    "GroupCommit": true,
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 2,
    "NumberLoadedRows": 2,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 19,
    "LoadTimeMs": 10044,
    "StreamLoadPutTimeMs": 4,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 10038
}

# 返回的GroupCommit为true，说明进入了攒批的流程
# 返回的Label是group_commit开头的，是真正消费数据的导入关联的label
```

关于 Stream Load 使用的更多详细语法及最佳实践，请参阅 [Stream Load](stream-load-manual.md)。

### Http Stream

* 异步模式
```sql
# 导入时在header中增加"group_commit:async_mode"配置

curl --location-trusted -u {user}:{passwd} -T data.csv  -H "group_commit:async_mode" -H "sql:insert into db.dt select * from http_stream('column_separator'=',', 'format' = 'CSV')"  http://{fe_host}:{http_port}/api/_http_stream
{
    "TxnId": 7011,
    "Label": "group_commit_3b45c5750d5f15e5_703428e462e1ebb0",
    "Comment": "",
    "GroupCommit": true,
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 2,
    "NumberLoadedRows": 2,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 19,
    "LoadTimeMs": 65,
    "StreamLoadPutTimeMs": 41,
    "ReadDataTimeMs": 47,
    "WriteDataTimeMs": 23
}

# 返回的GroupCommit为true，说明进入了攒批的流程
# 返回的Label是group_commit开头的，是真正消费数据的导入关联的label
```

* 同步模式
```sql
# 导入时在header中增加"group_commit:sync_mode"配置

curl --location-trusted -u {user}:{passwd} -T data.csv  -H "group_commit:sync_mode" -H "sql:insert into db.dt select * from http_stream('column_separator'=',', 'format' = 'CSV')"  http://{fe_host}:{http_port}/api/_http_stream
{
    "TxnId": 3011,
    "Label": "group_commit_fe470e6752aadbe6_a8f3ac328b02ea91",
    "Comment": "",
    "GroupCommit": true,
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 2,
    "NumberLoadedRows": 2,
    "NumberFilteredRows": 0,
    "NumberUnselectedRows": 0,
    "LoadBytes": 19,
    "LoadTimeMs": 10066,
    "StreamLoadPutTimeMs": 31,
    "ReadDataTimeMs": 32,
    "WriteDataTimeMs": 10034
}

# 返回的GroupCommit为true，说明进入了攒批的流程
# 返回的Label是group_commit开头的，是真正消费数据的导入关联的label
```

关于 Http Stream 使用的更多详细语法及最佳实践，请参阅 [Stream Load](stream-load-manual.md)。

### 使用`PreparedStatement`

当用户使用JDBC `insert into values`方式写入时，为了减少 SQL 解析和生成规划的开销， 我们在 FE 端支持了 MySQL 协议的`PreparedStatement`特性。当使用`PreparedStatement`时，SQL 和其导入规划将被缓存到 Session 级别的内存缓存中，后续的导入直接使用缓存对象，降低了 FE 的 CPU 压力。下面是在 JDBC 中使用 PreparedStatement 的例子：

1. 设置 JDBC url 并在 Server 端开启 prepared statement

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true
```

2. 开启 `group_commit` session变量，有如下两种方式：

* 通过JDBC url设置，增加`sessionVariables=group_commit=async_mode`

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true&sessionVariables=group_commit=async_mode
```

* 通过执行SQL设置

```
try (Statement statement = conn.createStatement()) {
    statement.execute("SET group_commit = async_mode;");
}
```

3. 使用 `PreparedStatement`

```java
private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
private static final String URL_PATTERN = "jdbc:mysql://%s:%d/%s?useServerPrepStmts=true";
private static final String HOST = "127.0.0.1";
private static final int PORT = 9087;
private static final String DB = "db";
private static final String TBL = "dt";
private static final String USER = "root";
private static final String PASSWD = "";
private static final int INSERT_BATCH_SIZE = 10;

private static void groupCommitInsert() throws Exception {
    Class.forName(JDBC_DRIVER);
    try (Connection conn = DriverManager.getConnection(String.format(URL_PATTERN, HOST, PORT, DB), USER, PASSWD)) {
        // set session variable 'group_commit'
        try (Statement statement = conn.createStatement()) {
            statement.execute("SET group_commit = async_mode;");
        }

        String query = "insert into " + TBL + " values(?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int i = 0; i < INSERT_BATCH_SIZE; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, "name" + i);
                stmt.setInt(3, i + 10);
                int result = stmt.executeUpdate();
                System.out.println("rows: " + result);
            }
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
}   

private static void groupCommitInsertBatch() throws Exception {
    Class.forName(JDBC_DRIVER);
    // add rewriteBatchedStatements=true and cachePrepStmts=true in JDBC url
    // set session variables by sessionVariables=group_commit=async_mode in JDBC url
    try (Connection conn = DriverManager.getConnection(
            String.format(URL_PATTERN + "&rewriteBatchedStatements=true&cachePrepStmts=true&sessionVariables=enable_insert_group_commit=true", HOST, PORT, DB), USER, PASSWD)) {

        String query = "insert into " + TBL + " values(?, ?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            for (int j = 0; j < 5; j++) {
                // 10 rows per insert
                for (int i = 0; i < INSERT_BATCH_SIZE; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, "name" + i);
                    stmt.setInt(3, i + 10);
                    stmt.addBatch();
                }
                int[] result = stmt.executeBatch();
            }
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
}
```

关于**JDBC**的更多用法，参考[使用Insert方式同步数据](../import-scenes/jdbc-load.md)。

## 修改攒批默认提交间隔

攒批的默认提交间隔为10秒，用户可以通过修改表的配置，调整攒批的提交间隔：

```sql
# 修改提交间隔为2秒
ALTER TABLE dt SET ("group_commit_interval_ms"="2000");
```

## 使用限制

* 当开启了攒批模式，系统会判断用户发起的`INSERT INTO VALUES`语句是否符合攒批的条件，如果符合，该语句的执行会进入到攒批写入中。主要的判断逻辑包括：

  + 不是事务写入，即`Begin`; `INSERT INTO VALUES`; `COMMIT`方式

  + 不指定label，即`INSERT INTO dt WITH LABEL {label} VALUES`

  + VALUES中不能包含表达式，即`INSERT INTO dt VALUES (1 + 100)`

  + 不是列更新写入


* 当开启了攒批模式，系统会判断用户发起的`Stream Load`和`Http Stream`是否符合攒批的条件，如果符合，该导入的执行会进入到攒批写入中。主要的判断逻辑包括：

  + 不是两阶段提交

  + 不指定label

  + 不是列更新写入


* 对`max_filter_ratio`语义的支持

  * 在默认的导入中，`filter_ratio`是导入完成后，通过失败的行数和总行数计算，决定是否commit transaction。

  * 在攒批模式下，由于多个用户发起的导入会被一个内部导入执行，虽然可以计算出每个导入的`filter_ratio`，但是数据一旦进入内部导入，就只能commit transaction

  * 但攒批模式支持了一定程度的`max_filter_ratio`语义，当导入的总行数不高于`group_commit_memory_rows_for_max_filter_ratio`(配置在be.conf中，默认为10000行)，会把数据缓存起来，计算出真正的`filter_ratio`，如果超过了`max_filter_ratio`，会把数据丢弃，用户导入失败


* WAL限制

  * 对于`async_mode`的攒批写入，会把数据写入WAL。如果内部写入成功，则WAL被立刻删除；如果内部导入失败，通过导入WAL的方法来恢复数据

  * 目前WAL文件只存储在一个BE上，如果这个BE磁盘损坏或文件误删等，可能导入丢失部分数据。

  * 当下线BE节点时，请使用[`DECOMMISSION`](../../../sql-manual/sql-reference/Cluster-Management-Statements/ALTER-SYSTEM-DECOMMISSION-BACKEND.md)命令，安全下线节点，防止该节点下线前WAL文件还没有全部处理完成，导致部分数据丢失

  * 对于`async_mode`的攒批写入，如果导入数据过大(超过WAL单目录的80%)，或不知道数据量的chunked stream load，为了防止生成的WAL占用太多的磁盘空间，会退化成`sync_mode`

  * 为了防止多个小的导入攒到一个内部导入中，导致WAL占用过多的磁盘空间的问题，当总WAL文件大小超过配置阈值(参考相关系统配置中的`group_commit_wal_max_disk_limit`)时，会阻塞攒批写入，直到磁盘空间释放或超时报错

  * 当发生重量级schema change时，为了保证WAL能够适配表的schema，在schema change最后的fe修改元数据阶段，会拒绝攒批写入，客户端收到`insert table ${table_name} is blocked on schema change`异常，客户端重试即可

## 相关系统配置

### BE 配置

+ group_commit_memory_rows_for_max_filter_ratio

  当导入的总行数不高于该值，会把数据缓存起来，计算出真正的`filter_ratio`，如果超过了`max_filter_ratio`，会把数据丢弃，用户导入失败。默认为10000行。

+ group_commit_replay_wal_dir

  存放WAL文件的目录，默认在用户配置的`storage_root_path`的各个目录下创建一个名为`wal`的目录，如无特殊要求，不需要修改。配置示例：

  ```
  group_commit_replay_wal_dir=/data1/storage/wal;/data2/storage/wal;/data3/storage/wal
  ```

+ group_commit_wal_max_disk_limit

  WAL文件的最大磁盘占用，当总WAL文件大小超过该值时，会阻塞攒批写入，直到磁盘空间释放或超时报错。默认为10%。