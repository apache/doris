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

攒批写入没有引入一种新的导入方式，而是对`INSERT INTO tbl VALUS(...)`、`Stream Load`、`Http Stream`的扩展。

在 Doris 中，所有的数据写入都是一个独立的导入作业，发起一个新的事务，产生一个新的数据版本。在高频写入的场景下，对transaction和compaction都产生了较大的压力。攒批写通过把多个小的写入合成一个写入作业，减少了transaction和compaction的次数，缓解了系统内部的压力，提高了写入的性能。

流程大致为：
1. 用户发起的导入，BE把处理后的数据写入内存和WAL中即返回，此时不能查询到数据；
2. 正常情况下，BE内部周期性(默认为10秒间隔)将内存中的数据提交，提交之后数据对用户可见；
3. 如果发生BE重启等，通过WAL走写入流程恢复数据。

## 原理介绍

### 写入流程

1. 用户发起攒批写入，FE生成执行计划；
2. BE执行规划，与非攒批导入不同，处理后的数据不是发给各个tablet，而是放到一个内存中的队列中，多个攒批共享这个队列；
3. BE内部发起一个导入规划，消费队列中的数据，写入WAL，并通知该数据对应的写入已完成；
4. 之后，消费后的数据和普通写入的处理流程一样，发给各个tablet，写入memtable，下刷为segment文件等；
5. BE内部发起的导入在达到固定的攒批时间(默认为10秒)后，开始提交，提交完成后，数据对用户可见。

### WAL介绍

每一次攒批会生成一个对应的WAL文件，其作用是用于恢复失败的攒批作业，在写入过程中如果发生了be重启或者攒批作业运行失败，be可以通过relay WAL文件，在后台发起一个stream load重新导入数据，保证攒批数据不丢失。如果攒批作业正常执行完成，WAL会被直接删掉。

## 基本操作

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

```sql
# 配置session变量开启攒批,默认为false
mysql> set enable_insert_group_commit = true;

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

### Stream Load

假如`data.csv`的内容为：
```sql
4,Amy,60
5,Ross,98
```

```sql
# 导入时在header中增加"group_commit:true"配置

curl --location-trusted -u {user}:{passwd} -T data.csv -H "group_commit:true"  -H "column_separator:,"  http://{fe_host}:{http_port}/api/db/dt/_stream_load
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

关于 Stream Load 使用的更多详细语法及最佳实践，请参阅 [Stream Load](stream-load-manual.md)。

### Http Stream

```sql
# 导入时在header中增加"group_commit:true"配置

curl --location-trusted -u {user}:{passwd} -T data.csv  -H "group_commit:true" -H "sql:insert into db.dt select * from http_stream('column_separator'=',', 'format' = 'CSV')"  http://{fe_host}:{http_port}/api/_http_stream
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

关于 Http Stream 使用的更多详细语法及最佳实践，请参阅 [Stream Load](stream-load-manual.md)。

### 使用`PreparedStatement`

为了减少 SQL 解析和生成规划的开销， 我们在 FE 端支持了 MySQL 协议的`PreparedStatement`特性。当使用`PreparedStatement`时，SQL 和其导入规划将被缓存到 Session 级别的内存缓存中，后续的导入直接使用缓存对象，降低了 FE 的 CPU 压力。下面是在 JDBC 中使用 PreparedStatement 的例子：

1. 设置 JDBC url 并在 Server 端开启 prepared statement

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true
```

2. 开启 `enable_insert_group_commit` session变量，有如下两种方式：

* 通过JDBC url设置，增加`sessionVariables=enable_insert_group_commit=true`

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true&sessionVariables=enable_insert_group_commit=true
```

* 通过执行SQL设置

```
try (Statement statement = conn.createStatement()) {
    statement.execute("SET enable_insert_group_commit = true;");
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
        // enable session variable 'enable_insert_group_commit'
        try (Statement statement = conn.createStatement()) {
            statement.execute("SET enable_insert_group_commit = true;");
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
    // enable session variables by sessionVariables=enable_insert_group_commit=true in JDBC url
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

## 相关系统配置

### Session变量

+ enable_insert_group_commit

  当该参数设置为 true 时，会判断用户发起的`INSERT INTO VALUES`语句是否符合攒批的条件，如果符合，该语句的执行会进入到攒批写入中。主要的判断逻辑包括：
  + 不是事务写入，即`Begin`; `INSERT INTO VALUES`; `COMMIT`方式
  + 不指定partition，即`INSERT INTO dt PARTITION()`等指定partition的语句
  + 不指定label，即`INSERT INTO dt WITH LABEL {label} VALUES`
  + VALUES中不能包含表达式，即`INSERT INTO dt VALUES (1 + 100)`

  默认为 false。可通过 `SET enable_insert_group_commit = true;` 来设置。

### BE 配置

+ group_commit_interval_ms

  攒批写入开启多久后结束，单位为毫秒，默认为10000，即10秒。

+ group_commit_replay_wal_dir

  存放WAL文件的目录，默认在用户配置的`storage_root_path`的各个目录下创建一个名为`wal`的目录，如无特殊要求，不需要修改。配置示例：

  ```
  group_commit_replay_wal_dir=/data1/storage/wal,/data2/storage/wal,/data3/storage/wal
  ```