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

Group commit load does not introduce a new import method, but an extension of `INSERT INTO tbl VALUS(...)`、`Stream Load`、`Http Stream`.

In Doris, all methods of data loading are independent jobs which initiate a new transaction and generate a new data version. In the scenario of high-frequency writes, both transactions and compactions are under great pressure. Group commit load reduces the number of transactions and compactions by combining multiple small load tasks into one load job, and thus improve write performance.

The process is roughly as follows:
1. User starts a group commit load, BE puts the data into the memory and WAL, and returns immediately. The data is not visible to users at this time;
2. BE will periodically (default is 10 seconds) commit the data in the memory, and the data is visible to users after committed;
3. If BE restarts, the data will be recovered through WAL.

## Fundamental

### Write process
1. User starts a group commit load, FE generates a plan fragment;
2. BE executes the plan. Unlike non group commit load, the processed data is not sent to each tablet, but put into a queue in the memory shared by multiple group commit load;
3. BE starts an internal load, which consumes the data in the queue, writes to WAL, and notifies that the data related load has been finished;
4. After that, the data is processed in the same way as non group commit load, send to each tablet, write memtable, and flushed to segment files;
5. The internal load is finished after a fixed time interval (default is 10 seconds), and the data is visible to users when it is committed.

### WAL Introduction

Each group commit load will generate a corresponding WAL file, which is used to recover failed load jobs. If there is a restart be or fail to run the group commit load during the writing process, be will replay WAL file through a stream load in the background to reimport the data, which can make sure that data is not lost. If the group commit load job is completed normally, the WAL will be directly deleted to reduce disk space usage.

## Basic operations

If the table schema is:
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
# Config session variable to enable the group commit, the default value is false
mysql> set enable_insert_group_commit = true;

# The retured label is start with 'group_commit', which is the label of the real load job
mysql> insert into dt values(1, 'Bob', 90), (2, 'Alice', 99);
Query OK, 2 rows affected (0.05 sec)
{'label':'group_commit_a145ce07f1c972fc-bd2c54597052a9ad', 'status':'PREPARE', 'txnId':'181508'}

# The returned label and txn_id are the same as the above, which means they are handled in on load job  
mysql> insert into dt(id, name) values(3, 'John');
Query OK, 1 row affected (0.01 sec)
{'label':'group_commit_a145ce07f1c972fc-bd2c54597052a9ad', 'status':'PREPARE', 'txnId':'181508'}

# The data is not visible
mysql> select * from dt;
Empty set (0.01 sec)

# After about 10 seconds, the data is visible
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

If the content of `data.csv` is:
```sql
4,Amy,60
5,Ross,98
```

```sql
# Add 'group_commit:true' configuration in the http header

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

# The returned 'GroupCommit' is 'true', which means this is a group commit load
# The retured label is start with 'group_commit', which is the label of the real load job
```

See [Stream Load](stream-load-manual.md) for more detailed syntax used by **Stream Load**.

### Http Stream

```sql
# Add 'group_commit:true' configuration in the http header

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

# The returned 'GroupCommit' is 'true', which means this is a group commit load
# The retured label is start with 'group_commit', which is the label of the real load job
```

See [Stream Load](stream-load-manual.md) for more detailed syntax used by **Http Stream**.

### Use `PreparedStatement`

To reduce the CPU cost of SQL parsing and query planning, we provide the `PreparedStatement` in the FE. When using `PreparedStatement`, the SQL and its plan will be cached in the session level memory cache and will be reused later on, which reduces the CPU cost of FE. The following is an example of using PreparedStatement in JDBC:

1. Setup JDBC url and enable server side prepared statement

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true
```

2. Enable `enable_insert_group_commit` session variable, there are two ways to do it:

* Add `sessionVariables=enable_insert_group_commit=true` in JDBC url

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true&sessionVariables=enable_insert_group_commit=true
```

*Use `SET enable_insert_group_commit = true;` command

```
try (Statement statement = conn.createStatement()) {
    statement.execute("SET enable_insert_group_commit = true;");
}
```

3. Using `PreparedStatement`

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

See [Synchronize Data Using Insert Method](../import-scenes/jdbc-load.md) for more details about **JDBC**.

## Relevant system configuration

### Session variable

+ enable_insert_group_commit

  If this configuration is true, FE will judge whether the `INSERT INTO VALUES` can be group commit, the conditions are as follows:
  + Not a transaction insert, as `Begin`; `INSERT INTO VALUES`; `COMMIT`
  + Not specifying partition, as `INSERT INTO dt PARTITION()`
  + Not specifying label, as `INSERT INTO dt WITH LABEL {label} VALUES`
  + VALUES does not contain any expression, as `INSERT INTO dt VALUES (1 + 100)`

  The default value is false, use `SET enable_insert_group_commit = true;` command to enable it.

### BE configuration

+ group_commit_interval_ms

  The time interval of the internal group commit load job will stop and start a new internal job, the default value is 10000 milliseconds.

+ group_commit_replay_wal_dir

  The directory for storing WAL files. By default, a directory named `wal` is created under each directory of the `storage_root_path`. Users don't need to configure this if there is no special requirement. Configuration examples:

  ```
  group_commit_replay_wal_dir=/data1/storage/wal,/data2/storage/wal,/data3/storage/wal
  ```
