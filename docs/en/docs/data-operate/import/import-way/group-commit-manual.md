---
{
    "title": "Group Commit",
    "language": "en"
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

Group commit load does not introduce a new data import method, but an extension of `INSERT INTO tbl VALUS(...)`, `Stream Load` and `Http Stream`. It is a way to improve the write performance of Doris with high-concurrency and small-data writes. Your application can directly use JDBC to do high-concurrency insert operation into Doris, at the same time, combining PreparedStatement can get even higher performance. In logging scenarios, you can also do high-concurrency Stream Load or Http Stream into Doris. 

## Group Commit Mode

Group Commit provides 3 modes:

* `off_mode`

Disable group commit, keep the original behavior for `INSERT INTO VALUES`, `Stream Load` and `Http Stream`.

* `sync_mode`

Doris groups multiple loads into one transaction commit based on the `group_commit_interval` table property. The load is returned after the transaction commit. This mode is suitable for high-concurrency writing scenarios and requires immediate data visibility after the load is finished.

* `async_mode`

Doris writes data to the Write Ahead Log (WAL) firstly, then the load is returned. Doris groups multiple loads into one transaction commit based on the `group_commit_interval` table property, and the data is visible after the commit. To prevent excessive disk space usage by the WAL, it automatically switches to `sync_mode`. This is suitable for latency-sensitive and high-frequency writing.

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

* async_mode
```sql
# Config session variable to enable the async group commit, the default value is off_mode
mysql> set group_commit = async_mode;

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

* sync_mode
```sql
# Config session variable to enable the sync group commit
mysql> set group_commit = sync_mode;

# The retured label is start with 'group_commit', which is the label of the real load job. 
# The insert costs at least the group_commit_interval_ms of table property.
mysql> insert into dt values(4, 'Bob', 90), (5, 'Alice', 99);
Query OK, 2 rows affected (10.06 sec)
{'label':'group_commit_d84ab96c09b60587_ec455a33cb0e9e87', 'status':'PREPARE', 'txnId':'3007', 'query_id':'fc6b94085d704a94-a69bfc9a202e66e2'}

# The data is visible after the insert is returned
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

* off_mode
```sql
mysql> set group_commit = off_mode;
```

### Stream Load

If the content of `data.csv` is:
```sql
6,Amy,60
7,Ross,98
```

* async_mode
```sql
# Add 'group_commit:async_mode' configuration in the http header

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

# The returned 'GroupCommit' is 'true', which means this is a group commit load
# The retured label is start with 'group_commit', which is the label of the real load job
```

* sync_mode
```sql
# Add 'group_commit:sync_mode' configuration in the http header

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

# The returned 'GroupCommit' is 'true', which means this is a group commit load
# The retured label is start with 'group_commit', which is the label of the real load job
```

See [Stream Load](stream-load-manual.md) for more detailed syntax used by **Stream Load**.

### Http Stream

* async_mode
```sql
# Add 'group_commit:async_mode' configuration in the http header

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

# The returned 'GroupCommit' is 'true', which means this is a group commit load
# The retured label is start with 'group_commit', which is the label of the real load job
```

* sync_mode
```sql
# Add 'group_commit:sync_mode' configuration in the http header

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

2. Set `group_commit` session variable, there are two ways to do it:

* Add `sessionVariables=group_commit=async_mode` in JDBC url

```
url = jdbc:mysql://127.0.0.1:9030/db?useServerPrepStmts=true&sessionVariables=group_commit=async_mode
```

* Use `SET group_commit = async_mode;` command

```
try (Statement statement = conn.createStatement()) {
    statement.execute("SET group_commit = async_mode;");
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
            String.format(URL_PATTERN + "&rewriteBatchedStatements=true&cachePrepStmts=true&sessionVariables=group_commit=async_mode", HOST, PORT, DB), USER, PASSWD)) {

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

## Modify the group commit interval

The default group commit interval is 10 seconds. Users can modify the configuration of the table:

```sql
# Modify the group commit interval to 2 seconds
ALTER TABLE dt SET ("group_commit_interval_ms"="2000");
```

## Limitations

* When the group commit is enabled, some `INSERT INTO VALUES` sqls are not executed in the group commit way if they meet the following conditions:

  * Transaction insert, such as `BEGIN`, `INSERT INTO VALUES`, `COMMIT`

  * Specify the label, such as `INSERT INTO dt WITH LABEL {label} VALUES`

  * Expressions within VALUES, such as `INSERT INTO dt VALUES (1 + 100)`

  * Column update

  * Tables that do not support light schema changes

* When the group commit is enabled, some `Stream Load` and `Http Stream` are not executed in the group commit way if they meet the following conditions:

  * Two phase commit

  * Specify the label

  * Column update

  * Tables that do not support light schema changes

* For unique table, because the group commit can not guarantee the commit order, users can use sequence column to ensure the data consistency.

* The limit of `max_filter_ratio`

  * For non group commit load, filter_ratio is calculated by the failed rows and total rows when load is finished. If the filter_ratio does not match, the transaction will not commit

  * In the group commit mode, multiple user loads are executed by one internal load. The internal load will commit all user loads.

  * Currently, group commit supports a certain degree of max_filter_ratio semantics. When the total number of rows does not exceed group_commit_memory_rows_for_max_filter_ratio (configured in `be.conf`, defaulting to `10000` rows), max_filter_ratio will work.

* The limit of WAL

  * For async_mode group commit, data is written to the Write Ahead Log (WAL). If the internal load succeeds, the WAL is immediately deleted. If the internal load fails, data is recovery by importing the WAL.

  * Currently, WAL files are stored only on one disk of one BE. If the BE's disk is damaged or the file is mistakenly deleted, it may result in data loss.

  * When decommissioning a BE node, please use the [`DECOMMISSION`](../../../sql-manual/sql-reference/Cluster-Management-Statements/ALTER-SYSTEM-DECOMMISSION-BACKEND.md) command to safely decommission the node. This prevents potential data loss if the WAL files are not processed before the node is taken offline.

  * For async_mode group commit writes, to protect disk space, it switches to sync_mode under the following conditions:

    * For an import with large amount of data: exceeding 80% of the disk space of a WAL directory. 

    * Chunked stream loads with an unknown data amount.

    * Insufficient disk space, even with it is an import with small amount of data.

  * During hard weight schema changes (adding or dropping columns, modifying varchar length, and renaming columns are lightweight schema changes, others are hard weight), to ensure WAL file is compatibility with the table's schema, the final stage of metadata modification in FE will reject group commit writes. Clients get `insert table ${table_name} is blocked on schema change` exception and can retry the import.

## Relevant system configuration

### BE configuration

#### `group_commit_wal_path`

* The `WAL` directory of group commit.
* Default: A directory named `wal` is created under each directory of the `storage_root_path`. Configuration examples:
  ```
  group_commit_wal_path=/data1/storage/wal;/data2/storage/wal;/data3/storage/wal
  ```

#### `group_commit_memory_rows_for_max_filter_ratio`

* Description: The `max_filter_ratio` limit can only work if the total rows of `group commit` is less than this value.
* Default: 10000

