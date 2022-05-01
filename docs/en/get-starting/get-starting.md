---
{
    "title": "Getting-Started",
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

# Getting Started

## Environmental preparation

1. CPU: 2C (minimum) 8C (recommended)
2. Memory: 4G (minimum) 48G (recommended)
3. Hard disk: 100G (minimum) 400G (recommended)
4. Platform: MacOS (Inter), LinuxOS, Windows virtual machine
5. System: CentOS (7.1 and above), Ubuntu (16.04 and above)
6. Software: JDK (1.8 and above), GCC (4.8.2 and above)

## Stand-alone deployment

**Before creating, please prepare the compiled FE/BE file, this tutorial will not repeat the compilation process. **

1. Set the maximum number of open file handles in the system

   ```shell
   vi /etc/security/limits.conf 
   # Add the following two lines of information
   * soft nofile 65536
   * hard nofile 65536
   # Save and exit and restart the server
   ```
   
2. Download binary package / self-compile FE / BE files

   ```shell
   wget https://dist.apache.org/repos/dist/release/incubator/doris/version to deploy
   # For example the following link
   wget https://dist.apache.org/repos/dist/release/incubator/doris/1.0/1.0.0-incubating/apache-doris-1.0.0-incubating-bin.tar.gz
   ```
   
3. Extract the tar.gz file

   ```shell
   tar -zxvf Downloaded binary archive
   # example
   tar -zxvf apache-doris-1.0.0-incubating-bin.tar.gz
   ```

4. Migrate the decompressed program files to the specified directory and rename them

   ```shell
   mv [unzipped root directory] [Target path]
   cd [Target path]
   # example
   mv apache-doris-1.0.0-incubating-bin /opt/doris
   cd /opt/doris
   ```

5. Configure FE

   ```shell
   # Configure FE-Config
   vi fe/conf/fe.conf
   # Uncomment priority_networks and modify parameters
   priority_networks = 127.0.0.0/24
   # save and exit
   ```

6. Configure BE

   ```shell
   # Configure BE-Config
   vi be/conf/be.conf
   # Uncomment priority_networks and modify parameters
   priority_networks = 127.0.0.0/24
   # save and exit
   ```

7. Configure environment variables

   ```shell
   # Configure environment variables
   vim /etc/profile.d/doris.sh
   export DORIS_HOME=Doris root path # example:/opt/doris
   export PATH=$PATH:$DORIS_HOME/fe/bin:$DORIS_HOME/be/bin
   # save and source
   source /etc/profile.d/doris.sh
   ```

8. Start FE and BE and register BE to FE

   ```shell
   start_fe.sh --daemon
   start_be.sh --daemon
   ```

   Check whether the FE startup is successful

   > 1. Check whether the startup is successful, and whether there is a PaloFe process under the JPS command
   > 2. After the FE process is started, the metadata will be loaded first. Depending on the role of the FE, you will see transfer from UNKNOWN to MASTER/FOLLOWER/OBSERVER in the log. Finally, you will see the thrift server started log, and you can connect to FE through the mysql client, which means FE started successfully.
   > 3. You can also check whether the startup is successful through the following connection: http://fe_host:fe_http_port/api/bootstrap If it returns: {"status":"OK","msg":"Success"}, it means the startup is successful, and the rest , there may be a problem.
   > 4. Visit http://fe_host:fe_http_port in the external environment to check whether you can access the WebUI interface. The default login account is root and the password is empty.
   >
   > **Note: If you can't see the startup failure information in fe.log, maybe you can see it in fe.out. **

   Verify that BE is successfully started

   > 1. After the BE process is started, if there is data before, it may take several minutes to load the data index.
   > 2. If it is the first start of a BE, or the BE has not joined any cluster, the BE log will periodically scroll waiting to receive first heartbeat from frontend. Indicates that the BE has not received the Master's address through the heart hop of the FE, and is passively waiting. This kind of error log will disappear after ADD BACKEND in FE and sending a heartbeat. If the words master client, get client from cache failed.host: , port: 0, code: 7 appear repeatedly after receiving the heart hop, it means that the FE has successfully connected to the BE, but the BE cannot actively connect to the FE. It may be necessary to check the connectivity of BE to FE's rpc_port.
   > 3. If the BE has been added to the cluster, the heartbeat log from the FE should be scrolled every 5 seconds: get heartbeat, host: xx.xx.xx.xx, port: 9020, cluster id: xxxxxx , Indicates that the heartbeat is normal.
   > 4. Secondly, the words finish report task success. return code: 0 should be scrolled in the log every 10 seconds, indicating that the communication between BE and FE is normal.
   > 5. At the same time, if there is a data query, you should be able to see the log that keeps scrolling, and there is a log of execute time is xxx, indicating that the BE has been started successfully and the query is normal.
   > 6. You can also check whether the startup is successful through the following connection: http://be_host:be_http_port/api/health If it returns: {"status": "OK","msg": "To Be Added"}, it means the startup is successful, In other cases, there may be problems.
   >
   > **Note: If you can't see the startup failure information in be.INFO, maybe you can see it in be.out.**

   Register BE to FE (using MySQL-Client, you need to install it yourself)

   ```shell
   # login
   mysql -h 127.0.0.1 -P 9030 -uroot
   # Register BE
   ALTER SYSTEM ADD BACKEND "127.0.0.1:9050";
   ```

## Apache Doris is easy to use

Doris uses the MySQL protocol for communication, and users can connect to the Doris cluster through MySQL Client or MySQL JDBC. When choosing the MySQL client version, it is recommended to use a version after 5.1, because the user name longer than 16 characters cannot be supported before 5.1. Doris SQL syntax basically covers MySQL syntax.

### Apache Doris Web UI access

By default, Http protocol is used for WebUI access, and the following format address is entered in the browser to access

```
http://FE_IP:FE_HTTP_PORT(默认8030)
```

If the account password is changed during cluster installation, use the new account password to log in

```
Default account: root
Default password: empty
```

1. Introduction to WebUI

   On the home page of FE-WEB-UI, Version and Hardware Info are listed

   The Tab page has the following six modules:

   - Playground (Visual SQL)

   - System (system status)

   - Log (cluster log)

   - QueryProfile (SQL execution log)

   - Session (linked list)

   - Configuration

2. View the BE list

    Go to `System` --> `backends` to view

    What needs to be paid attention to is the `Alive` column, the `True` and `False` of this column represent the normal and abnormal status of the corresponding BE node

3. profile query

    Enter QueryProfile to view the latest 100 SQL execution report information, click the specified ID in the QueryID column to view the Profile information

### MySQL command line/graphical client access

```shell
# Command Line
mysql -h FE-host -P 9030 -u username -p password
# Client (Navicat as an example)
Host: FE-host (if it is a cloud server, public IP is required)
Port: 9030
username: username
password password
````

#### Profile settings

FE splits the query plan into fragments and sends them to BE for task execution. When BE executes Fragment, it records **statistical value of running state**, and outputs the statistics of Fragment execution to the log. FE can also collect these statistical values recorded by each Fragment through switches, and print the results on the FE web page.

- Turn on the Report switch of FE

   ```mysql
   set enable_profile=true;
   ````

- After executing the SQL statement, you can view the corresponding SQL statement execution report information on the FE's WEB-UI interface

For a complete parameter comparison table, please go to [Profile parameter analysis](../admin-manual/query-profile.html) View Details


#### Library table operations

- View database list

  ```mysql
  show databases;
  ````

- create database

   ```mysql
   CREATE DATABASE database name;
   ````

   > For more detailed syntax and best practices used by Create-DataBase, see [Create-DataBase](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-DATABASE.html) command manual.
   >
   > If you don't know the full name of the command, you can use "help command a field" for fuzzy query. If you type 'HELP CREATE', you can match `CREATE DATABASE`, `CREATE TABLE`, `CREATE USER` and other commands.
   
   After the database is created, you can view the database information through `SHOW DATABASES;`.
   
   ```mysql
   MySQL> SHOW DATABASES;
   +--------------------+
   | Database |
   +--------------------+
   | example_db |
   | information_schema |
   +--------------------+
   2 rows in set (0.00 sec)
   ````
   
   `information_schema` exists to be compatible with the MySQL protocol. In practice, the information may not be very accurate, so it is recommended to obtain information about a specific database by directly querying the corresponding database.
   
- Create data table

  > For more detailed syntax and best practices used by Create-Table, see [Create-Table](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) command manual.

  Use the `CREATE TABLE` command to create a table (Table). More detailed parameters can be viewed:

  ```mysql
  HELP CREATE TABLE;
  ````

  First switch the database:

  ```mysql
  USE example_db;
  ````

  Doris supports two table creation methods, single partition and composite partition (for details, please refer to [Create-Table](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) command manual).

  The following takes the aggregation model as an example to demonstrate the table building statements for two partitions.

  - single partition

    Create a logical table named table1. The bucketed column is siteid, and the number of buckets is 10.

    The schema for this table is as follows:

    - siteid: type is INT (4 bytes), default value is 10
    - citycode: type is SMALLINT (2 bytes)
    - username: the type is VARCHAR, the maximum length is 32, the default value is an empty string
    - pv: the type is BIGINT (8 bytes), the default value is 0; this is an indicator column, Doris will perform aggregation operations on the indicator column internally, and the aggregation method of this column is sum (SUM)

    The table creation statement is as follows:

    ```mysql
    CREATE TABLE table1
    (
        siteid INT DEFAULT '10',
        citycode SMALLINT,
        username VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0'
    )
    AGGREGATE KEY(siteid, citycode, username)
    DISTRIBUTED BY HASH(siteid) BUCKETS 10
    PROPERTIES("replication_num" = "1");
    ```

  - composite partition

    Create a logical table named table2.

    The schema for this table is as follows:

    - event_day: type is DATE, no default value
    - siteid: type is INT (4 bytes), default value is 10
    - citycode: type is SMALLINT (2 bytes)
    - username: the type is VARCHAR, the maximum length is 32, the default value is an empty string
    - pv: The type is BIGINT (8 bytes), the default value is 0; this is an indicator column, Doris will perform aggregation operations on the indicator column internally, and the aggregation method of this column is sum (SUM)

    We use the event_day column as the partition column and create 3 partitions: p201706, p201707, p201708

    - p201706: Range is [Min, 2017-07-01)

    - p201707: The range is [2017-07-01, 2017-08-01)

    - p201708: The range is [2017-08-01, 2017-09-01)

       > Note that the interval is left closed and right open.

    Each partition uses siteid for hash bucketing, and the number of buckets is 10

    The table creation statement is as follows:

  - ```mysql
    CREATE TABLE table2
    (
        event_day DATE,
        siteid INT DEFAULT '10',
        citycode SMALLINT,
        username VARCHAR(32) DEFAULT '',
        pv BIGINT SUM DEFAULT '0'
    )
    AGGREGATE KEY(event_day, siteid, citycode, username)
    PARTITION BY RANGE(event_day)
    (
        PARTITION p201706 VALUES LESS THAN ('2017-07-01'),
        PARTITION p201707 VALUES LESS THAN ('2017-08-01'),
        PARTITION p201708 VALUES LESS THAN ('2017-09-01')
    )
    DISTRIBUTED BY HASH(siteid) BUCKETS 10
    PROPERTIES("replication_num" = "1");
    ```

    After the table is created, you can view the information of the table in example_db:

    ```mysql
    MySQL> SHOW TABLES;
    +----------------------+
    | Tables_in_example_db |
    +----------------------+
    | table1               |
    | table2               |
    +----------------------+
    2 rows in set (0.01 sec)
    
    MySQL> DESC table1;
    +----------+-------------+------+-------+---------+-------+
    | Field    | Type        | Null | Key   | Default | Extra |
    +----------+-------------+------+-------+---------+-------+
    | siteid   | int(11)     | Yes  | true  | 10      |       |
    | citycode | smallint(6) | Yes  | true  | N/A     |       |
    | username | varchar(32) | Yes  | true  |         |       |
    | pv       | bigint(20)  | Yes  | false | 0       | SUM   |
    +----------+-------------+------+-------+---------+-------+
    4 rows in set (0.00 sec)
    
    MySQL> DESC table2;
    +-----------+-------------+------+-------+---------+-------+
    | Field     | Type        | Null | Key   | Default | Extra |
    +-----------+-------------+------+-------+---------+-------+
    | event_day | date        | Yes  | true  | N/A     |       |
    | siteid    | int(11)     | Yes  | true  | 10      |       |
    | citycode  | smallint(6) | Yes  | true  | N/A     |       |
    | username  | varchar(32) | Yes  | true  |         |       |
    | pv        | bigint(20)  | Yes  | false | 0       | SUM   |
    +-----------+-------------+------+-------+---------+-------+
    5 rows in set (0.00 sec)
    ```

    > Precautions:
    >
    > 1. The above tables are all single-copy tables by setting replication_num. Doris recommends that users use the default 3-copy setting to ensure high availability.
    > 2. You can dynamically add or delete partitions to the composite partition table. See the Partition section in `HELP ALTER TABLE` for details.
    > 3. Data import can import the specified Partition. See `HELP LOAD` for details.
    > 4. The schema of the table can be dynamically modified.
    > 5. You can add Rollup to Table to improve query performance. For this part, please refer to the description of Rollup in the Advanced User Guide.
    > 6. The Null attribute of the table column defaults to true, which will have a certain impact on query performance.

#### Insert Data

1. Insert Into

   > For more detailed syntax and best practices for Insert usage, see [Insert](../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.html) Command Manual.

   The Insert Into statement is used in a similar way to the Insert Into statement in databases such as MySQL. But in Doris, all data writing is a separate import job. Therefore, Insert Into is also introduced as an import method here.

   The main Insert Into commands include the following two;

   - INSERT INTO tbl SELECT ...
   - INSERT INTO tbl (col1, col2, ...) VALUES (1, 2, ...), (1,3, ...);

   The second command is for Demo only, not in test or production environments.

   The Insert Into command needs to be submitted through the MySQL protocol. Creating an import request will return the import result synchronously.

   grammar:

   ```mysql
   INSERT INTO table_name [partition_info] [WITH LABEL label] [col_list] [query_stmt] [VALUES];
   ```

   Example:

   ```mysql
   INSERT INTO tbl2 WITH LABEL label1 SELECT * FROM tbl3;
   INSERT INTO tbl1 VALUES ("qweasdzxcqweasdzxc"),("a");
   ```

   **Notice**

   When using `CTE(Common Table Expressions)` as the query part in an insert operation, the `WITH LABEL` and `column list` parts must be specified. Example

   ```mysql
   INSERT INTO tbl1 WITH LABEL label1
   WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
   SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1;
   
   INSERT INTO tbl1 (k1)
   WITH cte1 AS (SELECT * FROM tbl1), cte2 AS (SELECT * FROM tbl2)
   SELECT k1 FROM cte1 JOIN cte2 WHERE cte1.k1 = 1;
   ```

   Insert Into itself is an SQL command, and its return results are divided into the following types according to the different execution results:

   - If the return result is `ERROR 1064 (HY000)`, it means the import failed.
   - If the returned result is `Query OK`, the execution is successful.
      - If `rows affected` is 0, the result set is empty and no data is imported.
      - If `rows affected` is greater than 0:
        - If `status` is `committed`, the data is not yet visible. Need to view status through `show transaction` statement until `visible`
        - If `status` is `visible`, the data import is successful.
      - If `warnings` is greater than 0, it means that data is filtered. You can get the url through the `show load` statement to view the filtered lines.

   For more detailed instructions, see the [Insert](../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/INSERT.html) command manual.

2. Batch Import

   Doris supports multiple data import methods. For details, please refer to the data import documentation. Here we use Stream-Load and Broker-Load as an example.

   - Stream-Load

     > For more detailed syntax and best practices used by Stream-Load, see [Stream-Load](../sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD.html) command manual.

     Streaming import transfers data to Doris through the HTTP protocol, and can directly import local data without relying on other systems or components. See `HELP STREAM LOAD;` for detailed syntax help.

     Example 1: With "table1_20170707" as the Label, use the local file table1_data to import the table1 table.

     ```CQL
     curl --location-trusted -u test:test_passwd -H "label:table1_20170707" -H "column_separator:," -T table1_data http://FE_HOST:8030/api/example_db/table1/_stream_load
     ```

     > 1. FE_HOST is the IP of the node where any FE is located, and 8030 is the http_port in fe.conf.
     > 2. You can use the IP of any BE and the webserver_port in be.conf to import. For example: `BE_HOST:8040`

     The local file `table1_data` uses `,` as the separation between data, the details are as follows:

     ```text
     1,1,jim,2
     2,1,grace,2
     3,2,tom,2
     4,3,bush,3
     5,3,helen,3
     ```

     Example 2: With "table2_20170707" as the Label, use the local file table2_data to import the table2 table.

     ```CQL
     curl --location-trusted -u test:test -H "label:table2_20170707" -H "column_separator:|" -T table2_data http://127.0.0.1:8030/api/example_db/table2/_stream_load
     ```

     The local file `table2_data` uses `|` as the separation between data, the details are as follows:

     ```text
     2017-07-03|1|1|jim|2
     2017-07-05|2|1|grace|2
     2017-07-12|3|2|tom|2
     2017-07-15|4|3|bush|3
     2017-07-12|5|3|helen|3
     ```

     > Precautions:
     >
     > 1. It is recommended to use streaming import to limit the file size to 10GB. Too large files will increase the cost of failed retry attempts.
     > 2. Each batch of imported data needs to take a Label. The Label should preferably be a string related to a batch of data, which is easy to read and manage. Based on Label, Doris guarantees that within a Database, the same batch of data can only be successfully imported once. Labels of failed tasks can be reused.
     > 3. Streaming import is a synchronous command. If the command returns successfully, it means that the data has been imported, and if it returns a failure, it means that the batch of data has not been imported.

   - Broker-Load

     Broker import uses the deployed Broker process to read data on external storage for import.

     > For more detailed syntax and best practices used by Broker Load, see [Broker Load](../sql-manual/sql-reference/Data-Manipulation-Statements/Load/BROKER-LOAD.html) command manual, you can also enter `HELP BROKER LOAD` in the MySql client command line for more help information.

     Example: With "table1_20170708" as the Label, import the files on HDFS into table1

        ```mysql
        LOAD LABEL table1_20170708
        (
            DATA INFILE("hdfs://your.namenode.host:port/dir/table1_data")
            INTO TABLE table1
        )
        WITH BROKER hdfs 
        (
            "username"="hdfs_user",
            "password"="hdfs_password"
        )
        PROPERTIES
        (
            "timeout"="3600",
            "max_filter_ratio"="0.1"
        );
        ```

     Broker imports are asynchronous commands. The successful execution of the above command only means that the submitted task is successful. Whether the import is successful needs to be checked through `SHOW LOAD;`. Such as:

        ```mysql
     SHOW LOAD WHERE LABEL = "table1_20170708";
        ```

     In the returned result, if the `State` field is FINISHED, the import is successful.

     For more information on `SHOW LOAD`, see `HELP SHOW LOAD;`

     Asynchronous import tasks can be canceled before they finish:

        ```mysql
     CANCEL LOAD WHERE LABEL = "table1_20170708";
        ```

#### Query Data

1. Simple query

   ```mysql
   MySQL> SELECT * FROM table1 LIMIT 3;
   +--------+----------+----------+------+
   | siteid | citycode | username | pv   |
   +--------+----------+----------+------+
   |      2 |        1 | 'grace'  |    2 |
   |      5 |        3 | 'helen'  |    3 |
   |      3 |        2 | 'tom'    |    2 |
   +--------+----------+----------+------+
   3 rows in set (0.01 sec)
   
   MySQL> SELECT * FROM table1 ORDER BY citycode;
   +--------+----------+----------+------+
   | siteid | citycode | username | pv   |
   +--------+----------+----------+------+
   |      2 |        1 | 'grace'  |    2 |
   |      1 |        1 | 'jim'    |    2 |
   |      3 |        2 | 'tom'    |    2 |
   |      4 |        3 | 'bush'   |    3 |
   |      5 |        3 | 'helen'  |    3 |
   +--------+----------+----------+------+
   5 rows in set (0.01 sec)
   ```

2. Join query

   ```mysql
   MySQL> SELECT SUM(table1.pv) FROM table1 JOIN table2 WHERE table1.siteid = table2.siteid;
   +--------------------+
   | sum(`table1`.`pv`) |
   +--------------------+
   |                 12 |
   +--------------------+
   1 row in set (0.20 sec)
   ```

3. subquery

   ```mysql
   MySQL> SELECT SUM(pv) FROM table2 WHERE siteid IN (SELECT siteid FROM table1 WHERE siteid > 2);
   +-----------+
   | sum(`pv`) |
   +-----------+
   |         8 |
   +-----------+
   1 row in set (0.13 sec)
   ```

#### Update Data

> For more detailed syntax and best practices used by Update, see [Update](../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/UPDATE.html) Command Manual.

The current UPDATE statement **only supports** row updates on the Unique model, and there may be data conflicts caused by concurrent updates. At present, Doris does not deal with such problems, and users need to avoid such problems from the business side.

1. grammar rules

   ```text
   UPDATE table_name 
       SET assignment_list
       WHERE expression
   
   value:
       {expr | DEFAULT}
   
   assignment:
       col_name = value
   
   assignment_list:
       assignment [, assignment] ...
   ```

   **Parameter Description**

   - table_name: The target table of the data to be updated. Can be of the form 'db_name.table_name'
   - assignment_list: the target column to be updated, in the format 'col_name = value, col_name = value'
   - where expression: the condition that is expected to be updated, an expression that returns true or false can be

2. Example

   The `test` table is a unique model table, which contains four columns: k1, k2, v1, v2. Where k1, k2 are keys, v1, v2 are values, and the aggregation method is Replace.

   1. Update the v1 column in the 'test' table that satisfies the conditions k1 =1 , k2 =2 to 1

      ```sql
      UPDATE test SET v1 = 1 WHERE k1=1 and k2=2;
      ```

   2. Increment the v1 column of the column k1=1 in the 'test' table by 1

      ```sql
      UPDATE test SET v1 = v1+1 WHERE k1=1;
      ```

#### Delete Data

> For more detailed syntax and best practices for Delete use, see [Delete](../sql-manual/sql-reference/Data-Manipulation-Statements/Manipulation/DELETE.html) Command Manual.

1. Grammar rules

   This statement is used to conditionally delete data in the specified table (base index) partition.

   This operation will also delete the data of the rollup index related to this base index.
   grammar:

   ```sql
   DELETE FROM table_name [PARTITION partition_name | PARTITIONS (p1, p2)]
   WHERE
   column_name1 op { value | value_list } [ AND column_name2 op { value | value_list } ...];
   ````

   illustrate:

    - Optional types of op include: =, >, <, >=, <=, !=, in, not in

    - Only conditions on the key column can be specified.

    - Cannot delete when the selected key column does not exist in a rollup.

    - Conditions can only have an AND relationship.

      If you want to achieve an "or" relationship, you need to write the conditions in two DELETE statements.

    - If it is a partitioned table, you can specify a partition, if not specified and the session variable delete_without_partition is true, it will be applied to all partitions. If it is a single-partition table, it can be left unspecified.

   Notice:

   - This statement may reduce query efficiency for a period of time after execution.
   - The degree of impact depends on the number of delete conditions specified in the statement.
   - The more conditions you specify, the greater the impact.

2. Example

    1. Delete the data row whose k1 column value is 3 in my_table partition p1

       ```sql
       DELETE FROM my_table PARTITION p1 WHERE k1 = 3;
       ````

    2. Delete the data rows where the value of column k1 is greater than or equal to 3 and the value of column k2 is "abc" in my_table partition p1

       ```sql
       DELETE FROM my_table PARTITION p1 WHERE k1 >= 3 AND k2 = "abc";
       ````

    3. Delete the data rows where the value of column k1 is greater than or equal to 3 and the value of column k2 is "abc" in my_table partition p1, p2

       ```sql
       DELETE FROM my_table PARTITIONS (p1, p2) WHERE k1 >= 3 AND k2 = "abc";
       ````
