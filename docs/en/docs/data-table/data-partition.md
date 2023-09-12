---
{
    "title": "Data Partition",
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

# Data Partition

This topic is about table creation and data partitioning in Doris, including the common problems in table creation and their solutions.

## Basic Concepts

In Doris, data is logically described in the form of table.

### Row & Column

A table contains rows and columns. 

Row refers to a row of user data. Column is used to describe different fields in a row of data.

Columns can be divided into two categories: Key and Value. From a business perspective, Key and Value correspond to dimension columns and metric columns, respectively. The key column of Doris is the column specified in the table creation statement. The column after the keyword 'unique key' or 'aggregate key' or 'duplicate key' in the table creation statement is the key column, and the rest except the key column is the value column. In the Aggregate Model, rows with the same values in Key columns will be aggregated into one row. The way how Value columns are aggregated is specified by the user when the table is built. For more information about the Aggregate Model, please see the [Data Model](./data-model.md).

### Tablet & Partition

In the Doris storage engine, user data are horizontally divided into data tablets (also known as data buckets). Each tablet contains several rows of data. The data between the individual tablets do not intersect and is physically stored independently.

Tablets are logically attributed to different Partitions. One Tablet belongs to only one Partition, and one Partition contains several Tablets. Since the tablets are physically stored independently, the partitions can be seen as physically independent, too. Tablet is the smallest physical storage unit for data operations such as movement and replication.

A Table is formed of multiple Partitions. Partition can be thought of as the smallest logical unit of management. Data import and deletion can be performed on only one Partition.

## Data Partitioning

The following illustrates on data partitioning in Doris using the example of a CREATE TABLE operation.

CREATE TABLE in Doris is a synchronous command. It returns results after the SQL execution is completed. Successful returns indicate successful table creation. For more information on the syntax, please refer to [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md), or input  the `HELP CREATE TABLE;` command. 

This section introduces how to create tables in Doris.

```sql
-- Range Partition

CREATE TABLE IF NOT EXISTS example_db.example_range_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "User ID",
    `date` DATE NOT NULL COMMENT "Date when the data are imported",
    `timestamp` DATETIME NOT NULL COMMENT "Timestamp when the data are imported",
    `city` VARCHAR(20) COMMENT "User location city",
    `age` SMALLINT COMMENT "User age",
    `sex` TINYINT COMMENT "User gender",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "User last visit time",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "Total user consumption",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "Maximum user dwell time",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "Minimum user dwell time"
)
ENGINE=olap
AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
PARTITION BY RANGE(`date`)
(
    PARTITION `p201701` VALUES LESS THAN ("2017-02-01"),
    PARTITION `p201702` VALUES LESS THAN ("2017-03-01"),
    PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "3",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2018-01-01 12:00:00"
);


-- List Partition

CREATE TABLE IF NOT EXISTS example_db.example_list_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "User ID",
    `date` DATE NOT NULL COMMENT "Date when the data are imported",
    `timestamp` DATETIME NOT NULL COMMENT "Timestamp when the data are imported",
    `city` VARCHAR(20) COMMENT "User location city",
    `age` SMALLINT COMMENT "User Age",
    `sex` TINYINT COMMENT "User gender",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "User last visit time",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "Total user consumption",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "Maximum user dwell time",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "Minimum user dwell time"
)
ENGINE=olap
AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
PARTITION BY LIST(`city`)
(
    PARTITION `p_cn` VALUES IN ("Beijing", "Shanghai", "Hong Kong"),
    PARTITION `p_usa` VALUES IN ("New York", "San Francisco"),
    PARTITION `p_jp` VALUES IN ("Tokyo")
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
    "replication_num" = "3",
    "storage_medium" = "SSD",
    "storage_cooldown_time" = "2018-01-01 12:00:00"
);

```

### Definition of Column

Here we only use the AGGREGATE KEY data model as an example. See [Doris Data Model](./data-model.md) for more information.

You can view the basic types of columns by executing `HELP CREATE TABLE;` in MySQL Client.

In the AGGREGATE KEY data model, all columns that are specified with an aggregation type (SUM, REPLACE, MAX, or MIN) are Value columns. The rest are the Key columns.

A few suggested rules for defining columns include:

1. The Key columns must precede all Value columns.
2. Try to choose the INT type as much as possible. Because calculations and lookups on INT types are much more efficient than those on strings.
3. For the lengths of the INT types, follow the **good enough** principle.
4. For the lengths of the VARCHAR and STRING types, also follow the **good enough** principle.

### Partitioning and Bucketing

Doris supports two layers of data partitioning. The first level is Partition, including range partitioning and list partitioning. The second is Bucket (Tablet), including hash and random partitioning.

It is also possible to use one layer of data partitioning, If you do not write the partition statement when creating the table, Doris will generate a default partition at this time, which is transparent to the user. In this case, it only supports data bucketing.

1. Partition

   * You can specify one or more columns as the partitioning columns, but they have to be KEY columns. The usage of multi-column partitions is described further below. 
   * Regardless of the type of the partitioning columns, double quotes are required for partition values.
   * There is no theoretical limit on the number of partitions.
   * If users create a table without specifying the partitions, the system will automatically generate a Partition with the same name as the table. This Partition contains all data in the table and is neither visible to users nor modifiable.
   * **Partitions should not have overlapping ranges**.

   #### Range Partitioning

   * Partitioning columns are usually time columns for easy management of old and new data.

   * Range partitioning support column type: [DATE,DATETIME,TINYINT,SMALLINT,INT,BIGINT,LARGEINT]

   * Range partitioning supports specifying only the upper bound by `VALUES LESS THAN (...)`. The system will use the upper bound of the previous partition as the lower bound of the next partition, and generate a left-closed right-open interval. It also supports specifying both the upper and lower bounds by `VALUES [...)`, and generate a left-closed right-open interval.

   * The following takes the `VALUES [...)` method as an example since it is more comprehensible. It shows how the partition ranges change as we use the  `VALUES LESS THAN (...)` statement to add or delete partitions:

     * As in the `example_range_tbl` example above, when the table is created, the following 3 partitions are automatically generated:

       ```
       P201701: [MIN_VALUE, 2017-02-01)
       P201702: [2017-02-01, 2017-03-01)
       P201703: [2017-03-01, 2017-04-01)
       ```

     * If we add Partition p201705 VALUES LESS THAN ("2017-06-01"), the results will be as follows:

       ```
       P201701: [MIN_VALUE, 2017-02-01)
       P201702: [2017-02-01, 2017-03-01)
       P201703: [2017-03-01, 2017-04-01)
       P201705: [2017-04-01, 2017-06-01)
       ```

     * Then we delete Partition p201703, the results will be as follows:

       ```
       p201701: [MIN_VALUE, 2017-02-01)
       p201702: [2017-02-01, 2017-03-01)
       p201705: [2017-04-01, 2017-06-01)
       ```

       > Note that the partition range of p201702 and p201705 has not changed, and there is a gap between the two partitions: [2017-03-01, 2017-04-01). That means, if the imported data is within this gap range, the import would fail.

     * Now we go on and delete Partition p201702, the results will be as follows:

       ```
       p201701: [MIN_VALUE, 2017-02-01)
       p201705: [2017-04-01, 2017-06-01)
       ```

       > The gap range expands to: [2017-02-01, 2017-04-01)

     * Then we add Partition p201702new VALUES LESS THAN ("2017-03-01"), the results will be as follows:

       ```
       p201701: [MIN_VALUE, 2017-02-01)
       p201702new: [2017-02-01, 2017-03-01)
       p201705: [2017-04-01, 2017-06-01)
       ```

       > The gap range shrinks to: [2017-03-01, 2017-04-01)

     * Now we delete Partition p201701 and add Partition p201612 VALUES LESS THAN ("2017-01-01"), the partition result is as follows:

       ```
       p201612: [MIN_VALUE, 2017-01-01)
       p201702new: [2017-02-01, 2017-03-01)
       p201705: [2017-04-01, 2017-06-01)
       ```

       > This results in a new gap range: [2017-01-01, 2017-02-01)

   In summary, the deletion of a partition does not change the range of the existing partitions, but might result in gaps. When a partition is added via the `VALUES LESS THAN` statement, the lower bound of one partition is the upper bound of its previous partition.

   In addition to the single-column partitioning mentioned above, Range Partitioning also supports **multi-column partitioning**. Examples are as follows:

   ```text
    PARTITION BY RANGE(`date`, `id`)
    (
        PARTITION `p201701_1000` VALUES LESS THAN ("2017-02-01", "1000"),
        PARTITION `p201702_2000` VALUES LESS THAN ("2017-03-01", "2000"),
        PARTITION `p201703_all` VALUES LESS THAN ("2017-04-01")
    )
   ```

    In the above example, we specify `date` (DATE type) and `id` (INT type) as the partitioning columns, so the resulting partitions will be as follows:

    ``` text
        *p201701_1000: [(MIN_VALUE, MIN_VALUE), ("2017-02-01", "1000") )
        *p201702_2000: [("2017-02-01", "1000"), ("2017-03-01", "2000") )
        *p201703_all: [("2017-03-01", "2000"), ("2017-04-01", MIN_VALUE))
    ```

   Note that in the last partition, the user only specifies the partition value of the `date` column, so the system fills in `MIN_VALUE` as the partition value of the `id` column by default. When data are imported, the system will compare them with the partition values in order, and put the data in their corresponding partitions. Examples are as follows:

    ``` text
       * Data --> Partition
       * 2017-01-01, 200   --> p201701_1000
       * 2017-01-01, 2000  --> p201701_1000
       * 2017-02-01, 100   --> p201701_1000
       * 2017-02-01, 2000  --> p201702_2000
       * 2017-02-15, 5000  --> p201702_2000
       * 2017-03-01, 2000  --> p201703_all
       * 2017-03-10, 1     --> p201703_all
       * 2017-04-01, 1000  --> Unable to import
       * 2017-05-01, 1000  --> Unable to import
    ```


<version since="1.2.0">
    

Range partitioning also supports batch partitioning. For example, you can create multiple partitions that are divided by day at a time using the `FROM ("2022-01-03") TO ("2022-01-06") INTERVAL 1 DAY`：2022-01-03 to 2022-01-06 (not including 2022-01-06), the results will be as follows:

    p20220103:    [2022-01-03,  2022-01-04)
    p20220104:    [2022-01-04,  2022-01-05)
    p20220105:    [2022-01-05,  2022-01-06)

</version>
    

#### List Partitioning

* The partitioning columns support the `BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME, CHAR, VARCHAR` data types, and the partition values are enumeration values. Partitions can be only hit if the data is one of the enumeration values in the target partition.

* List partitioning supports using `VALUES IN (...) ` to specify the enumeration values contained in each partition.

* The following example illustrates how partitions change when adding or deleting a partition.

  * As in the `example_list_tbl` example above, when the table is created, the following three partitions are automatically created.

    ```text
    p_cn: ("Beijing", "Shanghai", "Hong Kong")
    p_usa: ("New York", "San Francisco")
    p_jp: ("Tokyo")
    ```


  * If we add Partition p_uk VALUES IN ("London"), the results will be as follows:

    ```text
    p_cn: ("Beijing", "Shanghai", "Hong Kong")
    p_usa: ("New York", "San Francisco")
    p_jp: ("Tokyo")
    p_uk: ("London")
    ```

  * Now we delete Partition p_jp, the results will be as follows:

    ```text
    p_cn: ("Beijing", "Shanghai", "Hong Kong")
    p_usa: ("New York", "San Francisco")
    p_uk: ("London")
    ```

  List partitioning also supports **multi-column partitioning**. Examples are as follows:

  ```text
  PARTITION BY LIST(`id`, `city`)
  (
      PARTITION `p1_city` VALUES IN (("1", "Beijing"), ("1", "Shanghai")),
      PARTITION `p2_city` VALUES IN (("2", "Beijing"), ("2", "Shanghai")),
      PARTITION `p3_city` VALUES IN (("3", "Beijing"), ("3", "Shanghai"))
  )
  ```

  In the above example, we specify `id` (INT type) and `city` (VARCHAR type) as the partitioning columns, so the resulting partitions will be as follows:

  ```text
    * p1_city: [("1", "Beijing"), ("1", "Shanghai")]
    * p2_city: [("2", "Beijing"), ("2", "Shanghai")]
    * p3_city: [("3", "Beijing"), ("3", "Shanghai")]
  ```

  When data are imported, the system will compare them with the partition values in order, and put the data in their corresponding partitions. Examples are as follows:
  ```text
  Data ---> Partition
  1, Beijing  ---> p1_city
  1, Shanghai ---> p1_city
  2, Shanghai ---> p2_city
  3, Beijing  ---> p3_city
  1, Tianjin  ---> Unable to import
  4, Beijing  ---> Unable to import
  ```

2. Bucketing

   * If you use the Partition method, the `DISTRIBUTED ...` statement will describe how data are divided among partitions. If you do not use the Partition method, that statement will describe how data of the whole table are divided.
   * You can specify multiple columns as the bucketing columns. In Aggregate and Unique Models, bucketing columns must be Key columns; in the Duplicate Model, bucketing columns can be Key columns and Value columns. Bucketing columns can either be partitioning columns or not.
   * The choice of bucketing columns is a trade-off between **query throughput** and **query concurrency**:

     1. If you choose to specify multiple bucketing columns, the data will be more evenly distributed. However, if the query condition does not include the equivalent conditions for all bucketing columns, the system will scan all buckets, largely increasing the query throughput and decreasing the latency of a single query. This method is suitable for high-throughput, low-concurrency query scenarios.
     2. If you choose to specify only one or a few bucketing columns, point queries might scan only one bucket. Thus, when multiple point queries are preformed concurrently, they might scan various buckets, with no interaction between the IO operations (especially when the buckets are stored on various disks). This approach is suitable for high-concurrency point query scenarios.

   * AutoBucket: Calculates the number of partition buckets based on the amount of data. For partitioned tables, you can determine a bucket based on the amount of data, the number of machines, and the number of disks in the historical partition.
   * There is no theoretical limit on the number of buckets.

3. Recommendations on the number and data volume for Partitions and Buckets.

   * The total number of tablets in a table is equal to (Partition num * Bucket num).
   * The recommended number of tablets in a table, regardless of capacity expansion, is slightly more than the number of disks in the entire cluster.
   * The data volume of a single tablet does not have an upper or lower limit theoretically, but is recommended to be in the range of 1G - 10G. Overly small data volume of a single tablet can impose a stress on data aggregation and metadata management; while overly large data volume can cause trouble in data migration and completion, and increase the cost of Schema Change or Rollup operation failures (These operations are performed on the Tablet level).
   * For the tablets, if you cannot have the ideal data volume and the ideal quantity at the same time, it is recommended to prioritize the ideal data volume.
   * Upon table creation, you specify the same number of Buckets for each Partition. However, when dynamically increasing partitions (`ADD PARTITION`), you can specify the number of Buckets for the new partitions separately. This feature can help you cope with data reduction or expansion. 
   * Once you have specified the number of Buckets for a Partition, you may not change it afterwards. Therefore, when determining the number of Buckets, you need to consider the need of cluster expansion in advance. For example, if there are only 3 hosts, and each host has only 1 disk, and you have set the number of Buckets is only set to 3 or less, then no amount of newly added machines can increase concurrency.
   * For example, suppose that there are 10 BEs and each BE has one disk, if the total size of a table is 500MB, you can consider dividing it into 4-8 tablets; 5GB: 8-16 tablets; 50GB: 32 tablets; 500GB: you may consider dividing it into partitions, with each partition about 50GB in size, and 16-32 tablets per partition; 5TB: divided into partitions of around 50GB and 16-32 tablets per partition.

   > Note: You can check the data volume of the table using the [show data](../sql-manual/sql-reference/Show-Statements/SHOW-DATA.md) command. Divide the returned result by the number of copies, and you will know the data volume of the table.

4. About the settings and usage scenarios of Random Distribution:

   * If the OLAP table does not have columns of REPLACE type, set the data bucketing mode of the table to RANDOM. This can avoid severe data skew. (When loading data into the partition corresponding to the table, each batch of data in a single load task will be written into a randomly selected tablet).
   * When the bucketing mode of the table is set to RANDOM, since there are no specified bucketing columns, it is impossible to query only a few buckets, so all buckets in the hit partition will be scanned when querying the table. Thus, this setting is only suitable for aggregate query analysis of the table data as a whole, but not for highly concurrent point queries.
   * If the data distribution of the OLAP table is Random Distribution, you can set `load to single tablet`  to true when importing data. In this way, when importing large amounts of data, in one task, data will be only written in one tablet of the corresponding partition. This can improve both the concurrency and throughput of data import and reduce write amplification caused by data import and compaction, and thus, ensure cluster stability.

#### Compound Partitioning vs Single Partitioning

Compound Partitioning

- The first layer of data partitioning is called Partition. Users can specify a dimension column as the partitioning column (currently only supports columns of INT and TIME types), and specify the value range of each partition.
- The second layer is called Distribution, which means bucketing. Users can perform HASH distribution on data by specifying the number of buckets and one or more dimension columns as the bucketing columns, or perform random distribution on data by setting the mode to Random Distribution.

Compound partitioning is recommended for the following scenarios:

- Scenarios with time dimensions or similar dimensions with ordered values, which can be used as partitioning columns. The partitioning granularity can be evaluated based on data import frequency, data volume, etc.
- Scenarios with a need to delete historical data: If, for example, you only need to keep the data of the last N days), you can use compound partitioning so you can delete historical partitions. To remove historical data, you can also send a DELETE statement within the specified partition.
- Scenarios with a need to avoid data skew: you can specify the number of buckets individually for each partition. For example, if you choose to partition the data by day, and the data volume per day varies greatly, you can customize the number of buckets for each partition. For the choice of bucketing column, it is recommended to select the column(s) with variety in values.

Users can also choose for single partitioning, which is about HASH distribution.

### PROPERTIES

In the `PROPERTIES` section at the last of the CREATE TABLE statement, you can set the relevant parameters. Please see [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) for a detailed introduction.

### ENGINE

In this example, the ENGINE is of OLAP type, which is the default ENGINE type. In Doris, only the OALP ENGINE type is managed and stored by Doris. Other ENGINE types, such as MySQL, Broker, ES, are essentially mappings to tables in other external databases or systems to ensure that Doris can read the data. And Doris itself does not create, manage, or store any tables and data of non-OLAP ENGINE type.

### Other

`IF NOT EXISTS` means to create the table if it is non-existent. Note that the system only checks the existence of table based on the table name, but not compare the schema of the newly created table with the existing ones. So if there exists a table of the same name but different schema, the command will also return, but it does not mean that a new table of a new schema has been created.

## FAQ

### Table Creation

1. If a syntax error occurs in a long CREATE TABLE statement, the error message may be incomplete. Here is a list of possible syntax errors for your reference in manual touble shooting:

   * Incorrect syntax. Please use `HELP CREATE TABLE;`to check the relevant syntax.
   * Reserved words. Reserved words in user-defined names should be enclosed in backquotes ``. It is recommended that all user-defined names be enclosed in backquotes.
   * Chinese characters or full-width characters. Non-UTF8 encoded Chinese characters, or hidden full-width characters (spaces, punctuation, etc.) can cause syntax errors. It is recommended that you check for these characters using a text editor that can display non-printable characters.

2. `Failed to create partition [xxx] . Timeout`

   In Doris, tables are created in the order of the partitioning granularity. This error prompt may appear when a partition creation task fails, but it could also appear in table creation tasks with no partitioning operations, because, as mentioned earlier, Doris will create an unmodifiable default partition for tables with no partitions specified.

   This error usually pops up because the tablet creation goes wrong in BE. You can follow the steps below for troubleshooting:

   1. In fe.log, find the `Failed to create partition` log of the corresponding time point. In that log, find a number pair that looks like `{10001-10010}` . The first number of the pair is the Backend ID and the second number is the Tablet ID. As for `{10001-10010}`, it means that on Backend ID 10001, the creation of Tablet ID 10010 failed.
   2. After finding the target Backend, go to the corresponding be.INFO log and find the log of the target tablet, and then check the error message.
   3. A few common tablet creation failures include but not limited to:
      * The task is not received by BE. In this case, the tablet ID related information will be found in be.INFO, or the creation is successful in BE but it still reports a failure. To solve the above problems, see [Installation and Deployment](https://doris.apache.org/docs/dev/install/standard-deployment/) about how to check the connectivity of FE and BE.
      * Pre-allocated memory failure. It may be that the length of a row in the table exceeds 100KB.
      * `Too many open files`. The number of open file descriptors exceeds the Linux system limit. In this case, you need to change the open file descriptor limit of the Linux system.

   If it is a timeout error, you can set `tablet_create_timeout_second=xxx` and `max_create_table_timeout_second=xxx` in fe.conf. The default value of `tablet_create_timeout_second=xxx` is 1 second, and that of `max_create_table_timeout_second=xxx`  is 60 seconds. The overall timeout would be min(tablet_create_timeout_second * replication_num, max_create_table_timeout_second). For detailed parameter settings, please check [FE Configuration](https://doris.apache.org/docs/dev/admin-manual/config/fe-config/).

3. The build table command does not return results for a long time.

   Doris's table creation command is a synchronous command. The timeout of this command is currently set to be relatively simple, ie (tablet num * replication num) seconds. If you create more data fragments and have fragment creation failed, it may cause an error to be returned after waiting for a long timeout.

   Under normal circumstances, the statement will return in a few seconds or ten seconds. If it is more than one minute, it is recommended to cancel this operation directly and go to the FE or BE log to view the related errors.

## More Help

For more detailed instructions on data partitioning, please refer to the [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.md) command manual, or enter `HELP CREATE TABLE;` in MySQL Client.
