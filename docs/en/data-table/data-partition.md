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

This document mainly introduces Doris's table construction and data partitioning, as well as problems and solutions that may be encountered in the construction of the table.

## Basic Concepts

In Doris, data is logically described in the form of a table.

### Row & Column

A table includes rows (rows) and columns (columns). Row is a row of data for the user. Column is used to describe different fields in a row of data.

Column can be divided into two broad categories: Key and Value. From a business perspective, Key and Value can correspond to dimension columns and metric columns, respectively. From the perspective of the aggregation model, the same row of Key columns will be aggregated into one row. The way the Value column is aggregated is specified by the user when the table is built. For an introduction to more aggregation models, see the [Doris Data Model](./data-model.html).

### Tablet & Partition

In Doris's storage engine, user data is horizontally divided into several data slices (also known as data buckets). Each tablet contains several rows of data. The data between the individual tablets does not intersect and is physically stored independently.

Multiple tablets are logically attributed to different partitions. A tablet belongs to only one Partition. And a Partition contains several Tablets. Because the tablet is physically stored independently, it can be considered that the Partition is physically independent. Tablet is the smallest physical storage unit for data movement, replication, and so on.

Several Partitions form a Table. Partition can be thought of as the smallest logical unit of management. Importing and deleting data can be done for one Partition or only for one Partition.

## Data division

We use a table-building operation to illustrate Doris' data partitioning.

Doris's table creation is a synchronous command. The result is returned after the SQL execution is completed. If the command returns successfully, it means that the table creation is successful. For specific table creation syntax, please refer to [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html), or you can view more details through `HELP CREATE TABLE;` Much help.See more help with `HELP CREATE TABLE;`.

This section introduces Doris's approach to building tables with an example.

```sql
-- Range Partition

CREATE TABLE IF NOT EXISTS example_db.expamle_range_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "User id",
    `date` DATE NOT NULL COMMENT "Data fill in date time",
    `timestamp` DATETIME NOT NULL COMMENT "Timestamp of data being poured",
    `city` VARCHAR(20) COMMENT "The city where the user is located",
    `age` SMALLINT COMMENT "User age",
    `sex` TINYINT COMMENT "User gender",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "User last visit time",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "Total user consumption",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "User maximum dwell time",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "User minimum dwell time"
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

CREATE TABLE IF NOT EXISTS example_db.expamle_list_tbl
(
    `user_id` LARGEINT NOT NULL COMMENT "User id",
    `date` DATE NOT NULL COMMENT "Data fill in date time",
    `timestamp` DATETIME NOT NULL COMMENT "Timestamp of data being poured",
    `city` VARCHAR(20) COMMENT "The city where the user is located",
    `age` SMALLINT COMMENT "User Age",
    `sex` TINYINT COMMENT "User gender",
    `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "User last visit time",
    `cost` BIGINT SUM DEFAULT "0" COMMENT "Total user consumption",
    `max_dwell_time` INT MAX DEFAULT "0" COMMENT "User maximum dwell time",
    `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "User minimum dwell time"
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

### Column Definition

Here we only use the AGGREGATE KEY data model as an example. See the [Doris Data Model](./data-model.html) for more data models.

The basic type of column can be viewed by executing `HELP CREATE TABLE;` in mysql-client.

In the AGGREGATE KEY data model, all columns that do not specify an aggregation mode (SUM, REPLACE, MAX, MIN) are treated as Key columns. The rest is the Value column.

When defining columns, you can refer to the following suggestions:

1. The Key column must precede all Value columns.
2. Try to choose the type of integer. Because integer type calculations and lookups are much more efficient than strings.
3. For the selection principle of integer types of different lengths, follow **enough to**.
4. For lengths of type VARCHAR and STRING, follow **enough to**.
5. The total byte length of all columns (including Key and Value) cannot exceed 100KB.

### Partitioning and Bucket

Doris supports two levels of data partitioning. The first layer is Partition, which supports Range and List partitioning. The second layer is the Bucket (Tablet), which only supports Hash partitioning.

It is also possible to use only one layer of partitioning. When using a layer partition, only Bucket partitioning is supported.

1. Partition

    * The Partition column can specify one or more columns. The partition class must be a KEY column. The use of multi-column partitions is described later in the **Multi-column partitioning** summary. 
    * Regardless of the type of partition column, double quotes are required when writing partition values.
    * There is no theoretical limit on the number of partitions.
    * When you do not use Partition to build a table, the system will automatically generate a Partition with the same name as the table name. This Partition is not visible to the user and cannot be modified.
    * **Do not add partitions with overlapping ranges** when creating partitions.

    #### Range Partition

    * Partition columns are usually time columns for easy management of old and new data.

    * Partition supports only the upper bound by `VALUES LESS THAN (...)`, the system will use the upper bound of the previous partition as the lower bound of the partition, and generate a left closed right open interval. Passing, also supports specifying the upper and lower bounds by `VALUES [...)`, and generating a left closed right open interval.

    * It is easier to understand by specifying `VALUES [...)`. Here is an example of the change in partition range when adding or deleting partitions using the `VALUES LESS THAN (...)` statement:
        * As in the `example_range_tbl` example above, when the table is built, the following 3 partitions are automatically generated:
            ```
            P201701: [MIN_VALUE, 2017-02-01)
            P201702: [2017-02-01, 2017-03-01)
            P201703: [2017-03-01, 2017-04-01)
            ```
            
        * When we add a partition p201705 VALUES LESS THAN ("2017-06-01"), the partition results are as follows:
        
            ```
            P201701: [MIN_VALUE, 2017-02-01)
            P201702: [2017-02-01, 2017-03-01)
            P201703: [2017-03-01, 2017-04-01)
            P201705: [2017-04-01, 2017-06-01)
            ```
            
        * At this point we delete the partition p201703, the partition results are as follows:
        
            ```
            p201701: [MIN_VALUE, 2017-02-01)
            p201702: [2017-02-01, 2017-03-01)
            p201705: [2017-04-01, 2017-06-01)
            ```
            
            > Note that the partition range of p201702 and p201705 has not changed, and there is a hole between the two partitions: [2017-03-01, 2017-04-01). That is, if the imported data range is within this hole, it cannot be imported.
            
        * Continue to delete partition p201702, the partition results are as follows:
        
            ```
            p201701: [MIN_VALUE, 2017-02-01)
            p201705: [2017-04-01, 2017-06-01)
            ```
            
            > The void range becomes: [2017-02-01, 2017-04-01)
            
        * Now add a partition p201702new VALUES LESS THAN ("2017-03-01"), the partition results are as follows:
          
            ```
            p201701: [MIN_VALUE, 2017-02-01)
            p201702new: [2017-02-01, 2017-03-01)
            p201705: [2017-04-01, 2017-06-01)
            ```
            
            > You can see that the hole size is reduced to: [2017-03-01, 2017-04-01)
            
        * Now delete partition p201701 and add partition p201612 VALUES LESS THAN ("2017-01-01"), the partition result is as follows:
        
            ```
            p201612: [MIN_VALUE, 2017-01-01)
            p201702new: [2017-02-01, 2017-03-01)
            p201705: [2017-04-01, 2017-06-01)
            ```
            
            > A new void appeared: [2017-01-01, 2017-02-01)
        

    In summary, the deletion of a partition does not change the scope of an existing partition. There may be holes in deleting partitions. When a partition is added by the `VALUES LESS THAN` statement, the lower bound of the partition immediately follows the upper bound of the previous partition.

    In addition to the single-column partitioning we have seen above, Range partition also supports **multi-column partitioning**, examples are as follows:

   ```text
    PARTITION BY RANGE(`date`, `id`)
    (
        PARTITION `p201701_1000` VALUES LESS THAN ("2017-02-01", "1000"),
        PARTITION `p201702_2000` VALUES LESS THAN ("2017-03-01", "2000"),
        PARTITION `p201703_all` VALUES LESS THAN ("2017-04-01")
    )
    ```
    
    In the above example, we specify `date` (DATE type) and `id` (INT type) as partition columns. The resulting partitions in the above example are as follows:
    
    ```
    *p201701_1000: [(MIN_VALUE, MIN_VALUE), ("2017-02-01", "1000") )
    *p201702_2000: [("2017-02-01", "1000"), ("2017-03-01", "2000") )
    *p201703_all: [("2017-03-01", "2000"), ("2017-04-01", MIN_VALUE))
    ```
    
    Note that the last partition user defaults only the partition value of the `date` column, so the partition value of the `id` column will be filled with `MIN_VALUE` by default. When the user inserts data, the partition column values ​​are compared in order, and the corresponding partition is finally obtained. Examples are as follows:
    
    ```
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

    #### List Partition

    * The partition column supports the `BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME, CHAR, VARCHAR` data type, and the partition value is an enumeration value. Partitions can be hit only if the data is one of the target partition enumeration values.
    * Partition supports specifying the number of partitions contained in each partition via `VALUES IN (...) ` to specify the enumeration values contained in each partition.
    * The following example illustrates how partitions change when adding or deleting partitions.
      
        * As in the `example_list_tbl` example above, when the table is built, the following three partitions are automatically created.

            ```
            p_cn: ("Beijing", "Shanghai", "Hong Kong")
            p_usa: ("New York", "San Francisco")
            p_jp: ("Tokyo")
            ```

        * When we add a partition p_uk VALUES IN ("London"), the result of the partition is as follows
        
            ```
            p_cn: ("Beijing", "Shanghai", "Hong Kong")
            p_usa: ("New York", "San Francisco")
            p_jp: ("Tokyo")
            p_uk: ("London")
            ```
        
        * When we delete the partition p_jp, the result of the partition is as follows.

            ```
            p_cn: ("Beijing", "Shanghai", "Hong Kong")
            p_usa: ("New York", "San Francisco")
            p_uk: ("London")
            ```

    List partition also supports **multi-column partition**, examples are as follows:

    ```text
    PARTITION BY LIST(`id`, `city`)
    (
        PARTITION `p1_city` VALUES IN (("1", "Beijing"), ("1", "Shanghai")),
        PARTITION `p2_city` VALUES IN (("2", "Beijing"), ("2", "Shanghai")),
        PARTITION `p3_city` VALUES IN (("3", "Beijing"), ("3", "Shanghai"))
    )
    ```
    
    In the above example, we specify `id`(INT type) and `city`(VARCHAR type) as partition columns. The above example ends up with the following partitions.
    
    ```
    * p1_city: [("1", "Beijing"), ("1", "Shanghai")]
    * p2_city: [("2", "Beijing"), ("2", "Shanghai")]
    * p3_city: [("3", "Beijing"), ("3", "Shanghai")]
    ```
    
    When the user inserts data, the partition column values will be compared sequentially in order to finally get the corresponding partition. An example is as follows.
    
    ```
    * Data ---> Partition
    * 1, Beijing  ---> p1_city
    * 1, Shanghai ---> p1_city
    * 2, Shanghai ---> p2_city
    * 3, Beijing  ---> p3_city
    * 1, Tianjin  ---> Unable to import
    * 4, Beijing  ---> Unable to import
    ```

2. Bucket

    * If a Partition is used, the `DISTRIBUTED ...` statement describes the division rules for the data in each partition. If you do not use Partition, it describes the rules for dividing the data of the entire table.
    * The bucket column can be multiple columns, but it must be a Key column. The bucket column can be the same or different from the Partition column.
    * The choice of bucket column is a trade-off between **query throughput** and **query concurrency**:

        1. If you select multiple bucket columns, the data is more evenly distributed. However, if the query condition does not include the equivalent condition for all bucket columns, a query will scan all buckets. The throughput of such queries will increase, and the latency of a single query will decrease. This method is suitable for large throughput and low concurrent query scenarios.
        2. If you select only one or a few bucket columns, the point query can query only one bucket. This approach is suitable for high-concurrency point query scenarios.
        
    * There is no theoretical limit on the number of buckets.

3. Recommendations on the number and amount of data for Partitions and Buckets.

    * The total number of tablets in a table is equal to (Partition num * Bucket num).
    * The number of tablets in a table, which is slightly more than the number of disks in the entire cluster, regardless of capacity expansion.
    * The data volume of a single tablet does not theoretically have an upper and lower bound, but is recommended to be in the range of 1G - 10G. If the amount of data for a single tablet is too small, the aggregation of the data is not good and the metadata management pressure is high. If the amount of data is too large, it is not conducive to the migration, completion, and increase the cost of Schema Change or Rollup operation failure retry (the granularity of these operations failure retry is Tablet).
    * When the tablet's data volume principle and quantity principle conflict, it is recommended to prioritize the data volume principle.
    * When building a table, the number of Buckets for each partition is uniformly specified. However, when dynamically increasing partitions (`ADD PARTITION`), you can specify the number of Buckets for the new partition separately. This feature can be used to easily reduce or expand data.
    * Once the number of Buckets for a Partition is specified, it cannot be changed. Therefore, when determining the number of Buckets, you need to consider the expansion of the cluster in advance. For example, there are currently only 3 hosts, and each host has 1 disk. If the number of Buckets is only set to 3 or less, then even if you add more machines later, you can't increase the concurrency.
    * Give some examples: Suppose there are 10 BEs, one for each BE disk. If the total size of a table is 500MB, you can consider 4-8 shards. 5GB: 8-16. 50GB: 32. 500GB: Recommended partitions, each partition is about 50GB in size, with 16-32 shards per partition. 5TB: Recommended partitions, each with a size of around 50GB and 16-32 shards per partition.
    
    > Note: The amount of data in the table can be viewed by the [show data](../sql-manual/sql-reference/Show-Statements/SHOW-DATA.html) command. The result is divided by the number of copies, which is the amount of data in the table.
    

#### Compound Partitions vs Single Partitions

Compound Partitions

- The first level is called Partition, which is partition. Users can specify a dimension column as a partition column (currently only columns of integer and time types are supported), and specify the value range of each partition.
- The second level is called Distribution, which means bucketing. Users can specify one or more dimension columns and the number of buckets to perform HASH distribution on the data.

Composite partitions are recommended for the following scenarios

- There is a time dimension or similar dimension with ordered values, which can be used as a partition column. Partition granularity can be evaluated based on import frequency, partition data volume, etc.
- Deletion of historical data: If there is a need to delete historical data (for example, only keep the data of the last N days). With composite partitions, this can be achieved by removing historical partitions. Data deletion is also possible by sending a DELETE statement within the specified partition.
- Solve the problem of data skew: each partition can specify the number of buckets individually. For example, partitioning by day, when the amount of data per day varies greatly, you can reasonably divide the data in different partitions by specifying the number of buckets for the partition. It is recommended to select a column with a large degree of discrimination for the bucketing column.

The user can also use a single partition without using composite partitions. Then the data is only distributed in HASH.

### PROPERTIES

In the last PROPERTIES of the table building statement, for the relevant parameters that can be set in PROPERTIES, we can check [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) for a detailed introduction.

### ENGINE

In this example, the type of ENGINE is olap, the default ENGINE type. In Doris, only this ENGINE type is managed and stored by Doris. Other ENGINE types, such as mysql, broker, es, etc., are essentially mappings to tables in other external databases or systems to ensure that Doris can read the data. And Doris itself does not create, manage, and store any tables and data of a non-olap ENGINE type.

### Other

`IF NOT EXISTS` indicates that if the table has not been created, it is created. Note that only the table name is judged here, and it is not determined whether the new table structure is the same as the existing table structure. So if there is a table with the same name but different structure, the command will also return success, but it does not mean that a new table and a new structure have been created.

## common problem

### Build Table Operations FAQ

1. If a syntax error occurs in a long build statement, a syntax error may be incomplete. Here is a list of possible syntax errors for manual error correction:

    * The syntax is incorrect. Please read `HELP CREATE TABLE;` carefully to check the relevant syntax structure.
    * Reserved words. When the user-defined name encounters a reserved word, it needs to be enclosed in the backquote ``. It is recommended that all custom names be generated using this symbol.
    * Chinese characters or full-width characters. Non-utf8 encoded Chinese characters, or hidden full-width characters (spaces, punctuation, etc.) can cause syntax errors. It is recommended to check with a text editor with invisible characters.

2. `Failed to create partition [xxx] . Timeout`

    Doris builds are created in order of Partition granularity. This error may be reported when a Partition creation fails. Even if you don't use Partition, you will report `Failed to create partition` when there is a problem with the built table, because as mentioned earlier, Doris will create an unchangeable default Partition for tables that do not have a Partition specified.

    When this error is encountered, it is usually the BE that has encountered problems creating data fragments. You can follow the steps below to troubleshoot:

    1. In fe.log, find the `Failed to create partition` log for the corresponding point in time. In this log, a series of numbers like `{10001-10010}` will appear. The first number of the pair is the Backend ID and the second number is the Tablet ID. As for the pair of numbers above, on the Backend with ID 10001, creating a tablet with ID 10010 failed.
    2. Go to the be.INFO log corresponding to Backend and find the log related to the tablet id in the corresponding time period. You can find the error message.
    3. Listed below are some common tablet creation failure errors, including but not limited to:
        * BE did not receive the relevant task, and the tablet id related log could not be found in be.INFO. Or the BE is created successfully, but the report fails. For the above questions, see [Deployment and Upgrade Documentation] to check the connectivity of FE and BE.
        * Pre-allocated memory failed. It may be that the length of a line in a row in the table exceeds 100KB.
        * `Too many open files`. The number of open file handles exceeds the Linux system limit. The handle limit of the Linux system needs to be modified.

    You can also extend the timeout by setting `tablet_create_timeout_second=xxx` in fe.conf. The default is 2 seconds.

3. The build table command does not return results for a long time.

    Doris's table creation command is a synchronous command. The timeout of this command is currently set to be relatively simple, ie (tablet num * replication num) seconds. If you create more data fragments and have fragment creation failed, it may cause an error to be returned after waiting for a long timeout.

    Under normal circumstances, the statement will return in a few seconds or ten seconds. If it is more than one minute, it is recommended to cancel this operation directly and go to the FE or BE log to view the related errors.

## More help

For more detailed instructions on data partitioning, we can refer to the [CREATE TABLE](../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-TABLE.html) command manual, and also You can enter `HELP CREATE TABLE;` under the Mysql client to get more help information.
