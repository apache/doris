# Data partition

This document mainly introduces Doris's table building and data partitioning, as well as possible problems and solutions in table building operation.

## Basic concepts

In Doris, data is logically described in the form of tables.

### Row & Column

A table consists of rows and columns. Row is a row of user data. Column is used to describe different fields in a row of data.

Columns can be divided into two categories: Key and Value. From a business perspective, Key and Value can correspond to dimension columns and indicator columns, respectively. From the point of view of the aggregation model, the same row of the Key column is aggregated into a row. The aggregation of the Value column is specified by the user when the table is built. For an introduction to more aggregation models, see [Doris data model] (. / data-model-rollup. md).

'35;\ 35;\ 35; Tablet & Partition

In Doris's storage engine, user data is divided horizontally into several data fragments (Tablets, also known as data buckets). Each table contains several rows of data. Data between Tablets does not intersect and is physically stored independently.

Multiple Tablets logically belong to different Partitions. A Tablet belongs to only one Partition. A Partition contains several Tablets. Because Tablet is physically stored independently, it can be considered that Partition is physically independent. Tablet is the smallest physical storage unit for data movement, replication and other operations.

Several Partitions form a Table. Partition can be regarded as the smallest management unit logically. Data import and deletion can be done for a single partition.

## Data partition

We illustrate Doris's data partition with a table-building operation.

Doris's table building is a synchronization command. The command returns success, which means that the table building is successful.

可以通过 `HELP CREATE TABLE;` 查看更多帮助。

This section presents an example of how Doris is constructed.

```
CREATE TABLE IF NOT EXISTS example_db.expamle_tbl
(
`user_id` LARGEINT NOT NULL COMMENT "用户id",
"Date `date not null how `index `Fufu 8;'Back
`timestamp` DATETIME NOT NULL COMMENT "数据灌入的时间戳",
` City `VARCHAR (20) COMMENT `User City',
"Age" SMALLINT COMMENT "29992;" 25143;"24180;" 40836 ",
`sex` TINYINT COMMENT "用户性别",
"last visit date" DATETIME REPLACE DEFAULT "1970 -01 -01 00:00" COMMENT "25143;" 27425;"35775;" 3838382",
`cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
Best Answer: Best Answer
How about "99999" as time goes by???????????????????????????????????????????????????????????????????????????????????????????
)
ENGINE=olap
AGGREGATE KEY(`user_id`, `date`, `timestamp`, `city`, `age`, `sex`)
PARTITION BY RANGE(`date`)
(
Segmentation `P201701 `value'equals `2017-02-01',
Segmentation `P201702 `Value equals'(2017-03-01'),
Segmentation `P201703'`Value less than' (2017-04-01')
)
DISTRIBUTED BY HASH(`user_id`) BUCKETS 16
PROPERTIES
(
"Replication\ num" = "3",
"storage_medium" = "SSD",
"Storage = U Cooldown = U Time" = "2018-01-12:00:
);

```

### Column Definition

Here we only take the AGGREGATE KEY data model as an example to illustrate. Refer to [Doris data model] (. / data-model-rollup. md) for more data models.

The basic types of columns can be viewed by executing `HELP CREATE TABLE'in mysql-client.

In the AGGREGATE KEY data model, all columns without specified aggregation methods (SUM, REPLACE, MAX, MIN) are treated as Key columns. The rest are Value columns.

When defining columns, you can refer to the following suggestions:

1. The Key column must precede all Value columns.
2. Choose integer type as far as possible. Because integer types are much more efficient than strings in computation and lookup.
3. For the selection principle of integer types of different lengths, follow the ** sufficient can **.
4. For the length of VARCHAR and STING types, follow ** suffice **.
5. The total byte length of all columns (including Key and Value) cannot exceed 100KB.

### Zoning and Bucketing

Doris supports two-tier data partitioning. The first layer is Partition, which only supports Range partitioning. The second layer is Bucket (Tablet), which only supports Hash partitioning.

You can also use only one layer of partition. Bucket partitioning is only supported when using one-tier partitioning.

1. Partition

* Partition columns can specify one or more columns. Partition classes must be KEY columns. The usage of multi-column partitions is introduced in the following ** summary of multi-column partitions.
* Partition's boundaries are left-closed and right-open. For example, if you want to store all February data in p201702, you need to enter the partition value "2017-03-01", that is, the range: [2017-02-01, 2017-03-01].
* Regardless of the partition column type, double quotation marks are required when writing partition values.
* Partition columns are usually time columns to facilitate the management of old and new data.
* There is no theoretical upper limit on the number of zones.
* When Partition is not used to build tables, the system automatically generates a full-range Partition with the same name as the table name. The Partition is invisible to users and cannot be deleted.

An example is given to illustrate the change of partition scope when adding or deleting partitions.
* As shown in the example above, when the table is completed, the following three partitions are automatically generated:

```
p201701: [MIN VALUE, 2017 -02 -01]
p201702: [2017-02-01, 2017-03-01)
p201703: [2017-03-01, 2017-04-01)
```

* When we add a partition p201705 VALUES LESS THAN ("2017-06-01"), the partition results are as follows:

```
p201701: [MIN VALUE, 2017 -02 -01]
p201702: [2017-02-01, 2017-03-01)
p201703: [2017-03-01, 2017-04-01)
p201705: [2017-04-01, 2017-06-01)
```

* When we delete partition p201703, the partition results are as follows:

```
*p201701: [MIN VALUE, 2017 -02 -01]
* p201702: [2017-02-01, 2017-03-01)
* p201705: [2017-04-01, 2017-06-01)
```

> Notice that the partition ranges of p201702 and p201705 have not changed, and there is a gap between the two partitions: [2017-03-01, 2017-04-01]. That is, if the imported data range is within this empty range, it is imported as before.

* Continue to delete partition p201702, partition results are as follows:

```
*p201701: [MIN VALUE, 2017 -02 -01]
* p201705: [2017-04-01, 2017-06-01)
* The void range becomes: [2017-02-01, 2017-04-01]
```

* Now add a partition p201702 new VALUES LESS THAN ("2017-03-01"). The partition results are as follows:

```
*p201701: [MIN VALUE, 2017 -02 -01]
*p201702new: [2017 -02 -01, 2017 -03 -01]
*p201705: [2017 -04 -01, 2017 -06 -01]
```

> It can be seen that the void range is reduced to: [2017-03-01, 2017-04-01]

* Now delete partition p201701 and add partition p201612 VALUES LESS THAN ("2017-01-01"). The partition results are as follows:

```
*p201612: [MIN VALUE, 2017 -01 -01]
*p201702new: [2017 -02 -01, 2017 -03 -01]
*p201705: [2017 -04 -01, 2017 -06 -01]
```

> A new void appears: [2017-01-01, 2017-02-01]

In summary, deletion of partitions does not change the scope of existing partitions. Deleting partitions may cause holes. When partitions are added, the lower bound of a partition is immediately followed by the upper bound of a partition.
Partitions with overlapping ranges cannot be added.

2. Bucket

* If Partition is used, the `DISTRIBUTED...'statement describes the partitioning rules of data within ** partitions. If Partition is not used, the partitioning rules for the data of the entire table are described.
* Bucket columns can be multiple columns, but must be Key columns. Bucket columns can be the same or different as ARTITION columns.
* The choice of bucket columns is a trade-off between ** query throughput ** and ** query concurrency **:

1. If multiple bucket columns are selected, the data will be more evenly distributed. But if the query condition does not contain the equivalent condition of all bucket columns, a query scans all buckets. This increases the throughput of queries, but increases the latency of individual queries. This approach is suitable for query scenarios with high throughput and low concurrency.
2. If only one or a few bucket columns are selected, point query can query only one bucket. This method is suitable for high concurrent point query scenarios.

* There is theoretically no upper limit on the number of buckets.

3. Suggestions on the quantity and data quantity of Partition and Bucket.

* The total number of tables in a table is equal to (Partition num * Bucket num).
* The number of tables in a table is recommended to be slightly more than the number of disks in the entire cluster, regardless of capacity expansion.
* There is no upper and lower bound theoretically for the data volume of a single Tablet, but it is recommended to be within the range of 1G - 10G. If the amount of single Tablet data is too small, the aggregation effect of data is not good, and the pressure of metadata management is high. If the amount of data is too large, it is not conducive to the migration and completion of replicas, and will increase the cost of failed retries of Schema Change or Rollup operations (the granularity of these failed retries is Tablet).
* When Tablet's principle of data quantity conflicts with that of quantity, it is suggested that priority be given to the principle of data quantity.
* When tabulating, the number of Buckets per partition is specified uniformly. However, when adding partitions dynamically (`ADD PARTITION'), you can specify the number of Buckets for new partitions separately. This function can be used to deal with data shrinkage or expansion conveniently.
* Once specified, the number of Buckets for a Partition cannot be changed. Therefore, in determining the number of Buckets, it is necessary to consider the situation of cluster expansion in advance. For example, currently there are only three hosts, and each host has one disk. If the number of Buckets is set to 3 or less, concurrency cannot be improved even if machines are added later.
* For example, suppose there are 10 BEs, one disk per BE. If the total size of a table is 500 MB, 4-8 fragments can be considered. 5GB: 8-16. 50GB: 32. 500GB: Recommended partition, each partition size is about 50GB, each partition 16-32 partitions. 5TB: Recommended partitions, each partition size is about 50GB, each partition 16-32 partitions.

> Note: The amount of data in the table can be viewed by the `show data'command, and the result is divided by the number of copies, that is, the amount of data in the table.

#### Multi-column partition

Doris supports specifying multiple columns as partitioned columns, as shown below:

```
PARTITION BY RANGE(`date`, `id`)
(
Separating `P201701 `U1000 `values less than'(2017-02-01', `1000'),
Split `P201702 `U2000 `values less than'(2017-03-01', `2000'),
Segmentation `P201703 `U'all `values less than (`2017-04-01')
)
```

In the above example, we specify `date'(DATE type) and `id' (INT type) as partition columns. The final partition of the above example is as follows:

```
*p201701.1000: [(MIN VALUE, MIN VALUE), ("2017 -02 -01", "1000")
(2017 -02 -01, 1000), ("2017 -03 -01", "2000")
*p201703 all: [("2017 -03 -01", "2000"), ("2017 -04 -01", MIN VALUE))
```

Note that the last partition user specifies only the partition value of the `date'column by default, so the partition value of the `id' column is filled in by default `MIN_VALUE'. When the user inserts data, the partition column values are compared sequentially, and the corresponding partitions are finally obtained. Examples are as follows:

```
* Data - > Partition
*2017 -01, 200 --> p201701 -u 1000
* 2017-01-01, 2000    --> p201701_1000
*2017 -02 -01, 100 --> p201701 -u 1000
* 2017-02-01, 2000    --> p201702_2000
* 2017-02-15, 5000    --> p201702_2000
* 2017-03-01, 2000    --> p201703_all
* 2017-03-10, 1-> P201703  all
* 2017-04-01, 1000 - > Unable to import
* 2017-05-01, 1000 - > Unable to import
```

### PROPERTIES

In the final PROPERTIES of the table statement, you can specify the following two parameters:

One copy

* Number of copies per Tablet. The default is 3. It is recommended that the default be maintained. In the table statement, the number of Tablet replicas in all Partitions is specified uniformly. When adding a new partition, you can specify the number of Tablet copies in the new partition separately.
* The number of copies can be modified at run time. It is strongly recommended that odd numbers be maintained.
* The maximum number of copies depends on the number of independent IP in the cluster (note that it is not the number of BEs). The principle of duplicate distribution in Doris is that duplicates of the same Tablet are not allowed to be distributed on the same physical machine, while identifying the physical machine is through IP. Therefore, even if three or more BE instances are deployed on the same physical machine, if the IP of these BEs is the same, only 1 copy number can be set.
* For some small and infrequently updated dimension tables, you can consider setting more copies. In this way, when Join queries, there is a greater probability of local data Join.

2. storage_medium & storage\_cooldown\_time

* The data storage directory of BE can be explicitly specified as SSD or HDD (distinguished by. SSD or. HDD suffixes). When creating a table, you can specify all the media that Partition initially stores. Note that the suffix function is to explicitly specify the disk media without checking whether it matches the actual media type.
* The default initial storage medium is HDD. If SSD is specified, the data is initially stored on SSD.
* If storage cooldown time is not specified, data will be automatically migrated from SSD to HDD 7 days later by default. If storage cooldown time is specified, the data migrates only after the storage_cooldown_time time time is reached.
* Note that when storage_media is specified, this parameter is just a "best effort" setting. Even if SSD storage medium is not set up in the cluster, it will not report errors, but will be automatically stored in the available data directory. Similarly, if SSD media is inaccessible and space is insufficient, it may cause data to be stored directly on other available media initially. When data is migrated to HDD at maturity, if HDD media is inaccessible and space is insufficient, the migration may fail (but it will keep trying).

### ENGINE

In this example, the ENGINE type is olap, which is the default ENGINE type. In Doris, only this ENGINE type is responsible for data management and storage by Doris. Other ENGINE types, such as mysql, broker, es, etc., are essentially mappings to tables in other external databases or systems to ensure that Doris can read these data. Doris itself does not create, manage and store any tables and data of non-olap ENGINE type.

### Others

` IF NOT EXISTS ` indicates that if the table has not been created, it will be created. Note that only the existence of table names is judged here, not whether the new table structure is the same as the existing table structure. So if there is a table with the same name but different structure, the command returns success, but it does not mean that a new table and new structure have been created.

## Common Questions

### Common problems in table building operation

1. If a grammatical error occurs in a long table-building statement, the phenomenon of incomplete grammatical error hints may occur. Here is a list of possible grammatical errors for manual error correction:

* Error in grammatical structure. Please read `HELP CREATE TABLE'carefully; `Check the relevant grammatical structure.
* Keep words. When a user-defined name encounters a reserved word, it needs to be caused by a back quotation mark `. It is recommended that all custom names be generated using this symbol.
* Chinese characters or full-angle characters. Non-utf8 coded Chinese characters, or hidden full-angle characters (spaces, punctuation, etc.), can lead to grammatical errors. It is recommended to use a text editor with invisible characters to check.

2. `Failed to create partition [xxx] . Timeout`

Doris tables are created in order of partition granularity. This error may be reported when a Partition creation fails. Even if you don't use Partition, you will report `Failed to create Partition'when a table is built incorrectly, because Doris will create an immutable default Artition for a table that does not specify Partition, as described earlier.

When encountering this error, BE usually encounters problems in creating data fragments. Reference can be made to the following steps:

1. In fe.log, find the `Failed to create Partition'log for the corresponding time point. In this log, there will be a series of number pairs similar to {10001-10010}. The first number of the number pair represents the Backend ID, and the second number represents the Tablet ID. As the above number pair indicates, on the Backend with ID 10001, the creation of a Tablet with ID 10010 failed.
2. Go to the be.INFO log corresponding to Backend and find the tablet id-related log within the corresponding time period to find the error information.
3. Following are some common tablet creation failures, including but not limited to:
* BE did not receive the related task, and the tablet ID related log could not be found in be.INFO at this time. Or BE was created successfully, but failed to report. For the above questions, see the Deployment and Upgrade Document to check the connectivity between FE and BE.
* Pre-allocated memory failed. Perhaps the byte length of a row in the table exceeds 100KB.
*` Too many open files `. The number of open file handles exceeds the Linux system limit. Handle limit of Linux system needs to be modified.

You can also extend the timeout time by setting `tablet_create_timeout_second= xxx'in fe.conf. The default is 2 seconds.

3. Tabulation commands do not return results for a long time.

Doris's build command is a synchronization command. The command's timeout time is currently set in a relatively simple (tablet num * replication num) second. If you create more data fragments, and some of them fail to create fragments, you may be waiting for a longer timeout before returning an error.

Normally, the build statement will return in a few seconds or a dozen seconds. If it takes more than one minute, it is recommended to cancel this operation directly and go to the FE or BE log to check for related errors.
