# doris-iceberg-paimon-compose


## Launch Docker Compose
First, we need to ensure the environmental parameters of the machine.

```
sysctl -w vm.max_map_count=2000000
```

We can then start all the required containers via the script.

```
bash start_all.sh
```
It will start a set of docker, the environment includes:
- doris
- iceberg
- paimon
- flink
- spark

And it will automatically create an iceberg table and a paimon table. We can use these tables directly to experience doris.


## paimon table test

Enter the flink client.

```
bash start_flink_client.sh
```

Here is a table that has been created.

```sql

Flink SQL> use paimon.db_paimon;
[INFO] Execute statement succeed.

Flink SQL> show tables;
+------------+
| table name |
+------------+
|   customer |
+------------+
1 row in set

Flink SQL> show create table customer;
+------------------------------------------------------------------------+
|                                                                 result |
+------------------------------------------------------------------------+
| CREATE TABLE `paimon`.`db_paimon`.`customer` (
  `c_custkey` INT NOT NULL,
  `c_name` VARCHAR(25),
  `c_address` VARCHAR(40),
  `c_nationkey` INT NOT NULL,
  `c_phone` CHAR(15),
  `c_acctbal` DECIMAL(12, 2),
  `c_mktsegment` CHAR(10),
  `c_comment` VARCHAR(117),
  CONSTRAINT `PK_c_custkey_c_nationkey` PRIMARY KEY (`c_custkey`, `c_nationkey`) NOT ENFORCED
) PARTITIONED BY (`c_nationkey`)
WITH (
  'bucket' = '1',
  'path' = 's3://warehouse/wh/db_paimon.db/customer',
  'deletion-vectors.enabled' = 'true'
)
 |
+-------------------------------------------------------------------------+
1 row in set

Flink SQL> desc customer;
+--------------+----------------+-------+-----------------------------+--------+-----------+
|         name |           type |  null |                         key | extras | watermark |
+--------------+----------------+-------+-----------------------------+--------+-----------+
|    c_custkey |            INT | FALSE | PRI(c_custkey, c_nationkey) |        |           |
|       c_name |    VARCHAR(25) |  TRUE |                             |        |           |
|    c_address |    VARCHAR(40) |  TRUE |                             |        |           |
|  c_nationkey |            INT | FALSE | PRI(c_custkey, c_nationkey) |        |           |
|      c_phone |       CHAR(15) |  TRUE |                             |        |           |
|    c_acctbal | DECIMAL(12, 2) |  TRUE |                             |        |           |
| c_mktsegment |       CHAR(10) |  TRUE |                             |        |           |
|    c_comment |   VARCHAR(117) |  TRUE |                             |        |           |
+--------------+----------------+-------+-----------------------------+--------+-----------+
8 rows in set
```

This table already has some data.

```sql
Flink SQL> select * from customer order by c_custkey limit 4;
+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+--------------------------------+
| c_custkey |             c_name |                      c_address | c_nationkey |         c_phone | c_acctbal | c_mktsegment |                      c_comment |
+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+--------------------------------+
|         1 | Customer#000000001 |              IVhzIApeRb ot,c,E |          15 | 25-989-741-2988 |    711.56 |     BUILDING | to the even, regular platel... |
|         2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak |          13 | 23-768-687-3665 |    121.65 |   AUTOMOBILE | l accounts. blithely ironic... |
|         3 | Customer#000000003 |                   MG9kdTD2WBHm |           1 | 11-719-748-3364 |   7498.12 |   AUTOMOBILE |  deposits eat slyly ironic,... |
|        32 | Customer#000000032 | jD2xZzi UmId,DCtNBLXKj9q0Tl... |          15 | 25-430-914-2194 |   3471.53 |     BUILDING | cial ideas. final, furious ... |
+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+--------------------------------+
4 rows in set

Flink SQL> select * from customer order by c_custkey desc limit 4;
+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+--------------------------------+
| c_custkey |             c_name |                      c_address | c_nationkey |         c_phone | c_acctbal | c_mktsegment |                      c_comment |
+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+--------------------------------+
|    149987 | Customer#000149987 |           P6z8nSIgW55cSydfa1bZ |           8 | 18-187-349-6326 |   5338.96 |    HOUSEHOLD | aggle carefully quickly reg... |
|    149986 | Customer#000149986 |       HyEJpj2jvEqt,,pA50NOvuTP |           7 | 17-654-752-5642 |   1251.17 |     BUILDING | enticingly carefully carefu... |
|    149985 | Customer#000149985 | y4m,kcxXX6ZtGTJGxavBTJf52OM... |          22 | 32-595-455-4078 |   6012.98 |    MACHINERY | kages affix against the bli... |
|    149984 | Customer#000149984 | ZBEyUfjRsVtUNSIv9dnnyoPYeQw... |          12 | 22-283-613-7016 |   6567.62 |    HOUSEHOLD | ges integrate along the bli... |
+-----------+--------------------+--------------------------------+-------------+-----------------+-----------+--------------+--------------------------------+
4 rows in set
```

Now we can query this table through doris.

```
bash start_doris_client.sh
```

After entering the doris client, the paimon catalog has been created here, so the data of the paimon table can be directly queried.

```sql
mysql> use paimon.db_paimon;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> show tables;
+---------------------+
| Tables_in_db_paimon |
+---------------------+
| customer            |
+---------------------+
1 row in set (0.00 sec)

mysql> select * from customer order by c_custkey limit 4;
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address                             | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                              |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
|         1 | Customer#000000001 | IVhzIApeRb ot,c,E                     |          15 | 25-989-741-2988 |    711.56 | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e                                         |
|         2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        |          13 | 23-768-687-3665 |    121.65 | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref                                        |
|         3 | Customer#000000003 | MG9kdTD2WBHm                          |           1 | 11-719-748-3364 |   7498.12 | AUTOMOBILE   |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov |
|        32 | Customer#000000032 | jD2xZzi UmId,DCtNBLXKj9q0Tlp2iQ6ZcO3J |          15 | 25-430-914-2194 |   3471.53 | BUILDING     | cial ideas. final, furious requests across the e                                                       |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
4 rows in set (1.89 sec)

mysql> select * from customer order by c_custkey desc limit 4;
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address                             | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                             |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------+
|    149987 | Customer#000149987 | P6z8nSIgW55cSydfa1bZ                  |           8 | 18-187-349-6326 |   5338.96 | HOUSEHOLD    | aggle carefully quickly regular ideas-- ironic, bold packages are. regular tithes cajole regular requ |
|    149986 | Customer#000149986 | HyEJpj2jvEqt,,pA50NOvuTP              |           7 | 17-654-752-5642 |   1251.17 | BUILDING     | enticingly carefully careful courts. furiously                                                        |
|    149985 | Customer#000149985 | y4m,kcxXX6ZtGTJGxavBTJf52OMqBK9z      |          22 | 32-595-455-4078 |   6012.98 | MACHINERY    | kages affix against the blithely pending foxes. slyly final packages boost                            |
|    149984 | Customer#000149984 | ZBEyUfjRsVtUNSIv9dnnyoPYeQwi7czgCeeeM |          12 | 22-283-613-7016 |   6567.62 | HOUSEHOLD    | ges integrate along the blithely unusual                                                              |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------+
4 rows in set (0.35 sec)
```

Doris can perform partition pruning on Paimon and speed up the query process through native reading. We can check this through `explain verbose`.

```sql
mysql> explain verbose select * from customer where c_nationkey < 3;
+------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                                                                                |
+------------------------------------------------------------------------------------------------------------------------------------------------+
| ...............                                                                                                                                |
|   0:VPAIMON_SCAN_NODE(68)                                                                                                                      |
|      table: customer                                                                                                                           |
|      predicates: (c_nationkey[#3] < 3)                                                                                                         |
|      inputSplitNum=3, totalFileSize=193823, scanRanges=3                                                                                       |
|      partition=3/0                                                                                                                             |
|      backends:                                                                                                                                 |
|        10002                                                                                                                                   |
|          s3://warehouse/wh/db_paimon.db/customer/c_nationkey=1/bucket-0/data-15cee5b7-1bd7-42ca-9314-56d92c62c03b-0.orc start: 0 length: 66600 |
|          s3://warehouse/wh/db_paimon.db/customer/c_nationkey=2/bucket-0/data-e98fb7ef-ec2b-4ad5-a496-713cb9481d56-0.orc start: 0 length: 64059 |
|          s3://warehouse/wh/db_paimon.db/customer/c_nationkey=0/bucket-0/data-431be05d-50fa-401f-9680-d646757d0f95-0.orc start: 0 length: 63164 |
|      cardinality=18751, numNodes=1                                                                                                             |
|      pushdown agg=NONE                                                                                                                         |
|      paimonNativeReadSplits=3/3                                                                                                                |
|      PaimonSplitStats:                                                                                                                         |
|        SplitStat [type=NATIVE, rowCount=771, rawFileConvertable=true, hasDeletionVector=false]                                                 |
|        SplitStat [type=NATIVE, rowCount=750, rawFileConvertable=true, hasDeletionVector=false]                                                 |
|        SplitStat [type=NATIVE, rowCount=750, rawFileConvertable=true, hasDeletionVector=false]                                                 |
|      tuple ids: 0                                                                                                                              |
|                                                                                                                                                |
| Tuples:                                                                                                                                        |
| TupleDescriptor{id=0, tbl=customer}                                                                                                            |
|   SlotDescriptor{id=0, col=c_custkey, colUniqueId=0, type=INT, nullable=true, isAutoIncrement=false, subColPath=null}                          |
|   SlotDescriptor{id=1, col=c_name, colUniqueId=1, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                            |
|   SlotDescriptor{id=2, col=c_address, colUniqueId=2, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                         |
|   SlotDescriptor{id=3, col=c_nationkey, colUniqueId=3, type=INT, nullable=true, isAutoIncrement=false, subColPath=null}                        |
|   SlotDescriptor{id=4, col=c_phone, colUniqueId=4, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                           |
|   SlotDescriptor{id=5, col=c_acctbal, colUniqueId=5, type=DECIMALV3(12, 2), nullable=true, isAutoIncrement=false, subColPath=null}             |
|   SlotDescriptor{id=6, col=c_mktsegment, colUniqueId=6, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                      |
|   SlotDescriptor{id=7, col=c_comment, colUniqueId=7, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                         |
|                                                                                                                                                |
|                                                                                                                                                |
|                                                                                                                                                |
| Statistics                                                                                                                                     |
|  planed with unknown column statistics                                                                                                         |
+------------------------------------------------------------------------------------------------------------------------------------------------+
66 rows in set (0.17 sec)
```

Through the query plan, we can see that doris only reads three partition files whose c_nationkey is less than 3, and the file reading method is native.
In addition, doris supports the `deletion vectors` of paimon.
First, we will modify some data through flink.

```sql
Flink SQL> update customer set c_address='c_address_update' where c_nationkey = 1;
[INFO] Submitting SQL update statement to the cluster...
[INFO] SQL update statement has been successfully submitted to the cluster:
Job ID: ff838b7b778a94396b332b0d93c8f7ac

```

After waiting for the task to be completed, we can view the modified data through doris.

```sql
mysql> explain verbose select * from customer where c_nationkey < 3;
+------------------------------------------------------------------------------------------------------------------------------------------------+
| Explain String(Nereids Planner)                                                                                                                |
+------------------------------------------------------------------------------------------------------------------------------------------------+
| ...............                                                                                                                                |
|                                                                                                                                                |
|   0:VPAIMON_SCAN_NODE(68)                                                                                                                      |
|      table: customer                                                                                                                           |
|      predicates: (c_nationkey[#3] < 3)                                                                                                         |
|      inputSplitNum=4, totalFileSize=238324, scanRanges=4                                                                                       |
|      partition=3/0                                                                                                                             |
|      backends:                                                                                                                                 |
|        10002                                                                                                                                   |
|          s3://warehouse/wh/db_paimon.db/customer/c_nationkey=1/bucket-0/data-15cee5b7-1bd7-42ca-9314-56d92c62c03b-0.orc start: 0 length: 66600 |
|          s3://warehouse/wh/db_paimon.db/customer/c_nationkey=1/bucket-0/data-5d50255a-2215-4010-b976-d5dc656f3444-0.orc start: 0 length: 44501 |
|          s3://warehouse/wh/db_paimon.db/customer/c_nationkey=2/bucket-0/data-e98fb7ef-ec2b-4ad5-a496-713cb9481d56-0.orc start: 0 length: 64059 |
|          s3://warehouse/wh/db_paimon.db/customer/c_nationkey=0/bucket-0/data-431be05d-50fa-401f-9680-d646757d0f95-0.orc start: 0 length: 63164 |
|      cardinality=18751, numNodes=1                                                                                                             |
|      pushdown agg=NONE                                                                                                                         |
|      paimonNativeReadSplits=4/4                                                                                                                |
|      PaimonSplitStats:                                                                                                                         |
|        SplitStat [type=NATIVE, rowCount=1542, rawFileConvertable=true, hasDeletionVector=true]                                                 |
|        SplitStat [type=NATIVE, rowCount=750, rawFileConvertable=true, hasDeletionVector=false]                                                 |
|        SplitStat [type=NATIVE, rowCount=750, rawFileConvertable=true, hasDeletionVector=false]                                                 |
|      tuple ids: 0                                                                                                                              |
|                                                                                                                                                |
| Tuples:                                                                                                                                        |
| TupleDescriptor{id=0, tbl=customer}                                                                                                            |
|   SlotDescriptor{id=0, col=c_custkey, colUniqueId=0, type=INT, nullable=true, isAutoIncrement=false, subColPath=null}                          |
|   SlotDescriptor{id=1, col=c_name, colUniqueId=1, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                            |
|   SlotDescriptor{id=2, col=c_address, colUniqueId=2, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                         |
|   SlotDescriptor{id=3, col=c_nationkey, colUniqueId=3, type=INT, nullable=true, isAutoIncrement=false, subColPath=null}                        |
|   SlotDescriptor{id=4, col=c_phone, colUniqueId=4, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                           |
|   SlotDescriptor{id=5, col=c_acctbal, colUniqueId=5, type=DECIMALV3(12, 2), nullable=true, isAutoIncrement=false, subColPath=null}             |
|   SlotDescriptor{id=6, col=c_mktsegment, colUniqueId=6, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                      |
|   SlotDescriptor{id=7, col=c_comment, colUniqueId=7, type=TEXT, nullable=true, isAutoIncrement=false, subColPath=null}                         |
|                                                                                                                                                |
|                                                                                                                                                |
|                                                                                                                                                |
| Statistics                                                                                                                                     |
|  planed with unknown column statistics                                                                                                         |
+------------------------------------------------------------------------------------------------------------------------------------------------+
67 rows in set (0.23 sec)
```

From the plan, we can see that doris reads 4 splits using the native method, and the corresponding  paimon splits are 3, one of which carries the deletion vector.
Finally, we can read the modified data by doris.

```sql
mysql> select * from customer where c_nationkey=1 limit 2;
+-----------+--------------------+-----------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address       | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                              |
+-----------+--------------------+-----------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
|         3 | Customer#000000003 | c_address_update |           1 | 11-719-748-3364 |   7498.12 | AUTOMOBILE   |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov |
|       513 | Customer#000000513 | c_address_update |           1 | 11-861-303-6887 |    955.37 | HOUSEHOLD    | press along the quickly regular instructions. regular requests against the carefully ironic s          |
+-----------+--------------------+-----------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
2 rows in set (0.19 sec)

```
