---
{
    "title": "Multi-Catalog",
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

<version since="1.2.0">

# Multi-Catalog

Multi-Catalog is a feature introduced in Doris 1.2.0, which aims to make it easier to interface with external data sources to enhance Doris' data lake analysis and federated data query capabilities.

In previous versions of Doris, there were only two levels of user data: Database and Table. When we need to connect to an external data source, we can only connect at the Database or Table level. For example, create a mapping of a table in an external data source through `create external table`, or map a Database in an external data source through `create external database`. If there are too many Databases or Tables in the external data source, users need to manually map them one by one, and the experience is not good.

The new Multi-Catalog function adds a new layer of Catalog to the original metadata level, forming a three-layer metadata level of Catalog -> Database -> Table. Among them, Catalog can directly correspond to the external data source. Currently supported external data sources include:

1. Hive MetaStore: Connect to a Hive MetaStore, so that you can directly access Hive, Iceberg, Hudi and other data in it.
2. Elasticsearch: Connect to an ES cluster and directly access the tables and shards in it.

This function will be used as a supplement and enhancement to the previous external table connection method (External Table) to help users perform fast multi-catalog federated queries.

## Basic Concepts

1. Internal Catalog

	Doris's original Database and Table will belong to Internal Catalog. Internal Catalog is the built-in default Catalog, which cannot be modified or deleted by the user.

2. External Catalog

	An External Catalog can be created with the [CREATE CATALOG](../../sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-CATALOG.md) command. After creation, you can view the created catalog through the [SHOW CATALOGS](../../sql-manual/sql-reference/Show-Statements/SHOW-CATALOGS.md) command.

3. Switch Catalog

	After users log in to Doris, they enter the Internal Catalog by default, so the default usage is the same as the previous version. You can directly use `SHOW DATABASES`, `USE DB` and other commands to view and switch databases.

	Users can switch the catalog through the [SWITCH](../../sql-manual/sql-reference/Utility-Statements/SWITCH.md) command. like:

	````
	SWiTCH internal;
	SWITCH hive_catalog;
	````

	After switching, you can directly view and switch the Database in the corresponding Catalog through commands such as `SHOW DATABASES`, `USE DB`. Doris will automatically sync the Database and Table in the Catalog. Users can view and access data in the External Catalog as they would with the Internal Catalog.

	Currently, Doris only supports read-only access to data in the External Catalog.
	
4. Drop Catalog

	Both Database and Table in External Catalog are read-only. However, the catalog can be deleted (Internal Catalog cannot be deleted). An External Catalog can be dropped via the [DROP CATALOG](../../../sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-CATALOG) command.

	This operation will only delete the mapping information of the catalog in Doris, and will not modify or change the contents of any external data source.

## Samples

### Connect Hive MetaStore（Hive/Iceberg/Hudi）

> 1. hive supports version 2.3.7 and above.
> 2. Iceberg currently only supports V1 version, V2 version will be supported soon.
> 3. Hudi currently only supports Snapshot Query for Copy On Write tables and Read Optimized Query for Merge On Read tables. In the future, Incremental Query and Snapshot Query for Merge On Read tables will be supported soon.

The following example is used to create a Catalog named hive to connect the specified Hive MetaStore, and provide the HDFS HA connection properties to access the corresponding files in HDFS.

```
CREATE CATALOG hive PROPERTIES (
	"type"="hms",
	'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
	'dfs.nameservices'='service1',
	'dfs.ha.namenodes. service1'='nn1,nn2',
	'dfs.namenode.rpc-address.HDFS8000871.nn1'='172.21.0.2:4007',
	'dfs.namenode.rpc-address.HDFS8000871.nn2'='172.21.0.3:4007',
	'dfs.client.failover.proxy.provider.HDFS8000871'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```

Once created, you can view the catalog with the `SHOW CATALOGS` command:

```
mysql> SHOW CATALOGS;
+-----------+-------------+----------+
| CatalogId | CatalogName | Type     |
+-----------+-------------+----------+
|     10024 | hive        | hms      |
|         0 | internal    | internal |
+-----------+-------------+----------+
```

Switch to the hive catalog with the `SWITCH` command and view the databases in it:

```
mysql> SWITCH hive;
Query OK, 0 rows affected (0.00 sec)

mysql> SHOW DATABASES;
+-----------+
| Database  |
+-----------+
| default   |
| random    |
| ssb100    |
| tpch1     |
| tpch100   |
| tpch1_orc |
+-----------+
```

Switch to the tpch100 database and view the tables in it:

```
mysql> USE tpch100;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> SHOW TABLES;
+-------------------+
| Tables_in_tpch100 |
+-------------------+
| customer          |
| lineitem          |
| nation            |
| orders            |
| part              |
| partsupp          |
| region            |
| supplier          |
+-------------------+
```

View schema of table lineitem:

```
mysql> DESC lineitem;
+-----------------+---------------+------+------+---------+-------+
| Field           | Type          | Null | Key  | Default | Extra |
+-----------------+---------------+------+------+---------+-------+
| l_shipdate      | DATE          | Yes  | true | NULL    |       |
| l_orderkey      | BIGINT        | Yes  | true | NULL    |       |
| l_linenumber    | INT           | Yes  | true | NULL    |       |
| l_partkey       | INT           | Yes  | true | NULL    |       |
| l_suppkey       | INT           | Yes  | true | NULL    |       |
| l_quantity      | DECIMAL(15,2) | Yes  | true | NULL    |       |
| l_extendedprice | DECIMAL(15,2) | Yes  | true | NULL    |       |
| l_discount      | DECIMAL(15,2) | Yes  | true | NULL    |       |
| l_tax           | DECIMAL(15,2) | Yes  | true | NULL    |       |
| l_returnflag    | TEXT          | Yes  | true | NULL    |       |
| l_linestatus    | TEXT          | Yes  | true | NULL    |       |
| l_commitdate    | DATE          | Yes  | true | NULL    |       |
| l_receiptdate   | DATE          | Yes  | true | NULL    |       |
| l_shipinstruct  | TEXT          | Yes  | true | NULL    |       |
| l_shipmode      | TEXT          | Yes  | true | NULL    |       |
| l_comment       | TEXT          | Yes  | true | NULL    |       |
+-----------------+---------------+------+------+---------+-------+
```

Query:

```
mysql> SELECT l_shipdate, l_orderkey, l_partkey FROM lineitem limit 10;
+------------+------------+-----------+
| l_shipdate | l_orderkey | l_partkey |
+------------+------------+-----------+
| 1998-01-21 |   66374304 |    270146 |
| 1997-11-17 |   66374304 |    340557 |
| 1997-06-17 |   66374400 |   6839498 |
| 1997-08-21 |   66374400 |  11436870 |
| 1997-08-07 |   66374400 |  19473325 |
| 1997-06-16 |   66374400 |   8157699 |
| 1998-09-21 |   66374496 |  19892278 |
| 1998-08-07 |   66374496 |   9509408 |
| 1998-10-27 |   66374496 |   4608731 |
| 1998-07-14 |   66374592 |  13555929 |
+------------+------------+-----------+
```

You can also perform associated queries with tables in other data catalogs:

```
mysql> SELECT l.l_shipdate FROM hive.tpch100.lineitem l WHERE l.l_partkey IN (SELECT p_partkey FROM internal.db1.part) LIMIT 10;
+------------+
| l_shipdate |
+------------+
| 1993-02-16 |
| 1995-06-26 |
| 1995-08-19 |
| 1992-07-23 |
| 1998-05-23 |
| 1997-07-12 |
| 1994-03-06 |
| 1996-02-07 |
| 1997-06-01 |
| 1996-08-23 |
+------------+
```

Here we identify a table in a fully qualified way of `catalog.database.table`, such as: `internal.db1.part`.

`catalog` and `database` can be omitted, and the catalog and database switched after the current SWITCH and USE are used by default.

The table data in the hive catalog can be inserted into the internal table in the internal catalog through the INSERT INTO command, so as to achieve the effect of **importing external data source's data**:

```
mysql> SWITCH internal;
Query OK, 0 rows affected (0.00 sec)

mysql> USE db1;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> INSERT INTO part SELECT * FROM hive.tpch100.part limit 1000;
Query OK, 1000 rows affected (0.28 sec)
{'label':'insert_212f67420c6444d5_9bfc184bf2e7edb8', 'status':'VISIBLE', 'txnId':'4'}
```

### Connect Elasticsearch

> 1. 5.x and later versions are supported.
> 2. In 5.x and 6.x, multiple types in an index are taken as the first by default.

The following example creates a Catalog connection named es to the specified ES and turns off node discovery.

```
CREATE CATALOG es PROPERTIES (
	"type"="es",
	"elasticsearch.hosts"="http://192.168.120.12:29200",
	"elasticsearch.nodes_discovery"="false"
);
```

Once created, you can view the catalog with the `SHOW CATALOGS` command:

```
mysql> SHOW CATALOGS;
+-----------+-------------+----------+
| CatalogId | CatalogName | Type     |
+-----------+-------------+----------+
|         0 | internal    | internal |
|     11003 | es          | es       |
+-----------+-------------+----------+
2 rows in set (0.02 sec)
```

Switch to the hive catalog with the `SWITCH` command and view the databases in it(Only one default_db associates all index)

```
mysql> SWITCH es;
Query OK, 0 rows affected (0.00 sec)

mysql> SHOW DATABASES;
+------------+
| Database   |
+------------+
| default_db |
+------------+

mysql> show tables;
+----------------------+
| Tables_in_default_db |
+----------------------+
| test                 |
| test2                |
+----------------------+
```

Query

```
mysql> select * from test;
+------------+-------------+--------+-------+
| test4      | test2       | test3  | test1 |
+------------+-------------+--------+-------+
| 2022-08-08 | hello world |  2.415 | test2 |
| 2022-08-08 | hello world | 3.1415 | test1 |
+------------+-------------+--------+-------+
```

#### Parameters:

Parameter | Description
---|---
**elasticsearch.hosts** | ES Connection Address, maybe one or more node, load-balance is also accepted
**elasticsearch.username** | username for ES
**elasticsearch.password** | password for the user
**elasticsearch.doc_value_scan** | whether to enable ES/Lucene column storage to get the value of the query field, the default is false
**elasticsearch.keyword_sniff** | Whether to detect the string type text.fields in ES to obtain additional not analyzed keyword field name multi-fields mechanism
**elasticsearch.nodes_discovery** | Whether or not to enable ES node discovery, the default is true. In network isolation, set this parameter to false. Only the specified node is connected.
**elasticsearch.ssl** | Whether ES cluster enables https access mode, the current FE/BE implementation is to trust all

### Connect Aliyun Data Lake Formation

> [What is Data Lake Formation](https://www.alibabacloud.com/product/datalake-formation)

1. Create hive-site.xml

	Create hive-site.xml and put it in `fe/conf` and `be/conf`.
	
	```
	<?xml version="1.0"?>
	<configuration>
	    <!--Set to use dlf client-->
	    <property>
	        <name>hive.metastore.type</name>
	        <value>dlf</value>
	    </property>
	    <property>
	        <name>dlf.catalog.endpoint</name>
	        <value>dlf-vpc.cn-beijing.aliyuncs.com</value>
	    </property>
	    <property>
	        <name>dlf.catalog.region</name>
	        <value>cn-beijing</value>
	    </property>
	    <property>
	        <name>dlf.catalog.proxyMode</name>
	        <value>DLF_ONLY</value>
	    </property>
	    <property>
	        <name>dlf.catalog.uid</name>
	        <value>20000000000000000</value>
	    </property>
	    <property>
	        <name>dlf.catalog.accessKeyId</name>
	        <value>XXXXXXXXXXXXXXX</value>
	    </property>
	    <property>
	        <name>dlf.catalog.accessKeySecret</name>
	        <value>XXXXXXXXXXXXXXXXX</value>
	    </property>
	</configuration>
	```

	* `dlf.catalog.endpoint`: DLF Endpoint. See: [Regions and endpoints of DLF](https://www.alibabacloud.com/help/en/data-lake-formation/latest/regions-and-endpoints)
	* `dlf.catalog.region`: DLF Regio. See: [Regions and endpoints of DLF](https://www.alibabacloud.com/help/en/data-lake-formation/latest/regions-and-endpoints)
	* `dlf.catalog.uid`: Ali Cloud Account ID. That is, the "cloud account ID" of the personal information in the upper right corner of the Alibaba Cloud console.	* `dlf.catalog.accessKeyId`: AccessKey. See: [Ali Could Console](https://ram.console.aliyun.com/manage/ak).
	* `dlf.catalog.accessKeySecret`: SecretKey. See: [Ali Could Console](https://ram.console.aliyun.com/manage/ak).

	Other configuration items are fixed values and do not need to be changed.

2. Restart FE and create a catalog with the `CREATE CATALOG` statement.

	```
	CREATE CATALOG dlf PROPERTIES (
	    "type"="hms",
	    "hive.metastore.uris" = "thrift://127.0.0.1:9083"
	);
	```
	
	where `type` is fixed to `hms`. The value of `hive.metastore.uris` can be filled in at will, but it will not be used in practice. But it needs to be filled in the standard hive metastore thrift uri format.

After that, the metadata under DLF can be accessed like a normal Hive MetaStore.

## Column Type Mapping

After the user creates the catalog, Doris will automatically synchronize the database and tables of the data catalog. For different data catalog and data table formats, Doris will perform the following mapping relationships.

### Hive MetaStore

For Hive/Iceberge/Hudi

| HMS Type | Doris Type | Comment |
|---|---|---|
| boolean| boolean | |
| tinyint|tinyint | |
| smallint| smallint| |
| int| int | |
| bigint| bigint | |
| date| date| |
| timestamp| datetime| |
| float| float| |
| double| double| |
| `array<type>` | `array<type>`| Supprot nested array, such as `array<array<int>>` |
| char| char | |
| varchar| varchar| |
| decimal| decimal | |
| other | string | The rest of the unsupported types are uniformly processed as string |

### Elasticsearch

| HMS Type | Doris Type | Comment |
|---|---|---|
| boolean | boolean | |
| byte| tinyint| |
| short| smallint| |
| integer| int| |
| long| bigint| |
| unsigned_long| largeint | |
| float| float| |
| half_float| float| |
| double | double | |
| scaled_float| double | |
| date | date | |
| keyword | string | |
| text |string | |
| ip |string | |
| nested |string | |
| object |string | |
| array | | Comming soon |
|other| string ||

## Privilege Management

Using Doris to access the databases and tables in the External Catalog is not controlled by the permissions of the external data source itself, but relies on Doris's own permission access management.

The privilege management of Doris provides an extension to the Cataloig level. For details, please refer to the [privilege management](../../admin-manual/privilege-ldap/user-privilege.md) document.

## Metadata Refresh

Metadata changes of external data sources, such as creating, dropping tables, adding or dropping columns, etc., will not be synchronized to Doris.

Currently, users need to manually refresh metadata via the [REFRESH CATALOG](../../sql-manual/sql-reference/Utility-Statements/REFRESH-CATALOG.md) command.

Automatic synchronization of metadata will be supported soon.

</version>