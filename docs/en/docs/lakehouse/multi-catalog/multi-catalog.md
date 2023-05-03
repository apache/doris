---
{
    "title": "Multi Catalog",
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


# Multi Catalog

<version since="1.2.0">

Multi-Catalog is a newly added feature in Doris 1.2.0. It allows Doris to interface with external catalogs more conveniently and thus increases the data lake analysis and federated query capabilities of Doris.

In older versions of Doris, user data is in a two-tiered structure: database and table. Thus, connections to external catalogs could only be done at the database or table level. For example, users could create a mapping to a table in an external catalog via `create external table`, or to a database via `create external database` . If there were large amounts of databases or tables in the external catalog, users would need to create mappings to them one by one, which could be a heavy workload.

With the advent of Multi-Catalog, Doris now has a new three-tiered metadata hierarchy (catalog -> database -> table), which means users can connect to external data at the catalog level. The currently supported external catalogs include:

1. Hive
2. Iceberg
3. Hudi
4. Elasticsearch
5. JDBC

Multi-Catalog works as an additional and enhanced external table connection method. It helps users conduct multi-catalog federated queries quickly. 

</version>

## Basic Concepts

1. Internal Catalog

    Existing databases and tables in Doris are all under the Internal Catalog, which is the default catalog in Doris and cannot be modified or deleted.

2. External Catalog

    Users can create an External Catalog using the [CREATE CATALOG](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-CATALOG/) command, and view the existing Catalogs via the [SHOW CATALOGS](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Show-Statements/SHOW-CATALOGS/) command.

3. Switch Catalog

    After login, you will enter the Internal Catalog by default. Then, you can view or switch to your target database via `SHOW DATABASES` and `USE DB` . 
    
    Example of switching catalog:
    
    ```
    SWITCH internal;
    SWITCH hive_catalog;
    ```
    
    After switching catalog, you can view or switch to your target database in that catalog via `SHOW DATABASES` and `USE DB` . You can view and access data in External Catalogs the same way as doing that in the Internal Catalog.
    
    Doris only supports read-only access to data in External Catalogs currently. 
    
4. Delete Catalog

    Databases and tables in External Catalogs are for read only, but External Catalogs are deletable via the [DROP CATALOG](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Drop/DROP-CATALOG/) command. (The Internal Catalog cannot be deleted.)
    
    The deletion only means to remove the mapping in Doris to the corresponding catalog. It doesn't change the external catalog itself by all means.
    
5. Resource

	Resource is a set of configurations. Users can create a Resource using the [CREATE RESOURCE](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-RESOURCE/) command, and then apply this Resource for a newly created Catalog. One Resource can be reused for multiple Catalogs. 

## Examples

### Connect to Hive

The followings are the instruction on how to connect to a Hive catalog using the Catalog feature.

For more information about Hive, please see [Hive](./hive.md).

1. Create Catalog

	```sql
	CREATE CATALOG hive PROPERTIES (
	    'type'='hms',
	    'hive.metastore.uris' = 'thrift://172.21.0.1:7004'
	);
	```
	
	> Syntax Help: [CREATE CATALOG](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Create/CREATE-CATALOG/)
	
2. View Catalog

	View existing Catalogs via the `SHOW CATALOGS` command:
	
	```
	mysql> SHOW CATALOGS;
	+-----------+-------------+----------+
	| CatalogId | CatalogName | Type     |
	+-----------+-------------+----------+
	|     10024 | hive        | hms      |
	|         0 | internal    | internal |
	+-----------+-------------+----------+
	```
	
	> Syntax Help: [SHOW CATALOGS](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Show-Statements/SHOW-CATALOGS/)
	
	> You can view the CREATE CATALOG statement via [SHOW CREATE CATALOG](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Show-Statements/SHOW-CREATE-CATALOG/).
	
	> You can modify the Catalog PROPERTIES via [ALTER CATALOG](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Definition-Statements/Alter/ALTER-CATALOG/).
	
3. Switch Catalog

	Switch to the Hive Catalog using the `SWITCH`  command, and view the databases in it:

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
	
	> Syntax Help: [SWITCH](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Utility-Statements/SWITCH/)

4. Use the Catalog

	After switching to the Hive Catalog, you can use the relevant features.
	
	For example, you can switch to Database tpch100, and view the tables in it:
	
	```
	mysql> USE tpch100;
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
	
	You can view the schema of Table lineitem:
	
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
	
	You can perform a query:
	
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
	
	Or you can conduct a join query:
	
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

	The table is identified in the format of `catalog.database.table` . For example, `internal.db1.part`  in the above snippet.
	
	If the target table is in the current Database of the current Catalog,  `catalog` and `database` in the format can be omitted.
	
	You can use the `INSERT INTO` command to insert table data from the Hive Catalog into a table in the Internal Catalog. This is how you can **import data from External Catalogs to the Internal Catalog**:
	
	```
	mysql> SWITCH internal;
	Query OK, 0 rows affected (0.00 sec)
	
	mysql> USE db1;
	Database changed
	
	mysql> INSERT INTO part SELECT * FROM hive.tpch100.part limit 1000;
	Query OK, 1000 rows affected (0.28 sec)
	{'label':'insert_212f67420c6444d5_9bfc184bf2e7edb8', 'status':'VISIBLE', 'txnId':'4'}
	```

### Connect to Iceberg

See [Iceberg](./iceberg.md)

### Connect to Hudi

See [Hudi](./hudi.md)

### Connect to Elasticsearch

See [Elasticsearch](./es.md)

### Connect to JDBC

See [JDBC](./jdbc.md)

## Column Type Mapping

After you create a Catalog, Doris will automatically synchronize the databases and tables from the corresponding external catalog to it. The following shows how Doris maps different types of catalogs and tables.

<version since="1.2.2">

As for types that cannot be mapped to a Doris column type, such as `UNION` and `INTERVAL` , Doris will map them to an UNSUPPORTED type. Here are examples of queries in a table containing UNSUPPORTED types:

Suppose the table is of the following schema:

```
k1 INT,
k2 INT,
k3 UNSUPPORTED,
k4 INT
```

```
select * from table;                // Error: Unsupported type 'UNSUPPORTED_TYPE' in '`k3`
select * except(k3) from table;     // Query OK.
select k1, k3 from table;           // Error: Unsupported type 'UNSUPPORTED_TYPE' in '`k3`
select k1, k4 from table;           // Query OK.
```

</version>

You can find more details of the mapping of various data sources (Hive, Iceberg, Hudi, Elasticsearch, and JDBC) in the corresponding pages.

## Privilege Management

Access from Doris to databases and tables in an External Catalog is not under the privilege control of the external catalog itself, but is authorized by Doris.

Along with the new Multi-Catalog feature, we also added privilege management at the Catalog level (See [Privilege Management](https://doris.apache.org/docs/dev/admin-manual/privilege-ldap/user-privilege/) for details).

## Metadata Update

### Manual Update

By default, changes in metadata of external data sources, including addition or deletion of tables and columns, will not be synchronized into Doris.

Users need to manually update the metadata using the  [REFRESH CATALOG](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Utility-Statements/REFRESH/) command.

### Automatic Update

<version since="1.2.2"></version>

#### Hive Metastore

Currently, Doris only supports automatic update of metadata in Hive Metastore (HMS). It perceives changes in metadata by the FE node which regularly reads the notification events from HMS. The supported events are as follows:

| Event           | Corresponding Update Operation                               |
| :-------------- | :----------------------------------------------------------- |
| CREATE DATABASE | Create a database in the corresponding catalog.              |
| DROP DATABASE   | Delete a database in the corresponding catalog.              |
| ALTER DATABASE  | Such alterations mainly include changes in properties, comments, or storage location of databases. They do not affect Doris' queries in External Catalogs so they will not be synchronized. |
| CREATE TABLE    | Create a table in the corresponding database.                |
| DROP TABLE      | Delete a table in the corresponding database, and invalidate the cache of that table. |
| ALTER TABLE     | If it is a renaming, delete the table of the old name, and then create a new table with the new name; otherwise, invalidate the cache of that table. |
| ADD PARTITION   | Add a partition to the cached partition list of the corresponding table. |
| DROP PARTITION  | Delete a partition from the cached partition list of the corresponding table, and invalidate the cache of that partition. |
| ALTER PARTITION | If it is a renaming, delete the partition of the old name, and then create a new partition with the new name; otherwise, invalidate the cache of that partition. |

> After data ingestion, changes in partition tables will follow the `ALTER PARTITION` logic, while those in non-partition tables will follow the `ALTER TABLE` logic.
>
> If changes are conducted on the file system directly instead of through the HMS, the HMS will not generate an event. As a result, such changes will not be perceived by Doris.

The automatic update feature involves the following parameters in fe.conf:

1. `enable_hms_events_incremental_sync`: This specifies whether to enable automatic incremental synchronization for metadata, which is disabled by default. 
2. `hms_events_polling_interval_ms`: This specifies the interval between two readings, which is set to 10000 by default. (Unit: millisecond) 
3. `hms_events_batch_size_per_rpc`: This specifies the maximum number of events that are read at a time, which is set to 500 by default.

To enable automatic update, you need to modify the hive-site.xml of HMS and then restart HMS:

```
<property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
</property>
<property>
    <name>hive.metastore.dml.events</name>
    <value>true</value>
</property>
<property>
    <name>hive.metastore.transactional.event.listeners</name>
    <value>org.apache.hive.hcatalog.listener.DbNotificationListener</value>
</property>

```

> Note: To enable automatic update, whether for existing Catalogs or newly created Catalogs, all you need is to set `enable_hms_events_incremental_sync` to `true`, and then restart the FE node. You don't need to manually update the metadata before or after the restart.

<version since="dev">

#### Timing Refresh

When creating a catalog, specify the refresh time parameter `metadata_refresh_interval_sec` in the properties, in seconds. If this parameter is set when creating a catalog, the master node of FE will refresh the catalog regularly according to the parameter value. Three types are currently supported

- hms: Hive MetaStore
-es: Elasticsearch
- jdbc: Standard interface for database access (JDBC)

##### Example

```
-- Set the catalog refresh interval to 20 seconds
CREATE CATALOG es PROPERTIES (
     "type"="es",
     "hosts"="http://127.0.0.1:9200",
     "metadata_refresh_interval_sec"="20"
);
```

</version>
