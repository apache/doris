---
{
    "title": "CREATE SYNC JOB",
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

# CREATE SYNC JOB

## description

The sync job feature supports to submit a resident SyncJob, and CDC (change data capture) the user's update operation in MySQL database by reading the binlog log from the specified remote address.
	
At present, data synchronization only supports docking with the canal, getting the parsed binlog from the canal server and loading it into Doris.
	
You can view the SyncJob's status by command 'SHOW SYNC JOB'.
	
Syntax:

```
CREATE SYNC [db.]job_name
 (
 	channel_desc, 
 	channel_desc
 	...
 )
binlog_desc
```
   	
1. `job_name`

	job_Name is the unique identifier of the SyncJob in the current database. With a specified job name, only one SyncJob can be running at the same time.
   	
2. `channel_desc`

	The data channel under the job is used to describe the mapping relationship between the MySQL source table and the Doris target table.
	
	Syntax:
	
	```
	FROM mysql_db.src_tbl INTO des_tbl
	[partitions]
	[columns_mapping]
	```
	
	1. `mysql_db.src_tbl`

		Specify the database and source table on the MySQL side.
		
	2. `des_tbl`

		Specify the target table on the Doris side. Only the unique table is supported, and the batch delete feature of the table needs to be enabled.
		
	3. `partitions`

		Specify which partitions to be load into in target table. If it is not specified, it will be automatically loaded into the corresponding partition.
		
		Example:
		
		```
		PARTITION(p1, p2, p3)
		```
		
	4. `column_mapping`

		Specify the mapping relationship between the columns of the MySQL source table and the Doris target table. If not specified, Fe will default that the columns of the source table and the target table correspond one by one in order.
		
		Columns are not supported in the 'col_name = expr' form.
		
		Example:
		
		```
		Suppose the columns of target table are (K1, K2, V1),
		
		Change the order of columns K1 and K2
		COLUMNS(k2, k1, v1)
		
		Ignore the fourth column of the source data
		COLUMNS(k2, k1, v1, dummy_column)
		```
		
3. `binlog_desc`

	It is used to describe remote data sources. Currently, only canal is supported.
	
	Syntax:
	
	```
	FROM BINLOG
	(
		"key1" = "value1", 
		"key2" = "value2"
	)
	```
	
	1. The attribute related to the canal is prefixed with `canal.`

		1. canal.server.ip: the address of the canal server
		2. canal.server.port: the port of canal server
		3. canal.destination: Identifier of instance
		4. canal.batchSize: the maximum batch size. The default is 8192
		5. canal.username: the username of instance
		6. canal.password: password of instance
		7. canal.debug: optional. When set to true, the details of each batch and each row will be printed.

## example

1. create a sync job named `job1` for target table `test_tbl` in `test_db`,  connects to the local canal server, and corresponds to the MySQL source table `mysql_db1.tbl1`

		CREATE SYNC `test_db`.`job1`
		(
			FROM `mysql_db1`.`tbl1` INTO `test_tbl `
		)
		FROM BINLOG 
		(
			"type" = "canal",
			"canal.server.ip" = "127.0.0.1",
			"canal.server.port" = "11111",
			"canal.destination" = "example",
			"canal.username" = "",
			"canal.password" = ""
		);
		
2. create a sync job named `job1` for multiple target tables in `test_db`, correspond to multiple MySQL source tables one by one, and explicitly specify column mapping.

		CREATE SYNC `test_db`.`job1` 
		(
			FROM `mysql_db`.`t1` INTO `test1` COLUMNS(k1, k2, v1) PARTITIONS (p1, p2),
			FROM `mysql_db`.`t2` INTO `test2` COLUMNS(k3, k4, v2) PARTITION p1
		) 
		FROM BINLOG 
		(
			"type" = "canal", 
			"canal.server.ip" = "xx.xxx.xxx.xx", 
			"canal.server.port" = "12111", 
			"canal.destination" = "example",  
			"canal.username" = "username", 
			"canal.password" = "password"
		);

## keyword

	CREATE,SYNC,JOB,BINLOG


	
