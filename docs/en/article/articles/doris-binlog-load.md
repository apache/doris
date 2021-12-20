---
{
    "title": "How to use Apache Doris Binlog Load and examples",
    "description": "Binlog Load provides a CDC (Change Data Capture) function that enables Doris to incrementally synchronize the user's data update operation in the Mysql database, making it more convenient for users to complete the import of Mysql data.",
    "date": "2021-11-10",
    "metaTitle": "How to use Apache Doris Binlog Load and examples",
    "language": "en",
    "isArticle": true,
    "author": "张家锋",
    "layout": "Article",
    "sidebar": false
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

Binlog Load provides a CDC (Change Data Capture) function that enables Doris to incrementally synchronize the user's data update operation in the Mysql database, making it more convenient for users to complete the import of Mysql data.

>Note:
>
>This function needs to be used in 0.15 and later versions

## 1. Install and configure Mysql

1. Install Mysql

   Quickly use Docker to install and configure Mysql, refer to the following link for details

   https://segmentfault.com/a/1190000021523570

   If it is installed on a physical machine, please refer to the following connection:

   [在 CentOS 7 中安装 MySQL 8 的教程详解](https://cloud.tencent.com/developer/article/1721575)

2. enable Mysql binlog

   Enter the Docker container or modify the `/etc/my.cnf` file on the physical machine, and add the following content under [mysqld],

   ```
   log_bin=mysql_bin
   binlog-format=Row
   server-id=1
   ```

   Then restart Mysql

   ```
   systemctl restart mysqld
   ```

3. Create Mysql table

   ```sql
   create database demo;
   
    CREATE TABLE `test_cdc` (
     `id` int NOT NULL AUTO_INCREMENT,
     `sex` TINYINT(1) DEFAULT NULL,
     `name` varchar(20) DEFAULT NULL,
     `address` varchar(255) DEFAULT NULL,
     PRIMARY KEY (`id`)
    ) ENGINE=InnoDB
   ```


## 2. Install and configure Canal

Download canal-1.1.5: https://github.com/alibaba/canal/releases/download/canal-1.1.5/canal.deployer-1.1.5.tar.gz

1. Unzip Canal to the specified directory:

   ```shell
   tar zxvf canal.deployer-1.1.5.tar.gz -C ./canal
   ```

2. Create a new directory in the conf folder and rename it as the root directory of the instance. You can name the directory for easy identification.

   For example, my name here is consistent with the name of my database library: demo 

   ```shell
   vi conf/demo/instance.properties
   ```

   Given below is a sample configuration of mine:

   For the parameter description, please refer to Canal official documentation：[QuickStart ](https://github.com/alibaba/canal/wiki/QuickStart)

   ```ini
   #################################################
   ## mysql serverId , v1.0.26+ will autoGen
   canal.instance.mysql.slaveId=12115
   
   # enable gtid use true/false
   canal.instance.gtidon=false
   
   # position info
   canal.instance.master.address=10.220.146.11:3306
   canal.instance.master.journal.name=
   canal.instance.master.position=
   canal.instance.master.timestamp=
   canal.instance.master.gtid=
   
   # rds oss binlog
   canal.instance.rds.accesskey=
   canal.instance.rds.secretkey=
   canal.instance.rds.instanceId=
   
   # table meta tsdb info
   canal.instance.tsdb.enable=true
   #canal.instance.tsdb.url=jdbc:mysql://127.0.0.1:3306/canal_tsdb
   #canal.instance.tsdb.dbUsername=canal
   #canal.instance.tsdb.dbPassword=canal
   
   #canal.instance.standby.address =
   #canal.instance.standby.journal.name =
   #canal.instance.standby.position =
   #canal.instance.standby.timestamp =
   #canal.instance.standby.gtid=
   
   # username/password
   canal.instance.dbUsername=zhangfeng
   canal.instance.dbPassword=zhangfeng800729)(*Q
   canal.instance.connectionCharset = UTF-8
   # enable druid Decrypt database password
   canal.instance.enableDruid=false
   #canal.instance.pwdPublicKey=MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALK4BUxdDltRRE5/zXpVEVPUgunvscYFtEip3pmLlhrWpacX7y7GCMo2/JM6LeHmiiNdH1FWgGCpUfircSwlWKUCAwEAAQ==
   
   # table regex
   canal.instance.filter.regex=demo\\..*
   # table black regex
   canal.instance.filter.black.regex=
   # table field filter(format: schema1.tableName1:field1/field2,schema2.tableName2:field1/field2)
   #canal.instance.filter.field=test1.t_product:id/subject/keywords,test2.t_company:id/name/contact/ch
   # table field black filter(format: schema1.tableName1:field1/field2,schema2.tableName2:field1/field2)
   #canal.instance.filter.black.field=test1.t_product:subject/product_image,test2.t_company:id/name/contact/ch
   
   # mq config
   #canal.mq.topic=
   # dynamic topic route by schema or table regex
   #canal.mq.dynamicTopic=mytest1.user,mytest2\\..*,.*\\..*
   #canal.mq.partition=0
   # hash partition config
   #canal.mq.partitionsNum=3
   #canal.mq.partitionHash=test.table:id^name,.*\\..*
   #################################################
   ```

   

3. Start Canal

   ```shell
   sh bin/startup.sh
   ```

> Note: canal instance user/passwd
>
>Version 1.1.5, add these two configurations in canal.properties
>
>canal.user = canal
>canal.passwd = E3619321C1A937C46A0D8BD1DAC39F93B27D4458
>
>The default password is canal/canal, and the password value of canal.passwd can be obtained by selecting password("xxx")

4. Verify whether the startup is successful

   ```shell
   tail -200f logs/demo/demo.log
   ```

   ![image-20211110145044815](/images/binlog/image-20211110145044815.png)

## 3.Start synchronizing data

### 3.1 Create Doris target table

The user needs to first create the target table corresponding to the Mysql side on the Doris side

Binlog Load can only support unique target tables, and the Batch Delete function of the target table must be activated.

For the method of enabling Batch Delete, please refer to the batch delete function in `help alter table`.

```sql
CREATE TABLE `doris_mysql_binlog_demo` (
  `id` int NOT NULL,
  `sex` TINYINT(1),
  `name` varchar(20),
  `address` varchar(255) 
) ENGINE=OLAP
UNIQUE KEY(`id`,sex)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`sex`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 3",
"in_memory" = "false",
"storage_format" = "V2"
);

-- enable batch delete
ALTER TABLE test_2.doris_mysql_binlog_demo ENABLE FEATURE "BATCH_DELETE";
```



### 3.1 Create sync job

#### 3.1.1 Create Sync Job syntax description

The data synchronization (Sync Job) function supports the user to submit a resident data synchronization job. By reading the Binlog log from the specified remote address, incrementally synchronize the user's CDC (Change Data Capture) of the data update operation in the Mysql database Function.

Currently, the data synchronization job only supports Canal docking. The parsed Binlog data is obtained from Canal Server and imported into Doris.

Users can check the status of data synchronization job through `SHOW SYNC JOB`.

grammar:

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

	The name of the synchronization job is the unique identifier of the job in the current database. Only one job with the same `job_name` can be running.
   
2. `channel_desc`

	The data channel under the job is used to describe the mapping relationship between the mysql source table and the doris target table.
	
	grammar:  		
	
	```
	FROM mysql_db.src_tbl INTO des_tbl
	[partitions]
	[columns_mapping]
	```
	
	1. `mysql_db.src_tbl`

        Specify the database and source table on the mysql side.
		
	2. `des_tbl`

        Specify the target table on the doris side. Only unique tables are supported, and the batch delete function of the table needs to be enabled (for the opening method, please refer to the "batch delete function" of help alter table).
	
	3. `partitions`

        Specify which partitions of the destination table to import. If not specified, it will be automatically imported into the corresponding partition.
		
        Example:
	
        ```
        PARTITION(p1, p2, p3)
        ```
		
	4. `column_mapping`

        Specify the mapping relationship between the columns of the mysql source table and the doris target table. If not specified, FE will default the source table and target table columns in a one-to-one correspondence.
		
        The form of col_name = expr is not supported to represent columns.
		
        Example:
		
        ```
        Suppose the target table is listed as (k1, k2, v1),
	     
        Change the order of columns k1 and k2
        COLUMNS(k2, k1, v1)
	     
        Ignore the fourth column of the source data
        COLUMNS(k2, k1, v1, dummy_column)
        ```
	
3. `binlog_desc`

    Used to describe the remote data source, currently only supports one type of canal.

    grammar:

    ```
    FROM BINLOG
    (
        "key1" = "value1", 
        "key2" = "value2"
    )
    ```

    1. The attribute corresponding to the Canal data source, prefixed with `canal.`

      1. canal.server.ip: the address of the canal server
      2. canal.server.port: canal server port
      3. canal.destination: the identity of the instance
      4. canal.batchSize: the maximum value of the batch size obtained, the default is 8192
      5. canal.username: instance username
      6. canal.password: instance password
      7. canal.debug: Optional, when set to true, the batch and detailed information of each row of data will be printed out
          Examples:

1. Simply create a data synchronization job named `job1` for `test_tbl` of `test_db`, connect to the local Canal server, and correspond to the Mysql source table `mysql_db1.tbl1`.

		CREATE SYNC `test_db`.`job1`
		(
			FROM `mysql_db1`.`tbl1` INTO `test_tbl `
		)
		FROM BINLOG 
		(
			"type" = "canal",
			"canal.server.ip" = "127.0.0.1",
			"canal.server.port" = "11111",
			"canal.destination" = "example",
			"canal.username" = "",
			"canal.password" = ""
		);
	
2. Create a data synchronization job named `job1` for multiple tables of `test_db`, corresponding to multiple Mysql source tables one by one, and explicitly specify the column mapping.

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

#### 3.1.2 Start to synchronize data in mysql table to Doris

>Notice:
>
>Before creating a synchronization task, first configure enable_create_sync_job=true in fe.conf, this is false by default and not enabled, otherwise the synchronization task cannot be created

```
CREATE SYNC test_2.doris_mysql_binlog_demo_job 
(
	FROM demo.test_cdc INTO doris_mysql_binlog_demo
) 
FROM BINLOG 
(
	"type" = "canal", 
	"canal.server.ip" = "10.220.146.10", 
	"canal.server.port" = "11111", 
	"canal.destination" = "demo",  
	"canal.username" = "canal", 
	"canal.password" = "canal"
);
```

#### 3.1.3 View synchronization tasks

```sql
SHOW SYNC JOB from test_2;
```

![image-20211110160106602](/images/binlog/image-20211110160106602.png)

#### 3.1.4 View the data in the table

```sql
select * from doris_mysql_binlog_demo;
```

![image-20211110160331479](/images/binlog/image-20211110160331479.png)

#### 3.1.5 Delete Data

We delete the data in the Mysql data table, and then look at the changes in the Doris table

```
delete from test_cdc where id in (12,13)
```

We are looking at the Doris table, the two data with id 12 and 13 have been deleted

![image-20211110160710709](/images/binlog/image-20211110160710709.png)

#### 3.1.6 Multi-table synchronization

Multi-table synchronization only needs to be written like the following



```sql
CREATE SYNC test_2.doris_mysql_binlog_demo_job 
(
    FROM demo.test_cdc INTO doris_mysql_binlog_demo,
    FROM demo.test_cdc_1 INTO doris_mysql_binlog_demo,
    FROM demo.test_cdc_2 INTO doris_mysql_binlog_demo,
    FROM demo.test_cdc_3 INTO doris_mysql_binlog_demo
) 
```

