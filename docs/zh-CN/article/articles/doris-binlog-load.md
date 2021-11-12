---
{
    "title": "Apache Doris Binlog Load使用方法及示例",
    "description": "Binlog Load提供了一种使Doris增量同步用户在Mysql数据库的对数据更新操作的CDC(Change Data Capture)功能，使用户更方便的完成Mysql数据的导入",
    "date": "2021-11-10",
    "metaTitle": "Apache Doris Binlog Load使用方法及示例",
    "language": "zh-CN",
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

Binlog Load提供了一种使Doris增量同步用户在Mysql数据库的对数据更新操作的CDC(Change Data Capture)功能，使用户更方便的完成Mysql数据的导入

>注意：
>
>该功能需要在0.15及以后的版本里使用

## 1. 安装配置 Mysql 

1. 安装Mysql

   快速使用Docker安装配置Mysql，具体参照下面的连接

   https://segmentfault.com/a/1190000021523570

   如果是在物理机上安装可以参考下面的连接：

   [在 CentOS 7 中安装 MySQL 8 的教程详解](https://cloud.tencent.com/developer/article/1721575)

2. 开启Mysql binlog

   进入 Docker 容器或者物理机上修改/etc/my.cnf 文件，在 [mysqld] 下面添加以下内容，

   ```
   log_bin=mysql_bin
   binlog-format=Row
   server-id=1
   ```

   然后重启Mysql

   ```
   systemctl restart mysqld
   ```

3. 创建 Mysql 表

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


## 2. 安装配置Canal

下载canal-1.1.5: https://github.com/alibaba/canal/releases/download/canal-1.1.5/canal.deployer-1.1.5.tar.gz

1. 解压Canal到指定目录：

   ```shell
   tar zxvf canal.deployer-1.1.5.tar.gz -C ./canal
   ```

2. 在conf文件夹下新建目录并重命名，作为instance的根目录，目录名你可以自己命名便于识别即可

   例如我这里的命名是和我的数据库库名一致：demo

   ```shell
   vi conf/demo/instance.properties
   ```

   下面给出的是一个我的示例配置:

   这里面的参数说明请参考Canal官方文档：[QuickStart ](https://github.com/alibaba/canal/wiki/QuickStart)

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

   

3. 启动Canal

     ```shell
   sh bin/startup.sh
   ```

> 注意：canal instance user/passwd
>
>1.1.5 版本，在canal.properties里加上这两个配置
>
>canal.user = canal
>canal.passwd = E3619321C1A937C46A0D8BD1DAC39F93B27D4458
>
>默认密码为canal/canal，canal.passwd的密码值可以通过select password("xxx") 来获取 

4. 验证是否启动成功

   ```shell
   tail -200f logs/demo/demo.log
   ```

   ![image-20211110145044815](/images/binlog/image-20211110145044815.png)

## 3.开始同步数据

### 3.1 创建Doris目标表

用户需要先在Doris端创建好与Mysql端对应的目标表

Binlog Load只能支持Unique类型的目标表，且必须激活目标表的Batch Delete功能。

开启Batch Delete的方法可以参考`help alter table`中的批量删除功能。

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



### 3.1 创建同步作业

#### 3.1.1 Create Sync Job 语法说明

Name: 'CREATE SYNC JOB'
Description:

数据同步(Sync Job)功能，支持用户提交一个常驻的数据同步作业，通过从指定的远端地址读取Binlog日志，增量同步用户在Mysql数据库的对数据更新操作的CDC(Change Data Capture)功能。
	
目前数据同步作业只支持对接Canal，从Canal Server上获取解析好的Binlog数据，导入到Doris内。
	
用户可通过 `SHOW SYNC JOB` 查看数据同步作业状态。
	
语法：

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

	同步作业名称，是作业在当前数据库内的唯一标识，相同`job_name`的作业只能有一个在运行。
   
2. `channel_desc`

	作业下的数据通道，用来描述mysql源表到doris目标表的映射关系。
	
	语法：   		
	
	```
	FROM mysql_db.src_tbl INTO des_tbl
	[partitions]
	[columns_mapping]
	```
	
	1. `mysql_db.src_tbl`

        指定mysql端的数据库和源表。
		
	2. `des_tbl`

        指定doris端的目标表，只支持Unique表，且需开启表的batch delete功能(开启方法请看help alter table的'批量删除功能')。
	
	3. `partitions`

        指定导入目的表的哪些 partition 中。如果不指定，则会自动导入到对应的 partition 中。
		
        示例：
	
        ```
        PARTITION(p1, p2, p3)
        ```
		
	4. `column_mapping`

        指定mysql源表和doris目标表的列之间的映射关系。如果不指定，FE会默认源表和目标表的列按顺序一一对应。
		
        不支持 col_name = expr 的形式表示列。
		
        示例：
		
        ```
        假设目标表列为(k1, k2, v1)，
	     
        改变列k1和k2的顺序
        COLUMNS(k2, k1, v1)
	     
        忽略源数据的第四列
        COLUMNS(k2, k1, v1, dummy_column)
        ```
	
3. `binlog_desc`

    用来描述远端数据源，目前仅支持canal一种。
	
    语法：
	
    ```
    FROM BINLOG
    (
        "key1" = "value1", 
        "key2" = "value2"
    )
    ```
	
	1. Canal 数据源对应的属性，以`canal.`为前缀

		1. canal.server.ip: canal server的地址
		2. canal.server.port: canal server的端口
		3. canal.destination: instance的标识
		4. canal.batchSize: 获取的batch大小的最大值，默认8192
		5. canal.username: instance的用户名
		6. canal.password: instance的密码
		7. canal.debug: 可选，设置为true时，会将batch和每一行数据的详细信息都打印出来
Examples:

1. 简单为 `test_db` 的 `test_tbl` 创建一个名为 `job1` 的数据同步作业，连接本地的Canal服务器，对应Mysql源表 `mysql_db1.tbl1`。

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
	
2. 为 `test_db` 的多张表创建一个名为 `job1` 的数据同步作业，一一对应多张Mysql源表，并显式的指定列映射。

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

#### 3.1.2 开始同步mysql表里数据到Doris

>注意：
>
>创建同步任务之前，首先要在fe.conf里配置enable_create_sync_job=true，这个默认是false不启用，否则就不能创建同步任务

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

#### 3.1.3 查看同步任务

```sql
SHOW SYNC JOB from test_2;
```

![image-20211110160106602](/images/binlog/image-20211110160106602.png)

#### 3.1.4 查看表里的数据

```sql
select * from doris_mysql_binlog_demo;
```

![image-20211110160331479](/images/binlog/image-20211110160331479.png)

#### 3.1.5 删除数据

我们在Mysql 数据表里删除数据，然后看Doris表里的变化

```
delete from test_cdc where id in (12,13)
```

我们在去看Doris表里，id是12,13这两条数据已经被删除

![image-20211110160710709](/images/binlog/image-20211110160710709.png)

#### 3.1.6 多表同步

多表同步只需要像下面这样写法就可以了



```sql
CREATE SYNC test_2.doris_mysql_binlog_demo_job 
(
    FROM demo.test_cdc INTO doris_mysql_binlog_demo,
    FROM demo.test_cdc_1 INTO doris_mysql_binlog_demo,
    FROM demo.test_cdc_2 INTO doris_mysql_binlog_demo,
    FROM demo.test_cdc_3 INTO doris_mysql_binlog_demo
) 
```

