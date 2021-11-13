---
{
    "title": "How to use Apache doris Datax DorisWriter extension",
    "description": "In order to better expand the Apache doris ecosystem and provide more convenient data import for doris users, the community development and extension supports Datax DorisWriter, making it more convenient for Datax to access data.",
    "date": "2021-11-11",
    "metaTitle": "How to use Apache doris Datax DorisWriter extension",
    "language": "en",
    "author": "张家锋",
    "isArticle": true,
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

DataX is an open source version of Alibaba Cloud DataWorks data integration, an offline data synchronization tool/platform widely used in Alibaba Group. DataX implements efficient data synchronization functions between various heterogeneous data sources including MySQL, Oracle, SqlServer, Postgre, HDFS, Hive, ADS, HBase, TableStore (OTS), MaxCompute (ODPS), Hologres, DRDS, etc.

In order to better expand the Apache doris ecosystem and provide more convenient data import for doris users, the community development and extension supports Datax DorisWriter, making it more convenient for Datax to access data..

## 1.Scenes

Here is a demonstration of using Doris' Datax extension DorisWriter to extract data from Mysql data and import it into the Doris data warehouse table.

## 2.Build DorisWriter

The compilation of this extension can not be done in the docker compilation environment of doris, this article is compiled under the WLS under windows

First pull the source code from github

```
git clone https://github.com/apache/incubator-doris.git
```

Enter `incubator-doris/extension/DataX/` to execute compilation

First execute:

```shell
sh init_env.sh
```

This script is mainly used to build the DataX development environment. It mainly performs the following operations:

1. Clone the DataX code base to the local.

2. Softlink the `doriswriter/` directory to the `DataX/doriswriter` directory.

3. Add the `<module>doriswriter</module>` module in the `DataX/pom.xml` file.

4. Change the httpclient version in the `DataX/core/pom.xml` file from 4.5 to 4.5.13.

    > httpclient v4.5 has a bug in handling 307 forwarding.

After the script is executed, the developer can enter the `DataX/` directory to start development or compilation. Because of the soft link, any modification to the files in the `DataX/doriswriter` directory will be reflected in the `doriswriter/` directory, which is convenient for developers to submit code

### 2.1 Build

Here I removed a lot of useless plug-ins in order to speed up the compilation: here, just comment out the pom.xml in the Datax directory.

```
hbase11xreader
hbase094xreader
tsdbreader
oceanbasev10reader
odpswriter
hdfswriter
adswriter
ocswriter
oscarwriter
oceanbasev10writer
```

Then enter the Datax directory under the `incubator-doris/extension/DataX/` directory and execute the compilation

Here I am performing the compilation of Datax into a tar package, which is not the same as the official compilation command.

```
mvn -U clean package assembly:assembly -Dmaven.test.skip=true
```

<img src="/images/image-20210903132250723.png" alt="image-20210903132250723" style="zoom:50%;" />

![image-20210903132539511](/images/image-20210903132539511.png)

After the compilation is complete, the tar package is in the `Datax/target` directory. You can copy the tar package to the place you need. Here I am directly performing the test in datax. Because the python version is 3.x version, you need to The three files in the bin directory are replaced with other versions of python 3. You can download this from the following address:

```
https://github.com/WeiYe-Jing/datax-web/tree/master/doc/datax-web/datax-python3
```

After replacing the downloaded three files with the files in the bin directory, the entire compilation is complete, and the installation is complete

## 3.Data access

At this time we can start to use Datax's doriswriter extension to directly extract data from Mysql (or other data sources) and import it into the Doris table.

### 3.1 Mysql database preparation

The following is the database table creation script (mysql 8):

```sql
CREATE TABLE `order_analysis` (
  `date` varchar(19) DEFAULT NULL,
  `user_src` varchar(9) DEFAULT NULL,
  `order_src` varchar(11) DEFAULT NULL,
  `order_location` varchar(2) DEFAULT NULL,
  `new_order` int DEFAULT NULL,
  `payed_order` int DEFAULT NULL,
  `pending_order` int DEFAULT NULL,
  `cancel_order` int DEFAULT NULL,
  `reject_order` int DEFAULT NULL,
  `good_order` int DEFAULT NULL,
  `report_order` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPACT
```

Sample data

```sql
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-12 00:00:00', '广告二维码', 'Android APP', '上海', 15253, 13210, 684, 1247, 1000, 10824, 862);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-14 00:00:00', '微信朋友圈H5页面', 'iOS APP', '广州', 17134, 11270, 549, 204, 224, 10234, 773);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-17 00:00:00', '地推二维码扫描', 'iOS APP', '北京', 16061, 9418, 1220, 1247, 458, 13877, 749);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-17 00:00:00', '微信朋友圈H5页面', '微信公众号', '武汉', 12749, 11127, 1773, 6, 5, 9874, 678);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-18 00:00:00', '地推二维码扫描', 'iOS APP', '上海', 13086, 15882, 1727, 1764, 1429, 12501, 625);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-18 00:00:00', '微信朋友圈H5页面', 'iOS APP', '武汉', 15129, 15598, 1204, 1295, 1831, 11500, 320);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-19 00:00:00', '地推二维码扫描', 'Android APP', '杭州', 20687, 18526, 1398, 550, 213, 12911, 185);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-19 00:00:00', '应用商店', '微信公众号', '武汉', 12388, 11422, 702, 106, 158, 5820, 474);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-20 00:00:00', '微信朋友圈H5页面', '微信公众号', '上海', 14298, 11682, 1880, 582, 154, 7348, 354);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-21 00:00:00', '地推二维码扫描', 'Android APP', '深圳', 22079, 14333, 5565, 1742, 439, 8246, 211);
INSERT INTO `sql12298540`.`order_analysis` (`date`, `user_src`, `order_src`, `order_location`, `new_order`, `payed_order`, `pending_order`, `cancel_order`, `reject_order`, `good_order`, `report_order`) VALUES ('2015-10-22 00:00:00', 'UC浏览器引流', 'iOS APP', '上海', 28968, 18151, 7212, 2373, 1232, 10739, 578);

```

### 3.2 doris database preparation

The following is the table creation script corresponding to the above data table in doris

```sql
CREATE TABLE `order_analysis` (
  `date` datetime DEFAULT NULL,
  `user_src` varchar(30) DEFAULT NULL,
  `order_src` varchar(50) DEFAULT NULL,
  `order_location` varchar(10) DEFAULT NULL,
  `new_order` int DEFAULT NULL,
  `payed_order` int DEFAULT NULL,
  `pending_order` int DEFAULT NULL,
  `cancel_order` int DEFAULT NULL,
  `reject_order` int DEFAULT NULL,
  `good_order` int DEFAULT NULL,
  `report_order` int DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`date`,user_src)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`user_src`) BUCKETS 1
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"storage_format" = "V2"
);
```

### 3.3 Datax Job JSON file

Create and edit the datax job task json file and save it to the specified directory

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 1
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "root",
                        "password": "zhangfeng",
                        "column": ["date","user_src","order_src","order_location","new_order","payed_order"," pending_order"," cancel_order"," reject_order"," good_order"," report_order" ],
                        "connection": [ { "table": [ "order_analysis" ], "jdbcUrl": [ "jdbc:mysql://localhost:3306/demo" ] } ] }
                },
                "writer": {
                    "name": "doriswriter",
                    "parameter": {
                        "feLoadUrl": ["fe:8030"],
                        "beLoadUrl": ["be1:8040","be1:8040","be1:8040","be1:8040","be1:8040","be1:8040"],
                        "jdbcUrl": "jdbc:mysql://fe:9030/",
                        "database": "test_2",
                        "table": "order_analysis",
                        "column": ["date","user_src","order_src","order_location","new_order","payed_order"," pending_order"," cancel_order"," reject_order"," good_order"," report_order"],
                        "username": "root",
                        "password": "",
                        "postSql": [],
                        "preSql": [],
                        "loadProps": {
                        },
                        "maxBatchRows" : 10000,
                        "maxBatchByteSize" : 104857600,
                        "labelPrefix": "datax_doris_writer_demo_",
                        "lineDelimiter": "\n"
                    }
                }
            }
        ]
    }
}
```

Refer to the usage of this piece of Mysql reader:

```
https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md
```

The use and parameter description of doriswriter:

```
https://github.com/apache/incubator-doris/blob/master/extension/DataX/doriswriter/doc/doriswriter.md
```

## 4.Perform Datax data import tasks

```python
python bin/datax.py doris.json
```

Then you can see the execution result:

<img src="/images/image-20210903134043421.png" alt="image-20210903134043421" style="zoom:30%;" />

Then go to the Doris database to check your table, the data has been imported, and the task execution is over

Because Datax tasks are executed by external triggers, here you can use Linux crontab or Apache DolphinScheduler to control task operation
