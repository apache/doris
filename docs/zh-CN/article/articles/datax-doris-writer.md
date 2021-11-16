---
{
    "title": "Apache Doris Datax DorisWriter扩展使用方法",
    "description": "为了更好的扩展Apache doris生态，为doris用户提供更方便的数据导入，社区开发扩展支持了Datax DorisWriter，使大家更方便Datax进行数据接入.",
    "date": "2021-11-11",
    "metaTitle": "Apache Doris Datax DorisWriter扩展使用方法",
    "isArticle": true,
    "language": "zh-CN",
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

DataX 是阿里云 DataWorks数据集成 的开源版本，在阿里巴巴集团内被广泛使用的离线数据同步工具/平台。DataX 实现了包括 MySQL、Oracle、SqlServer、Postgre、HDFS、Hive、ADS、HBase、TableStore(OTS)、MaxCompute(ODPS)、Hologres、DRDS 等各种异构数据源之间高效的数据同步功能

为了更好的扩展Apache doris生态，为doris用户提供更方便的数据导入，社区开发扩展支持了Datax DorisWriter，使大家更方便Datax进行数据接入

## 1.场景

这里演示介绍的使用 Doris 的 Datax 扩展 DorisWriter实现从Mysql数据定时抽取数据导入到Doris数仓表里

## 2.编译 DorisWriter

这个的扩展的编译可以不在 doris 的 docker 编译环境下进行，本文是在 windows 下的 WLS 下进行编译的

首先从github上拉取源码

```
git clone https://github.com/apache/incubator-doris.git
```

进入到`incubator-doris/extension/DataX/` 执行编译

首先执行：

```shell
sh init_env.sh
```

这个脚本主要用于构建 DataX 开发环境，他主要进行了以下操作：

1. 将 DataX 代码库 clone 到本地。

2. 将 `doriswriter/` 目录软链到 `DataX/doriswriter` 目录。

3. 在 `DataX/pom.xml` 文件中添加 `<module>doriswriter</module>` 模块。

4. 将 `DataX/core/pom.xml` 文件中的 httpclient 版本从 4.5 改为 4.5.13.

   > httpclient v4.5 在处理 307 转发时有bug。

这个脚本执行后，开发者就可以进入 `DataX/` 目录开始开发或编译了。因为做了软链，所以任何对 `DataX/doriswriter` 目录中文件的修改，都会反映到 `doriswriter/` 目录中，方便开发者提交代码

### 2.1 开始编译

这里为了加快编译速度你可以去掉一些没有用到的插件，例如下面这些：

这里直接在Datax目录下的pom.xml里注释掉就行

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

然后进入到`incubator-doris/extension/DataX/` 目录下的 Datax 目录，执行编译

这里我是执行的将 Datax 编译成 tar 包，和官方的编译命令不太一样。

```
mvn -U clean package assembly:assembly -Dmaven.test.skip=true
```

<img src="/images/image-20210903132250723.png" alt="image-20211105134818474" style="zoom:50%;" />

![image-20210903132539511](/images/image-20210903132539511.png)

编译完成以后，tar 包在 `Datax/target` 目录下，你可以将这tar包拷贝到你需要的地方，这里我是直接在 datax 执行测试，这里因为的 python 版本是 3.x版本，需要将 bin 目录下的三个文件换成 python 3 版本的，这个你可以去下面的地址下载：

```
https://github.com/WeiYe-Jing/datax-web/tree/master/doc/datax-web/datax-python3
```

将下载的三个文件替换 bin 目录下的文件以后，整个编译，安装就完成了

## 3.数据接入

这个时候我们就可以开始使用 Datax  的`doriswriter`扩展开始从Mysql（或者其他数据源）直接将数据抽取出来导入到 Doris 表中了。

下面以 Mysql 数据库为例演示怎么使用 `Datax DorisWriter` 来完成 Mysql 数据库数据的抽取

### 3.1 Mysql 数据库准备

下面是数据库的建表脚本（mysql 8）：

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

示例数据

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

### 3.2 doris数据库准备

下面是我上面数据表在doris对应的建表脚本

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

### 3.3 Datax Job JSON文件

创建并编辑datax job任务json文件，并保存到指定目录

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

这块 Mysql reader 使用方式参照：

```
https://github.com/alibaba/DataX/blob/master/mysqlreader/doc/mysqlreader.md
```

doriswriter的使用及参数说明：

```
https://github.com/apache/incubator-doris/blob/master/extension/DataX/doriswriter/doc/doriswriter.md
```

## 4.执行Datax数据导入任务

```python
python bin/datax.py doris.json
```

然后就可以看到执行结果：

<img src="/images/image-20210903134043421.png" alt="image-20210903134043421" style="zoom:30%;" />

再去 Doris 数据库中查看你的表，数据就已经导入进去了，任务执行结束

因为 Datax 的任务是要靠外部触发才能执行，这里你可以使用Linux的crontab或者海豚调度之类的来控制任务运行
