---
{
    "title": "Alibaba Cloud DLF",
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


# Alibaba Cloud DLF

Data Lake Formation (DLF) is the unified metadata management service of Alibaba Cloud. It is compatible with the Hive Metastore protocol.

> [What is DLF](https://www.alibabacloud.com/product/datalake-formation)

Doris can access DLF the same way as it accesses Hive Metastore.

## Connect to DLF

### The First Way, Create a Hive Catalog.

```sql
CREATE CATALOG hive_with_dlf PROPERTIES (
   "type"="hms",
   "dlf.catalog.proxyMode" = "DLF_ONLY",
   "hive.metastore.type" = "dlf",
   "dlf.catalog.endpoint" = "dlf.cn-beijing.aliyuncs.com",
   "dlf.catalog.region" = "cn-beijing",
   "dlf.catalog.uid" = "uid",
   "dlf.catalog.accessKeyId" = "ak",
   "dlf.catalog.accessKeySecret" = "sk"
);
```

`type` should always be `hms`. If you need to access Alibaba Cloud OSS on the public network, can add `"dlf.catalog.accessPublic"="true"`.

* `dlf.catalog.endpoint`: DLF Endpoint. See [Regions and Endpoints of DLF](https://www.alibabacloud.com/help/en/data-lake-formation/latest/regions-and-endpoints).
* `dlf.catalog.region`: DLF Region. See [Regions and Endpoints of DLF](https://www.alibabacloud.com/help/en/data-lake-formation/latest/regions-and-endpoints).
* `dlf.catalog.uid`: Alibaba Cloud account. You can find the "Account ID" in the upper right corner on the Alibaba Cloud console.
* `dlf.catalog.accessKeyId`：AccessKey, which you can create and manage on the [Alibaba Cloud console](https://ram.console.aliyun.com/manage/ak).
* `dlf.catalog.accessKeySecret`：SecretKey, which you can create and manage on the [Alibaba Cloud console](https://ram.console.aliyun.com/manage/ak).

Other configuration items are fixed and require no modifications.

After the above steps, you can access metadata in DLF the same way as you access Hive MetaStore.

Doris supports accessing Hive/Iceberg/Hudi metadata in DLF.

### The Second Way, Configure the Hive Conf

1. Create the `hive-site.xml` file, and put it in the `fe/conf`  directory.

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

2. Restart FE, Doris will read and parse `fe/conf/hive-site.xml`. And then Create Catalog via the `CREATE CATALOG` statement.

```sql
CREATE CATALOG hive_with_dlf PROPERTIES (
    "type"="hms",
    "hive.metastore.uris" = "thrift://127.0.0.1:9083"
)
```

`type` should always be `hms`; while `hive.metastore.uris` can be arbitary since it is not used in real practice, but it should follow the format of Hive Metastore Thrift URI.


