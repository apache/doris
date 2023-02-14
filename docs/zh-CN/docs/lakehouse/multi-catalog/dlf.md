---
{
    "title": "阿里云 DLF",
    "language": "zh-CN"
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


# 阿里云 DLF

阿里云 Data Lake Formation(DLF) 是阿里云上的统一元数据管理服务。兼容 Hive Metastore 协议。

> [什么是 Data Lake Formation](https://www.aliyun.com/product/bigdata/dlf)

因此我们也可以和访问 Hive Metastore 一样，连接并访问 DLF。

## 连接 DLF

### 方式一：创建Hive Catalog连接DLF

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

其中 `type` 固定为 `hms`。 如果需要公网访问阿里云对象存储的数据，可以设置 `"dlf.catalog.accessPublic"="true"`

* `dlf.catalog.endpoint`：DLF Endpoint，参阅：[DLF Region和Endpoint对照表](https://www.alibabacloud.com/help/zh/data-lake-formation/latest/regions-and-endpoints)
* `dlf.catalog.region`：DLF Region，参阅：[DLF Region和Endpoint对照表](https://www.alibabacloud.com/help/zh/data-lake-formation/latest/regions-and-endpoints)
* `dlf.catalog.uid`：阿里云账号。即阿里云控制台右上角个人信息的“云账号ID”。
* `dlf.catalog.accessKeyId`：AccessKey。可以在 [阿里云控制台](https://ram.console.aliyun.com/manage/ak) 中创建和管理。
* `dlf.catalog.accessKeySecret`：SecretKey。可以在 [阿里云控制台](https://ram.console.aliyun.com/manage/ak) 中创建和管理。

其他配置项为固定值，无需改动。

之后，可以像正常的 Hive MetaStore 一样，访问 DLF 下的元数据。

同 Hive Catalog 一样，支持访问 DLF 中的 Hive/Iceberg/Hudi 的元数据信息。

### 方式二：配置Hive Conf连接DLF

1. 创建 hive-site.xml 文件，并将其放置在 `fe/conf` 目录下。

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

2. 重启 FE，Doris 会读取和解析 fe/conf/hive-site.xml。 并通过 `CREATE CATALOG` 语句创建 catalog。

```sql
CREATE CATALOG hive_with_dlf PROPERTIES (
   "type"="hms",
    "hive.metastore.uris" = "thrift://127.0.0.1:9083"
)
```

其中 `type` 固定为 `hms`。`hive.metastore.uris` 的值随意填写即可，实际不会使用。但需要按照标准 hive metastore thrift uri 格式填写。
   


