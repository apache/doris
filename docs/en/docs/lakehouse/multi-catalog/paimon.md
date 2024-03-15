---
{
"title": "Paimon",
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


# Paimon

<version since="dev">
</version>

## Instructions for use

1. When data in hdfs,need to put core-site.xml, hdfs-site.xml and hive-site.xml in the conf directory of FE and BE. First read the hadoop configuration file in the conf directory, and then read the related to the environment variable `HADOOP_CONF_DIR` configuration file.
2. The currently adapted version of the payment is 0.6.0

## Create Catalog

Paimon Catalog Currently supports two types of Metastore creation catalogs:
* filesystem(default),Store both metadata and data in the file system.
* hive metastore,It also stores metadata in Hive metastore. Users can access these tables directly from Hive.

### Creating a Catalog Based on FileSystem

> For versions 2.0.1 and earlier, please use the following `Create Catalog based on Hive Metastore`.

#### HDFS

```sql
CREATE CATALOG `paimon_hdfs` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "hdfs://HDFS8000871/user/paimon",
    "dfs.nameservices" = "HDFS8000871",
    "dfs.ha.namenodes.HDFS8000871" = "nn1,nn2",
    "dfs.namenode.rpc-address.HDFS8000871.nn1" = "172.21.0.1:4007",
    "dfs.namenode.rpc-address.HDFS8000871.nn2" = "172.21.0.2:4007",
    "dfs.client.failover.proxy.provider.HDFS8000871" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "hadoop.username" = "hadoop"
);

CREATE CATALOG `paimon_kerberos` PROPERTIES (
    'type'='paimon',
    "warehouse" = "hdfs://HDFS8000871/user/paimon",
    "dfs.nameservices" = "HDFS8000871",
    "dfs.ha.namenodes.HDFS8000871" = "nn1,nn2",
    "dfs.namenode.rpc-address.HDFS8000871.nn1" = "172.21.0.1:4007",
    "dfs.namenode.rpc-address.HDFS8000871.nn2" = "172.21.0.2:4007",
    "dfs.client.failover.proxy.provider.HDFS8000871" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    'hadoop.security.authentication' = 'kerberos',
    'hadoop.kerberos.keytab' = '/doris/hdfs.keytab',   
    'hadoop.kerberos.principal' = 'hdfs@HADOOP.COM'
);
```

#### MINIO

```sql
CREATE CATALOG `paimon_s3` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "s3://bucket_name/paimons3",
    "s3.endpoint" = "http://<ip>:<port>",
    "s3.access_key" = "ak",
    "s3.secret_key" = "sk"
);
```

#### OBS

```sql
CREATE CATALOG `paimon_obs` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "obs://bucket_name/paimon",
    "obs.endpoint"="obs.cn-north-4.myhuaweicloud.com",
    "obs.access_key"="ak",
    "obs.secret_key"="sk"
);
```

#### COS

```sql
CREATE CATALOG `paimon_cos` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "cosn://paimon-1308700295/paimoncos",
    "cos.endpoint" = "cos.ap-beijing.myqcloud.com",
    "cos.access_key" = "ak",
    "cos.secret_key" = "sk"
);
```

#### OSS

```sql
CREATE CATALOG `paimon_oss` PROPERTIES (
    "type" = "paimon",
    "warehouse" = "oss://paimon-zd/paimonoss",
    "oss.endpoint" = "oss-cn-beijing.aliyuncs.com",
    "oss.access_key" = "ak",
    "oss.secret_key" = "sk"
);
```

### Creating a Catalog Based on Hive Metastore

```sql
CREATE CATALOG `paimon_hms` PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "hms",
    "warehouse" = "hdfs://HDFS8000871/user/zhangdong/paimon2",
    "hive.metastore.uris" = "thrift://172.21.0.44:7004",
    "dfs.nameservices" = "HDFS8000871",
    "dfs.ha.namenodes.HDFS8000871" = "nn1,nn2",
    "dfs.namenode.rpc-address.HDFS8000871.nn1" = "172.21.0.1:4007",
    "dfs.namenode.rpc-address.HDFS8000871.nn2" = "172.21.0.2:4007",
    "dfs.client.failover.proxy.provider.HDFS8000871" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "hadoop.username" = "hadoop"
);

CREATE CATALOG `paimon_kerberos` PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "hms",
    "warehouse" = "hdfs://HDFS8000871/user/zhangdong/paimon2",
    "hive.metastore.uris" = "thrift://172.21.0.44:7004",
    "hive.metastore.sasl.enabled" = "true",
    "hive.metastore.kerberos.principal" = "hive/xxx@HADOOP.COM",
    "dfs.nameservices" = "HDFS8000871",
    "dfs.ha.namenodes.HDFS8000871" = "nn1,nn2",
    "dfs.namenode.rpc-address.HDFS8000871.nn1" = "172.21.0.1:4007",
    "dfs.namenode.rpc-address.HDFS8000871.nn2" = "172.21.0.2:4007",
    "dfs.client.failover.proxy.provider.HDFS8000871" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
    "hadoop.security.authentication" = "kerberos",
    "hadoop.kerberos.principal" = "hdfs@HADOOP.COM",
    "hadoop.kerberos.keytab" = "/doris/hdfs.keytab"
);
```

## Column Type Mapping

| Paimon Data Type                      | Doris Data Type           | Comment   |
|---------------------------------------|---------------------------|-----------|
| BooleanType                           | Boolean                   |           |
| TinyIntType                           | TinyInt                   |           |
| SmallIntType                          | SmallInt                  |           |
| IntType                               | Int                       |           |
| FloatType                             | Float                     |           |
| BigIntType                            | BigInt                    |           |
| DoubleType                            | Double                    |           |
| VarCharType                           | VarChar                   |           |
| CharType                              | Char                      |           |
| DecimalType(precision, scale)         | Decimal(precision, scale) |           |
| TimestampType,LocalZonedTimestampType | DateTime                  |           |
| DateType                              | Date                      |           |
| MapType                               | Map                       | Support Map nesting   |
| ArrayType                             | Array                     | Support Array nesting |
| VarBinaryType, BinaryType             | Binary                    |           |

## FAQ

1. Kerberos

    - Make sure principal and keytab are correct.
    - You need to start a scheduled task (such as crontab) on the BE node, and execute the `kinit -kt your_principal your_keytab` command every certain time (such as 12 hours).

2. Unknown type value: UNSUPPORTED

    This is a compatible issue exist in 2.0.2 with Paimon 0.5, you need to upgrade to 2.0.3 or higher to solve this problem. Or [patch](https://github.com/apache/doris/pull/24985) yourself.

3. When accessing object storage (OSS, S3, etc.), encounter "file system does not support".

     In versions before 2.0.5 (inclusive), users need to manually download the following jar package and place it in the `${DORIS_HOME}/be/lib/java_extensions/preload-extensions` directory, and restart BE.

    - OSS: [paimon-oss-0.6.0-incubating.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/0.6.0-incubating/paimon-oss-0.6.0-incubating.jar)
    - Other Object Storage: [paimon-s3-0.6.0-incubating.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/0.6.0-incubating/paimon-s3-0.6.0-incubating.jar)

