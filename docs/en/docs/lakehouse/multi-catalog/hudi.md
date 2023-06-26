---
{
    "title": "Hudi",
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


# Hudi

## Usage

1. Doris supports Snapshot Query on Copy-on-Write Hudi tables and Read Optimized Query / Snapshot on Merge-on-Read tables. In the future, it will support Incremental Query and Time Travel.

|  Table Type   | Supported Query types  |
|  ----  | ----  |
| Copy On Write  | Snapshot Query |
| Merge On Read  | Snapshot Queries + Read Optimized Queries |

2. Doris supports Hive Metastore(Including catalogs compatible with Hive MetaStore, like [AWS Glue](./hive.md)/[Alibaba DLF](./dlf.md)) Catalogs.

## Create Catalog

Same as creating Hive Catalogs. A simple example is provided here. See [Hive](./hive.md) for more information.

```sql
CREATE CATALOG hudi PROPERTIES (
    'type'='hms',
    'hive.metastore.uris' = 'thrift://172.21.0.1:7004',
    'hadoop.username' = 'hive',
    'dfs.nameservices'='your-nameservice',
    'dfs.ha.namenodes.your-nameservice'='nn1,nn2',
    'dfs.namenode.rpc-address.your-nameservice.nn1'='172.21.0.2:4007',
    'dfs.namenode.rpc-address.your-nameservice.nn2'='172.21.0.3:4007',
    'dfs.client.failover.proxy.provider.your-nameservice'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);
```

## Column Type Mapping

Same as that in Hive Catalogs. See the relevant section in [Hive](./hive.md).

## Query Optimization
Doris uses the parquet native reader to read the data files of the COW table, and uses the Java SDK (By calling hudi-bundle through JNI) to read the data files of the MOR table. In `upsert` scenario, there may still remains base files that have not been updated in the MOR table, which can be read through the parquet native reader. Users can view the execution plan of hudi scan through the [explain](../../advanced/best-practice/query-analysis.md) command, where `hudiNativeReadSplits` indicates how many split files are read through the parquet native reader.
```
|0:VHUDI_SCAN_NODE                                                             |
|      table: minbatch_mor_rt                                                  |
|      predicates: `o_orderkey` = 100030752                                    |
|      inputSplitNum=810, totalFileSize=5645053056, scanRanges=810             |
|      partition=80/80                                                         |
|      numNodes=6                                                              |
|      hudiNativeReadSplits=717/810                                            |
```
Users can view the perfomace of Java SDK through [profile](../../admin-manual/http-actions/fe/profile-action.md), for exmpale:
```
-  HudiJniScanner:  0ns
  -  FillBlockTime:  31.29ms
  -  GetRecordReaderTime:  1m5s
  -  JavaScanTime:  35s991ms
  -  OpenScannerTime:  1m6s
```
1. `OpenScannerTime`: Time to create and initialize JNI reader
2. `JavaScanTime`: Time to read data by Java SDK
3. `FillBlockTime`: Time co convert Java column data into C++ column data
4. `GetRecordReaderTime`: Time to create and initialize Hudi Record Reader
