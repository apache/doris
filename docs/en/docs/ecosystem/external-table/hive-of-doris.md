---
{
    "title": "Doris On Hive",
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

# Hive External Table of Doris

Hive External Table of Doris provides Doris with direct access to Hive external tables, which eliminates the need for cumbersome data import and solves the problem of analyzing Hive tables with the help of Doris' OLAP capabilities: 

 1. support for Hive data sources to access Doris
 2. Support joint queries between Doris and Hive data sources to perform more complex analysis operations
 3. Support access to kerberos-enabled Hive data sources

This document introduces how to use this feature and the considerations.

## Glossary

### Noun in Doris

* FE: Frontend, the front-end node of Doris, responsible for metadata management and request access.
* BE: Backend, the backend node of Doris, responsible for query execution and data storage

## How To Use

### Create Hive External Table 

```sql
-- Syntax
CREATE [EXTERNAL] TABLE table_name (
  col_name col_type [NULL | NOT NULL] [COMMENT "comment"]
) ENGINE=HIVE
[COMMENT "comment"] )
PROPERTIES (
  'property_name'='property_value',
  ...
);

-- Example 1: Create the hive_table table under hive_db in a Hive cluster
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table'
);

-- Example 2: Create the hive_table table under hive_db in a Hive cluster with HDFS HA configuration.
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table',
'dfs.nameservices'='hacluster',
'dfs.ha.namenodes.hacluster'='n1,n2',
'dfs.namenode.rpc-address.hacluster.n1'='192.168.0.1:8020',
'dfs.namenode.rpc-address.hacluster.n2'='192.168.0.2:8020',
'dfs.client.failover.proxy.provider.hacluster'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'
);

-- Example 3: Create the hive external table under hive_db in Hive cluster with HDFS HA and enable kerberos authentication. 
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table',
'dfs.nameservices'='hacluster',
'dfs.ha.namenodes.hacluster'='n1,n2',
'dfs.namenode.rpc-address.hacluster.n1'='192.168.0.1:8020',
'dfs.namenode.rpc-address.hacluster.n2'='192.168.0.2:8020',
'dfs.client.failover.proxy.provider.hacluster'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
'hadoop.security.authentication'='kerberos',
'dfs.namenode.kerberos.principal'='hadoop/_HOST@REALM.COM'
'hadoop.kerberos.principal'='doris_test@REALM.COM',
'hadoop.kerberos.keytab'='/path/to/doris_test.keytab'
);

-- Example 4: Create the hive_table table under hive_db in a Hive cluster with data stored on S3
CREATE TABLE `t_hive` (
  `k1` int NOT NULL COMMENT "",
  `k2` char(10) NOT NULL COMMENT "",
  `k3` datetime NOT NULL COMMENT "",
  `k5` varchar(20) NOT NULL COMMENT "",
  `k6` double NOT NULL COMMENT ""
) ENGINE=HIVE
COMMENT "HIVE"
PROPERTIES (
'hive.metastore.uris' = 'thrift://192.168.0.1:9083',
'database' = 'hive_db',
'table' = 'hive_table',
'AWS_ACCESS_KEY'='your_access_key',
'AWS_SECRET_KEY'='your_secret_key',
'AWS_ENDPOINT'='s3.us-east-1.amazonaws.com',
'AWS_REGION'='us-east-1'
);

```

#### Parameter Description

- External Table Columns
    - Column names should correspond to the Hive table
    - The order of the columns should be the same as the Hive table
    - Must contain all the columns in the Hive table
    - Hive table partition columns do not need to be specified, they can be defined as normal columns.
- ENGINE should be specified as HIVE
- PROPERTIES attribute.
    - `hive.metastore.uris`: Hive Metastore service address
    - `database`: the name of the database to which Hive is mounted
    - `table`: the name of the table to which Hive is mounted
    - `hadoop.username`: the username to visit HDFS (need to specify it when the authentication type is simple)
    - `dfs.nameservices`: the logical name for this new nameservice. See hdfs-site.xml
    - `dfs.ha.namenodes.[nameservice ID]`：unique identifiers for each NameNode in the nameservice. See hdfs-site.xml
    - `dfs.namenode.rpc-address.[nameservice ID].[name node ID]`：the fully-qualified RPC address for each NameNode to listen on. See hdfs-site.xml
    - `dfs.client.failover.proxy.provider.[nameservice ID]`：the Java class that HDFS clients use to contact the Active NameNode, usually it is org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
- For a kerberos enabled Hive datasource, additional properties need to be set:
    - `dfs.namenode.kerberos.principal`: HDFS namenode service principal
    - `hadoop.security.authentication`: HDFS authentication type please set kerberos, default simple
    - `hadoop.kerberos.principal`: The Kerberos pincipal that Doris will use when connectiong to HDFS.
    - `hadoop.kerberos.keytab`: HDFS client keytab location.
    - `AWS_ACCESS_KEY`: AWS access key id.
    - `AWS_SECRET_KEY`: AWS secret access key.
    - `AWS_ENDPOINT`: S3 endpoint. e.g. s3.us-east-1.amazonaws.com
    - `AWS_REGION`: AWS region. e.g. us-east-1

**Note:**
- To enable Doris to access the hadoop cluster with kerberos authentication enabled, you need to deploy the Kerberos client kinit on the Doris all FE and BE nodes, configure krb5.conf, and fill in the KDC service information.
- The value of the PROPERTIES property `hadoop.kerberos.keytab` needs to specify the absolute path of the keytab local file and allow the Doris process to access it.
- The configuration of the HDFS cluster can be written into the hdfs-site.xml file. The configuration file is in the conf directory of fe and be. When users create a Hive table, they do not need to fill in the relevant information of the HDFS cluster configuration.


    
## Data Type Matching

The supported Hive column types correspond to Doris in the following table.

|  Hive  | Doris  |             Description              |
| :------: | :----: | :-------------------------------: |
|   BOOLEAN  | BOOLEAN  |                         |
|   CHAR   |  CHAR  |    Only UTF8 encoding is supported      |
|   VARCHAR | VARCHAR |  Only UTF8 encoding is supported     |
|   TINYINT   | TINYINT |  |
|   SMALLINT  | SMALLINT |  |
|   INT  | INT |  |
|   BIGINT  | BIGINT |  |
|   FLOAT   |  FLOAT  |                                   |
|   DOUBLE  | DOUBLE |  |
|   DECIMAL  | DECIMAL |  |
|   DATE   |  DATE  |                                   |
|   TIMESTAMP  | DATETIME | Timestamp to Datetime will lose precision |

**Note:** 
- Hive table Schema changes **are not automatically synchronized** and require rebuilding the Hive external table in Doris.
- The current Hive storage format only supports Text, Parquet and ORC types
- The Hive version currently supported by default is `2.3.7、3.1.2`, which has not been tested in other versions. More versions will be supported in the future.

### Query Usage

After you finish building the Hive external table in Doris, it is no different from a normal Doris OLAP table except that you cannot use the data model in Doris (rollup, preaggregation, materialized view, etc.)

```sql
select * from t_hive where k1 > 1000 and k3 = 'term' or k4 like '%doris';
```
