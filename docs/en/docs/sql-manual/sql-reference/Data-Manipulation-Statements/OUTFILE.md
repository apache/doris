---
{
    "title": "OUTFILE",
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

## OUTFILE
### Name

OURFILE

### description

This statement is used to export query results to a file using the `SELECT INTO OUTFILE` command. Currently, it supports exporting to remote storage, such as HDFS, S3, BOS, COS (Tencent Cloud), through the Broker process, S3 protocol, or HDFS protocol.
    
**grammar:**

```sql
query_stmt
INTO OUTFILE "file_path"
[format_as]
[properties]
```

**illustrate:**

1. file_path

    file_path points to the path where the file is stored and the file prefix. Such as `hdfs://path/to/my_file_`.

    ```
    The final filename will consist of `my_file_`, the file number and the file format suffix. The file serial number starts from 0, and the number is the number of files to be divided. Such as:

    my_file_abcdefg_0.csv
    my_file_abcdefg_1.csv
    my_file_abcdegf_2.csv
    ```
    You can also omit the file prefix and specify only the file directory, such as: `hdfs://path/to/`

2. format_as

    ```
    FORMAT AS CSV
    ```

    Specifies the export format. Supported formats include CSV, PARQUET, CSV_WITH_NAMES, CSV_WITH_NAMES_AND_TYPES and ORC. Default is CSV.
    > Note: PARQUET, CSV_WITH_NAMES, CSV_WITH_NAMES_AND_TYPES, and ORC are supported starting in version 1.2 .

3. properties

    Specify related properties. Currently exporting via the Broker process, S3 protocol, or HDFS protocol is supported.

    ```
    grammar:
    [PROPERTIES ("key"="value", ...)]
    The following properties are supported:

    File related properties
        column_separator: column separator,is only for CSV format. mulit-bytes is supported starting in version 1.2, such as: "\\x01", "abc".
        line_delimiter: line delimiter,is only for CSV format. mulit-bytes supported starting in version 1.2, such as: "\\x01", "abc".
        max_file_size: the size limit of a single file, if the result exceeds this value, it will be cut into multiple files, the value range of max_file_size is [5MB, 2GB] and the default is 1GB. (When specified that the file format is ORC, the size of the actual division file will be a multiples of 64MB, such as: specify max_file_size = 5MB, and actually use 64MB as the division; specify max_file_size = 65MB, and will actually use 128MB as cut division points.)
        delete_existing_files: default `false`. If it is specified as true, you will first delete all files specified in the directory specified by the file_path, and then export the data to the directory.For example: "file_path" = "/user/tmp", then delete all files and directory under "/user/"; "file_path" = "/user/tmp/", then delete all files and directory under "/user/tmp/" 
        file_suffix: Specify the suffix of the export file. If this parameter is not specified, the default suffix for the file format will be used.
    
    Broker related properties need to be prefixed with `broker.`:
        broker.name: broker name
        broker.hadoop.security.authentication: specify the authentication method as kerberos
        broker.kerberos_principal: specifies the principal of kerberos
        broker.kerberos_keytab: specifies the path to the keytab file of kerberos. The file must be the absolute path to the file on the server where the broker process is located. and can be accessed by the Broker process
    
    HDFS related properties:
        fs.defaultFS: namenode address and port
        hadoop.username: hdfs username
        dfs.nameservices: if hadoop enable HA, please set fs nameservice. See hdfs-site.xml
        dfs.ha.namenodes.[nameservice ID]：unique identifiers for each NameNode in the nameservice. See hdfs-site.xml
        dfs.namenode.rpc-address.[nameservice ID].[name node ID]: the fully-qualified RPC address for each NameNode to listen on. See hdfs-site.xml
        dfs.client.failover.proxy.provider.[nameservice ID]：the Java class that HDFS clients use to contact the Active NameNode, usually it is org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider

        For a kerberos-authentication enabled Hadoop cluster, additional properties need to be set:
        dfs.namenode.kerberos.principal: HDFS namenode service principal
        hadoop.security.authentication: kerberos
        hadoop.kerberos.principal: the Kerberos pincipal that Doris will use when connectiong to HDFS.
        hadoop.kerberos.keytab: HDFS client keytab location.
    
    For the S3 protocol, you can directly execute the S3 protocol configuration:
    s3.endpoint
    s3.access_key
    s3.secret_key
    s3.region
    use_path_stype: (optional) default false . The S3 SDK uses the virtual-hosted style by default. However, some object storage systems may not be enabled or support virtual-hosted style access. At this time, we can add the use_path_style parameter to force the use of path style access method.
    ```

    > Note that to use the `delete_existing_files` parameter, you also need to add the configuration `enable_delete_existing_files = true` to the fe.conf file and restart the FE. Only then will the `delete_existing_files` parameter take effect. Setting `delete_existing_files = true` is a dangerous operation and it is recommended to only use it in a testing environment.

4. Data Types for Export

    All file formats support the export of basic data types, while only csv/orc/csv_with_names/csv_with_names_and_types currently support the export of complex data types (ARRAY/MAP/STRUCT). Nested complex data types are not supported.

5. Concurrent Export

    Setting the session variable `set enable_parallel_outfile = true;` enables concurrent export using outfile. For detailed usage, see [Export Query Result](../../../data-operate/export/outfile.md).

6. Export to Local

    To export to a local file, you need configure `enable_outfile_to_local=true` in fe.conf.

    ```sql
    select * from tbl1 limit 10 
    INTO OUTFILE "file:///home/work/path/result_";
    ```

### example

1. Use the broker method to export, and export the simple query results to the file `hdfs://path/to/result.txt`. Specifies that the export format is CSV. Use `my_broker` and set kerberos authentication information. Specify the column separator as `,` and the row separator as `\n`.

   ```sql
   SELECT * FROM tbl
   INTO OUTFILE "hdfs://path/to/result_"
   FORMAT AS CSV
   PROPERTIES
   (
       "broker.name" = "my_broker",
       "broker.hadoop.security.authentication" = "kerberos",
       "broker.kerberos_principal" = "doris@YOUR.COM",
       "broker.kerberos_keytab" = "/home/doris/my.keytab",
       "column_separator" = ",",
       "line_delimiter" = "\n",
       "max_file_size" = "100MB"
   );
   ````

   If the final generated file is not larger than 100MB, it will be: `result_0.csv`.
   If larger than 100MB, it may be `result_0.csv, result_1.csv, ...`.

2. Export the simple query results to the file `hdfs://path/to/result.parquet`. Specify the export format as PARQUET. Use `my_broker` and set kerberos authentication information.

   ```sql
   SELECT c1, c2, c3 FROM tbl
   INTO OUTFILE "hdfs://path/to/result_"
   FORMAT AS PARQUET
   PROPERTIES
   (
       "broker.name" = "my_broker",
       "broker.hadoop.security.authentication" = "kerberos",
       "broker.kerberos_principal" = "doris@YOUR.COM",
       "broker.kerberos_keytab" = "/home/doris/my.keytab"
   );
   ````

3. Export the query result of the CTE statement to the file `hdfs://path/to/result.txt`. The default export format is CSV. Use `my_broker` and set hdfs high availability information. Use the default row and column separators.

   ```sql
   WITH
   x1 AS
   (SELECT k1, k2 FROM tbl1),
   x2 AS
   (SELECT k3 FROM tbl2)
   SELEC k1 FROM x1 UNION SELECT k3 FROM x2
   INTO OUTFILE "hdfs://path/to/result_"
   PROPERTIES
   (
       "broker.name" = "my_broker",
       "broker.username"="user",
       "broker.password"="passwd",
       "broker.dfs.nameservices" = "my_ha",
       "broker.dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2",
       "broker.dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
       "broker.dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
       "broker.dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
   );
   ````

   If the final generated file is not larger than 1GB, it will be: `result_0.csv`.
   If larger than 1GB, it may be `result_0.csv, result_1.csv, ...`.

4. Export the query result of the UNION statement to the file `bos://bucket/result.txt`. Specify the export format as PARQUET. Use `my_broker` and set hdfs high availability information. The PARQUET format does not require a column delimiter to be specified.
   After the export is complete, an identity file is generated.

   ```sql
   SELECT k1 FROM tbl1 UNION SELECT k2 FROM tbl1
   INTO OUTFILE "bos://bucket/result_"
   FORMAT AS PARQUET
   PROPERTIES
   (
       "broker.name" = "my_broker",
       "broker.bos_endpoint" = "http://bj.bcebos.com",
       "broker.bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxxx",
       "broker.bos_secret_accesskey" = "yyyyyyyyyyyyyyyyyyyyyyyyy"
   );
   ````

5. Export the query result of the select statement to the file `s3a://${bucket_name}/path/result.txt`. Specify the export format as csv.
   After the export is complete, an identity file is generated.

   ```sql
   select k1,k2,v1 from tbl1 limit 100000
   into outfile "s3a://my_bucket/export/my_file_"
   FORMAT AS CSV
   PROPERTIES
   (
       "broker.name" = "hdfs_broker",
       "broker.fs.s3a.access.key" = "xxx",
       "broker.fs.s3a.secret.key" = "xxxx",
       "broker.fs.s3a.endpoint" = "https://cos.xxxxxx.myqcloud.com/",
       "column_separator" = ",",
       "line_delimiter" = "\n",
       "max_file_size" = "1024MB",
       "success_file_name" = "SUCCESS"
   )
   ````

   If the final generated file is not larger than 1GB, it will be: `my_file_0.csv`.
   If larger than 1GB, it may be `my_file_0.csv, result_1.csv, ...`.
   Verify on cos

          1. A path that does not exist will be automatically created
          2. Access.key/secret.key/endpoint needs to be confirmed with students of cos. Especially the value of endpoint does not need to fill in bucket_name.

6. Use the s3 protocol to export to bos, and enable concurrent export.

   ```sql
   set enable_parallel_outfile = true;
   select k1 from tb1 limit 1000
   into outfile "s3://my_bucket/export/my_file_"
   format as csv
   properties
   (
        "s3.endpoint" = "http://s3.bd.bcebos.com",
        "s3.access_key" = "xxxx",
        "s3.secret_key" = "xxx",
        "s3.region" = "bd"
   )
   ````

   The resulting file is prefixed with `my_file_{fragment_instance_id}_`.

7. Use the s3 protocol to export to bos, and enable concurrent export of session variables.
   Note: However, since the query statement has a top-level sorting node, even if the concurrently exported session variable is enabled for this query, it cannot be exported concurrently.

   ```sql
   set enable_parallel_outfile = true;
   select k1 from tb1 order by k1 limit 1000
   into outfile "s3://my_bucket/export/my_file_"
   format as csv
   properties
   (
        "s3.endpoint" = "http://s3.bd.bcebos.com",
        "s3.access_key" = "xxxx",
        "s3.secret_key" = "xxx",
        "s3.region" = "bd"
   )
   ````

8. Use hdfs export to export simple query results to the file `hdfs://${host}:${fileSystem_port}/path/to/result.txt`. Specify the export format as CSV and the user name as work. Specify the column separator as `,` and the row separator as `\n`.

   ```sql
   -- the default port of fileSystem_port is 9000
   SELECT * FROM tbl
   INTO OUTFILE "hdfs://${host}:${fileSystem_port}/path/to/result_"
   FORMAT AS CSV
   PROPERTIES
   (
       "fs.defaultFS" = "hdfs://ip:port",
       "hadoop.username" = "work"
   );
   ```

   If the Hadoop cluster is highly available and Kerberos authentication is enabled, you can refer to the following SQL statement:

   ```sql
   SELECT * FROM tbl
   INTO OUTFILE "hdfs://path/to/result_"
   FORMAT AS CSV
   PROPERTIES
   (
   'fs.defaultFS'='hdfs://hacluster/',
   'dfs.nameservices'='hacluster',
   'dfs.ha.namenodes.hacluster'='n1,n2',
   'dfs.namenode.rpc-address.hacluster.n1'='192.168.0.1:8020',
   'dfs.namenode.rpc-address.hacluster.n2'='192.168.0.2:8020',
   'dfs.client.failover.proxy.provider.hacluster'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
   'dfs.namenode.kerberos.principal'='hadoop/_HOST@REALM.COM'
   'hadoop.security.authentication'='kerberos',
   'hadoop.kerberos.principal'='doris_test@REALM.COM',
   'hadoop.kerberos.keytab'='/path/to/doris_test.keytab'
   );

   If the final generated file is not larger than 100MB, it will be: `result_0.csv`.
   If larger than 100MB, it may be `result_0.csv, result_1.csv, ...`.

9. Export the query result of the select statement to the file `cosn://${bucket_name}/path/result.txt` on Tencent Cloud Object Storage (COS). Specify the export format as csv.
   After the export is complete, an identity file is generated.

   ```sql
   select k1,k2,v1 from tbl1 limit 100000
   into outfile "cosn://my_bucket/export/my_file_"
   FORMAT AS CSV
   PROPERTIES
   (
       "broker.name" = "broker_name",
       "broker.fs.cosn.userinfo.secretId" = "xxx",
       "broker.fs.cosn.userinfo.secretKey" = "xxxx",
       "broker.fs.cosn.bucket.endpoint_suffix" = "cos.xxxxxx.myqcloud.com",
       "column_separator" = ",",
       "line_delimiter" = "\n",
       "max_file_size" = "1024MB",
       "success_file_name" = "SUCCESS"
   )
   ````

### keywords

OUTFILE

### Best Practice

1. Export data volume and export efficiency

   This function essentially executes an SQL query command. The final result is a single-threaded output. Therefore, the time-consuming of the entire export includes the time-consuming of the query itself and the time-consuming of writing the final result set. If the query is large, you need to set the session variable `query_timeout` to appropriately extend the query timeout.

2. Management of export files

   Doris does not manage exported files. Including the successful export, or the remaining files after the export fails, all need to be handled by the user.

3. Export to local file

   The ability to export to a local file is not available for public cloud users, only for private deployments. And the default user has full control over the cluster nodes. Doris will not check the validity of the export path filled in by the user. If the process user of Doris does not have write permission to the path, or the path does not exist, an error will be reported. At the same time, for security reasons, if a file with the same name already exists in this path, the export will also fail.

   Doris does not manage files exported locally, nor does it check disk space, etc. These files need to be managed by the user, such as cleaning and so on.

4. Results Integrity Guarantee

   This command is a synchronous command, so it is possible that the task connection is disconnected during the execution process, so that it is impossible to live the exported data whether it ends normally, or whether it is complete. At this point, you can use the `success_file_name` parameter to request that a successful file identifier be generated in the directory after the task is successful. Users can use this file to determine whether the export ends normally.

5. Other Points to Note

    See [Export Query Result](../../../data-operate/export/outfile.md)