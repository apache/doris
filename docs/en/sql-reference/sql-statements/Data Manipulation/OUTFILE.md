---
{
    "title": "OUTFILE",
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

# OUTFILE
## description

    The `SELECT INTO OUTFILE` statement can export the query results to a file. Currently supports export to remote storage through Broker process, or directly through S3, HDFS  protocol such as HDFS, S3, BOS and COS(Tencent Cloud) through the Broker process. The syntax is as follows:

    Grammar：
        query_stmt
        INTO OUTFILE "file_path"
        [format_as]
        [properties]

    1. file_path
        `file_path` specify the file path and file name prefix. Like: `hdfs://path/to/my_file_`.
        The final file name will be assembled as `my_file_`, file seq no and the format suffix. File seq no starts from 0, determined by the number of split.
            my_file_abcdefg_0.csv
            my_file_abcdefg_1.csv
            my_file_abcdegf_2.csv

    2. format_as
        FORMAT AS CSV
        Specify the export format. The default is CSV.


    3. properties
        Specify the relevant attributes. Currently it supports exporting through the Broker process, or through the S3, HDFS protocol.

        Grammar：
        [PROPERTIES ("key"="value", ...)]
        The following parameters can be specified:
          column_separator: Specifies the exported column separator, defaulting to t. Supports invisible characters, such as'\x07'.
          line_delimiter: Specifies the exported line separator, defaulting to\n. Supports invisible characters, such as'\x07'.
          max_file_size: max size for each file

        Broker related attributes need to be prefixed with `broker.`:
        broker.name: broker name
        broker.hadoop.security.authentication: Specify authentication as kerberos
        broker.kerberos_principal: Specify the principal of kerberos
        broker.kerberos_keytab: Specify the keytab path of kerberos, this file is the path on the broker.

        HDFS protocal can directly execute HDFS protocal configuration:
        hdfs.fs.defaultFS: namenode ip and port
        hdfs.hdfs_user: hdfs user name

        S3 protocol can directly execute S3 protocol configuration:
        AWS_ENDPOINT
        AWS_ACCESS_KEY
        AWS_SECRET_KEY
        AWS_REGION

## example

    1. Export simple query results to the file `hdfs://path/to/result.txt`. Specify the export format as CSV. Use `my_broker` and set kerberos authentication information. Specify the column separator as `,` and the line delimiter as `\n`.
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
    If the result is less than 100MB, file will be: `result_0.csv`.
    If larger than 100MB, may be: `result_0.csv, result_1.csv, ...`.

    2. Export simple query results to the file `hdfs://path/to/result.parquet`. Specify the export format as PARQUET. Use `my_broker` and set kerberos authentication information. 
    SELECT c1, c2, c3 FROM tbl
    INTO OUTFILE "hdfs://path/to/result_"
    FORMAT AS PARQUET
    PROPERTIES
    (
        "broker.name" = "my_broker",
        "broker.hadoop.security.authentication" = "kerberos",
        "broker.kerberos_principal" = "doris@YOUR.COM",
        "broker.kerberos_keytab" = "/home/doris/my.keytab",
        "schema"="required,int32,c1;required,byte_array,c2;required,byte_array,c2"
    );
    If the exported file format is PARQUET, `schema` must be specified.

    3. Export the query result of the CTE statement to the file `hdfs://path/to/result.txt`. The default export format is CSV. Use `my_broker` and set hdfs high availability information. Use the default column separators and line delimiter.
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
    If the result is less than 1GB, file will be: `result_0.csv`.
    If larger than 1GB, may be: `result_0.csv, result_1.csv, ...`.
    
    4. Export the query results of the UNION statement to the file `bos://bucket/result.parquet`. Specify the export format as PARQUET. Use `my_broker` and set hdfs high availability information. PARQUET format does not need to specify the column separator and line delimiter.
    SELECT k1 FROM tbl1 UNION SELECT k2 FROM tbl1
    INTO OUTFILE "bos://bucket/result_"
    FORMAT AS PARQUET
    PROPERTIES
    (
        "broker.name" = "my_broker",
        "broker.bos_endpoint" = "http://bj.bcebos.com",
        "broker.bos_accesskey" = "xxxxxxxxxxxxxxxxxxxxxxxxxx",
        "broker.bos_secret_accesskey" = "yyyyyyyyyyyyyyyyyyyyyyyyyy",
        "schema"="required,int32,k1;required,byte_array,k2"
    );

    5. Export simple query results to the file `cos://${bucket_name}/path/result.txt`. Specify the export format as CSV.
    And create a mark file after export finished.
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
    Please Note: 
        1. Paths that do not exist are automatically created.
        2. These parameters(access.key/secret.key/endpointneed) need to be confirmed with `Tecent Cloud COS`. In particular, the value of endpoint does not need to be filled in bucket_name.

    6. Use the s3 protocol to export to bos, and concurrent export is enabled.
    set enable_parallel_outfile = true;
    select k1 from tb1 limit 1000
    into outfile "s3://my_bucket/export/my_file_"
    format as csv
    properties
    (
        "AWS_ENDPOINT" = "http://s3.bd.bcebos.com",
        "AWS_ACCESS_KEY" = "xxxx",
        "AWS_SECRET_KEY" = "xxx",
        "AWS_REGION" = "bd"
    )
    The final generated file prefix is `my_file_{fragment_instance_id}_`。

    7. Use the s3 protocol to export to bos, and enable concurrent export of session variables.
    set enable_parallel_outfile = true;
    select k1 from tb1 order by k1 limit 1000
    into outfile "s3://my_bucket/export/my_file_"
    format as csv
    properties
    (
        "AWS_ENDPOINT" = "http://s3.bd.bcebos.com",
        "AWS_ACCESS_KEY" = "xxxx",
        "AWS_SECRET_KEY" = "xxx",
        "AWS_REGION" = "bd"
    )
    But because the query statement has a top-level sorting node, even if the query is enabled for concurrently exported session variables, it cannot be exported concurrently.

    8. Use libhdfs to export to hdfs cluster. Export the query results of the UNION statement to the file `hdfs://path/to/result.txt`
        Specify the export format as CSV. Use the user name as 'work', the column separators as ',' and line delimiter as '\n'.
    SELECT * FROM tbl
    INTO OUTFILE "hdfs://path/to/result_"
    FORMAT AS CSV
    PROPERTIES
    (
        "hdfs.fs.defaultFS" = "hdfs://ip:port",
        "hdfs.hdfs_user" = "work"
    );
    If the result is less than 1GB, file will be: `my_file_0.csv`.
    If larger than 1GB, may be: `my_file_0.csv, result_1.csv, ...`.

## keyword
    OUTFILE

