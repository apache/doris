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

    该语句用于使用 `SELECT INTO OUTFILE` 命令将查询结果的导出为文件。目前支持通过 Broker 进程, 通过 S3 协议, 或直接通过 HDFS 协议，导出到远端存储，如 HDFS，S3，BOS，COS（腾讯云）上。

    语法：
        query_stmt
        INTO OUTFILE "file_path"
        [format_as]
        [properties]

    1. file_path
        file_path 指向文件存储的路径以及文件前缀。如 `hdfs://path/to/my_file_`。
        最终的文件名将由 `my_file_`，文件序号以及文件格式后缀组成。其中文件序号由0开始，数量为文件被分割的数量。如：
            my_file_abcdefg_0.csv
            my_file_abcdefg_1.csv
            my_file_abcdegf_2.csv

    2. format_as
        FORMAT AS CSV
        指定导出格式。默认为 CSV。


    3. properties
        指定相关属性。目前支持通过 Broker 进程, 或通过 S3 协议进行导出。

        语法：
        [PROPERTIES ("key"="value", ...)]
        支持如下属性：
        column_separator: 列分隔符
        line_delimiter: 行分隔符
        max_file_size: 单个文件大小限制，如果结果超过这个值，将切割成多个文件。

        Broker 相关属性需加前缀 `broker.`:
        broker.name: broker名称
        broker.hadoop.security.authentication: 指定认证方式为 kerberos
        broker.kerberos_principal: 指定 kerberos 的 principal
        broker.kerberos_keytab: 指定 kerberos 的 keytab 文件路径。该文件必须为 Broker 进程所在服务器上的文件的绝对路径。并且可以被 Broker 进程访问

        HDFS 相关属性需加前缀 `hdfs.`:
        hdfs.fs.defaultFS: namenode 地址和端口
        hdfs.hdfs_user: hdfs 用户名

        S3 协议则直接执行 S3 协议配置即可:
        AWS_ENDPOINT
        AWS_ACCESS_KEY
        AWS_SECRET_KEY
        AWS_REGION

## example

    1. 使用 broker 方式导出，将简单查询结果导出到文件 `hdfs://path/to/result.txt`。指定导出格式为 CSV。使用 `my_broker` 并设置 kerberos 认证信息。指定列分隔符为 `,`，行分隔符为 `\n`。
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
    最终生成文件如如果不大于 100MB，则为：`result_0.csv`。
    如果大于 100MB，则可能为 `result_0.csv, result_1.csv, ...`。

    2. 将简单查询结果导出到文件 `hdfs://path/to/result.parquet`。指定导出格式为 PARQUET。使用 `my_broker` 并设置 kerberos 认证信息。
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
    查询结果导出到parquet文件需要明确指定`schema`。

    3. 将 CTE 语句的查询结果导出到文件 `hdfs://path/to/result.txt`。默认导出格式为 CSV。使用 `my_broker` 并设置 hdfs 高可用信息。使用默认的行列分隔符。
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
    最终生成文件如如果不大于 1GB，则为：`result_0.csv`。
    如果大于 1GB，则可能为 `result_0.csv, result_1.csv, ...`。
    
    4. 将 UNION 语句的查询结果导出到文件 `bos://bucket/result.txt`。指定导出格式为 PARQUET。使用 `my_broker` 并设置 hdfs 高可用信息。PARQUET 格式无需指定列分割符。
    导出完成后，生成一个标识文件。
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

    5. 将 select 语句的查询结果导出到文件 `cos://${bucket_name}/path/result.txt`。指定导出格式为 csv。
    导出完成后，生成一个标识文件。
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
    最终生成文件如如果不大于 1GB，则为：`my_file_0.csv`。
    如果大于 1GB，则可能为 `my_file_0.csv, result_1.csv, ...`。
    在cos上验证
        1. 不存在的path会自动创建
        2. access.key/secret.key/endpoint需要和cos的同学确认。尤其是endpoint的值，不需要填写bucket_name。

    6. 使用 s3 协议导出到 bos，并且并发导出开启。
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
    最终生成的文件前缀为 `my_file_{fragment_instance_id}_`。

    7. 使用 s3 协议导出到 bos，并且并发导出 session 变量开启。
    注意：但由于查询语句带了一个顶层的排序节点，所以这个查询即使开启并发导出的 session 变量，也是无法并发导出的。
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

    8. 使用 hdfs 方式导出，将简单查询结果导出到文件 `hdfs://path/to/result.txt`。指定导出格式为 CSV，用户名为work。指定列分隔符为 `,`，行分隔符为 `\n`。
    SELECT * FROM tbl
    INTO OUTFILE "hdfs://path/to/result_"
    FORMAT AS CSV
    PROPERTIES
    (
        "hdfs.fs.defaultFS" = "hdfs://ip:port",
        "hdfs.hdfs_user" = "work"
    );
    最终生成文件如如果不大于 100MB，则为：`result_0.csv`。
    如果大于 100MB，则可能为 `result_0.csv, result_1.csv, ...`。

## keyword
    OUTFILE

