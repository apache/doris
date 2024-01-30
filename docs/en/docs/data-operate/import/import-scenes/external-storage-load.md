---
{
    "title": "External Storage Data Import",
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

# External Storage Data Import

The following mainly introduces how to import data stored in an external system. For example (HDFS, All object stores that support the S3 protocol)
## HDFS LOAD

### Ready to work

Upload the files to be imported to HDFS. For specific commands, please refer to [HDFS upload command](https://hadoop.apache.org/docs/r3.3.2/hadoop-project-dist/hadoop-common/FileSystemShell.html#put )

### start import

Hdfs load creates an import statement. The import method is basically the same as [Broker Load](../../../data-operate/import/import-way/broker-load-manual.md), only need to `WITH BROKER broker_name () ` statement with the following

```
  LOAD LABEL db_name.label_name 
  (data_desc, ...)
  WITH HDFS
  [PROPERTIES (key1=value1, ... )]
```

1. create a table

   ```sql
   CREATE TABLE IF NOT EXISTS load_hdfs_file_test
   (
       id INT,
       age TINYINT,
       name VARCHAR(50)
   )
   unique key(id)
   DISTRIBUTED BY HASH(id) BUCKETS 3;
   ```

2. Import data Execute the following command to import HDFS files:

   ```sql
   LOAD LABEL demo.label_20220402
       (
       DATA INFILE("hdfs://host:port/tmp/test_hdfs.txt")
       INTO TABLE `load_hdfs_file_test`
       COLUMNS TERMINATED BY "\t"            
       (id,age,name)
       )
       with HDFS (
       "fs.defaultFS"="hdfs://testFs",
       "hdfs_user"="user"
       )
       PROPERTIES
       (
       "timeout"="1200",
       "max_filter_ratio"="0.1"
       );
   ```

   For parameter introduction, please refer to [Broker Load](../../../data-operate/import/import-way/broker-load-manual.md), HA cluster creation syntax, view through `HELP BROKER LOAD`

3. Check import status

   Broker load is an asynchronous import method. The specific import results can be accessed through [SHOW LOAD](../../../sql-manual/sql-reference/Show-Statements/SHOW-LOAD.md) command to view
   
   ```
   mysql> show load order by createtime desc limit 1\G;
   *************************** 1. row ***************************
            JobId: 41326624
            Label: broker_load_2022_04_15
            State: FINISHED
         Progress: ETL:100%; LOAD:100%
             Type: BROKER
          EtlInfo: unselected.rows=0; dpp.abnorm.ALL=0; dpp.norm.ALL=27
         TaskInfo: cluster:N/A; timeout(s):1200; max_filter_ratio:0.1
         ErrorMsg: NULL
       CreateTime: 2022-04-01 18:59:06
     EtlStartTime: 2022-04-01 18:59:11
    EtlFinishTime: 2022-04-01 18:59:11
    LoadStartTime: 2022-04-01 18:59:11
   LoadFinishTime: 2022-04-01 18:59:11
              URL: NULL
       JobDetails: {"Unfinished backends":{"5072bde59b74b65-8d2c0ee5b029adc0":[]},"ScannedRows":27,"TaskNumber":1,"All backends":{"5072bde59b74b65-8d2c0ee5b029adc0":[36728051]},"FileNumber":1,"FileSize":5540}
   1 row in set (0.01 sec)
   ```
   
   

## S3 LOAD

Starting from version 0.14, Doris supports the direct import of data from online storage systems that support the S3 protocol through the S3 protocol.

This document mainly introduces how to import data stored in AWS S3. It also supports the import of other object storage systems that support the S3 protocol.
### Applicable scenarios

* Source data in S3 protocol accessible storage systems, such as S3.
* Data volumes range from tens to hundreds of GB.

### Preparing
1. Standard AK and SK
   First, you need to find or regenerate AWS `Access keys`, you can find the generation method in `My Security Credentials` of AWS console, as shown in the following figure:
   [AK_SK](/images/aws_ak_sk.png)
   Select `Create New Access Key` and pay attention to save and generate AK and SK.
2. Prepare REGION and ENDPOINT
   REGION can be selected when creating the bucket or can be viewed in the bucket list. ENDPOINT can be found through REGION on the following page [AWS Documentation](https://docs.aws.amazon.com/general/latest/gr/s3.html#s3_region)

Other cloud storage systems can find relevant information compatible with S3 in corresponding documents

### Start Loading
Like [Broker Load](../../../data-operate/import/import-way/broker-load-manual.md)  just replace `WITH BROKER broker_name ()` with
```
    WITH S3
    (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "AWS_REGION"
    )
```

example:
```
    LOAD LABEL example_db.exmpale_label_1
    (
        DATA INFILE("s3://your_bucket_name/your_file.txt")
        INTO TABLE load_test
        COLUMNS TERMINATED BY ","
    )
    WITH S3
    (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY"="AWS_SECRET_KEY",
        "AWS_REGION" = "AWS_REGION"
    )
    PROPERTIES
    (
        "timeout" = "3600"
    );
```

### FAQ

1. S3 SDK uses virtual-hosted style by default. However, some object storage systems may not be enabled or support virtual-hosted style access. At this time, we can add the `use_path_style` parameter to force the use of path style:

```
   WITH S3
   (
         "AWS_ENDPOINT" = "AWS_ENDPOINT",
         "AWS_ACCESS_KEY" = "AWS_ACCESS_KEY",
         "AWS_SECRET_KEY"="AWS_SECRET_KEY",
         "AWS_REGION" = "AWS_REGION",
         "use_path_style" = "true"
   )
```

<version since="1.2">

2. Support using temporary security credentials to access object stores that support the S3 protocol:

```
  WITH S3
  (
        "AWS_ENDPOINT" = "AWS_ENDPOINT",
        "AWS_ACCESS_KEY" = "AWS_TEMP_ACCESS_KEY",
        "AWS_SECRET_KEY" = "AWS_TEMP_SECRET_KEY",
        "AWS_TOKEN" = "AWS_TEMP_TOKEN",
        "AWS_REGION" = "AWS_REGION"
  )
```

</version>
