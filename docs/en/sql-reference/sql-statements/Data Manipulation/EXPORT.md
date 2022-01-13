---
{
    "title": "EXPORT",
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

# EXPORT
## Description

    This statement is used to export data from a specified table to a specified location.
    This function is implemented by broker process. For different purpose storage systems, different brokers need to be deployed. Deployed brokers can be viewed through SHOW BROKER.
    This is an asynchronous operation, which returns if the task is submitted successfully. After execution, you can use the SHOW EXPORT command to view progress.

    Grammar:
        EXPORT TABLE table_name
        [PARTITION (p1 [,p2]]
        [WHERE [expr]]
        TO export_path
        [opt_properties]
        [broker｜S3];

    1. table_name
       The table names to be exported currently support the export of tables with engine as OLAP and mysql.

    2. partition
       You can export only certain specified partitions of the specified table

    3. expr
       Export rows that meet the where condition, optional. If you leave it blank, all rows are exported by default. 

    4. export_path
       The exported path needs to be a directory. At present, it can't be exported to local, so it needs to be exported to broker.

    5. opt_properties
       Used to specify some special parameters.
       Grammar:
       [PROPERTIES ("key"="value", ...)]

        The following parameters can be specified:
          label: The identifier of this export job. You can use this identifier to view the job status later.
          column_separator: Specifies the exported column separator, defaulting to t. Supports invisible characters, such as'\x07'.
          column: Specify the columns to be exported, separated by commas. If you do not fill in this parameter, the default is to export all the columns of the table.
          line_delimiter: Specifies the exported line separator, defaulting to\n. Supports invisible characters, such as'\x07'.
          exec_mem_limit: Exports the upper limit of memory usage for a single BE node, defaulting to 2GB in bytes.
          timeout: The time-out for importing jobs is 1 day by default, in seconds.
          tablet_num_per_task: The maximum number of tablets that each subtask can allocate.

     6. broker|S3
        Specify to use broker export or export through S3 protocol
          Grammar:
          WITH [BROKER broker_name| S3] ("key"="value"[,...])
          Here you need to specify the specific broker name and the required broker attributes, If you use the S3 protocol, you do not need to specify the broker name

        For brokers corresponding to different storage systems, the input parameters are different. Specific parameters can be referred to: `help broker load', broker required properties.
        When exporting to local, you do not need to fill in this part.

    7. hdfs
      Specify to use libhdfs export to hdfs
          Grammar：
          WITH HDFS ("key"="value"[,...])

          The following parameters can be specified:
            fs.defaultFS: Set the fs such as：hdfs://ip:port
            hdfs_user：Specify hdfs user name

## example

    1. Export all data from the testTbl table to HDFS
       EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

    2. Export partitions P1 and P2 from the testTbl table to HDFS
       EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

    3. Export all data in the testTbl table to hdfs, using "," as column separator, and specify label
       EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("label" = "mylabel", "column_separator"=",") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

    4. Export the row meet condition k1 = 1 in the testTbl table to hdfs.
       EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WHERE k1=1 WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

    5. Export all data in the testTbl table to the local.
       EXPORT TABLE testTbl TO "file:///home/data/a";

    6. Export all data in the testTbl table to hdfs, using the invisible character "\x07" as the column and row separator. 
       EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("column_separator"="\\x07", "line_delimiter" = "\\x07") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy")

    7. Export column k1, v1 from the testTbl to the local.
       EXPORT TABLE testTbl TO "file:///home/data/a" PROPERTIES ("columns" = "k1,v1");

    8. Export all data in the testTbl table to hdfs, using the invisible character "\x07" as the column and row separator. 
        EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("column_separator"="\\x07", "line_delimiter" = "\\x07") WITH HDFS ("fs.defaultFS"="hdfs://hdfs_host:port", "hdfs_user"="yyy")

## keyword
    EXPORT
