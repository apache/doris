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
Description

This statement is used to export data from a specified table to a specified location.
This function is implemented by broker process. For different purpose storage systems, different brokers need to be deployed. Deployed brokers can be viewed through SHOW BROKER.
This is an asynchronous operation, which returns if the task is submitted successfully. After execution, you can use the SHOW EXPORT command to view progress.

Grammar:
EXPORT TABLE table_name
[PARTITION (p1 [,p2]]
TO export_path
[opt_properties]
broker;

1. table_name
The table names to be exported currently support the export of tables with engine as OLAP and mysql.

2. partition
You can export only certain specified partitions of the specified table

3. export_path
The exported path needs to be a directory. At present, it can't be exported to local, so it needs to be exported to broker.

4. opt_properties
Used to specify some special parameters.
Grammar:
[PROPERTIES ("key"="value", ...)]

The following parameters can be specified:
Column_separator: Specifies the exported column separator, defaulting to t.
Line_delimiter: Specifies the exported line separator, defaulting to\n.
Exc_mem_limit: Exports the upper limit of memory usage for a single BE node, defaulting to 2GB in bytes.
Timeout: The time-out for importing jobs is 1 day by default, in seconds.
Tablet_num_per_task: The maximum number of tablets that each subtask can allocate.

Five. debris
Broker used to specify export usage
Grammar:
WITH BROKER broker_name ("key"="value"[,...])
Here you need to specify the specific broker name and the required broker attributes

For brokers corresponding to different storage systems, the input parameters are different. Specific parameters can be referred to: `help broker load', broker required properties.

'35;'35; example

1. Export all data from the testTbl table to HDFS
EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

2. Export partitions P1 and P2 from the testTbl table to HDFS

EXPORT TABLE testTbl PARTITION (p1,p2) TO "hdfs://hdfs_host:port/a/b/c" WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");
3. Export all data in the testTbl table to hdfs, using "," as column separator

EXPORT TABLE testTbl TO "hdfs://hdfs_host:port/a/b/c" PROPERTIES ("column_separator"=",") WITH BROKER "broker_name" ("username"="xxx", "password"="yyy");

## keyword
EXPORT
