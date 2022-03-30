---
{
    "title": "SHOW PARTITIONS",
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

# SHOW PARTITIONS
## Description
This statement is used to display partition information
Grammar:
SHOW PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT];
Explain:
Support filter with following columns: PartitionId,PartitionName,State,Buckets,ReplicationNum,
LastConsistencyCheckTime

## example
1. Display partition information for the specified table below the specified DB
SHOW PARTITIONS FROM example_db.table_name;

2. Display information about the specified partition of the specified table below the specified DB
SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";

3. Display information about the newest partition of the specified table below the specified DB
SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;

## keyword
SHOW,PARTITIONS

