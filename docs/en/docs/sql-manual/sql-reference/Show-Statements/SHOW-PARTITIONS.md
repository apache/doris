---
{
    "title": "SHOW-PARTITIONS",
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

## SHOW-PARTITIONS

### Name

SHOW PARTITIONS

### Description

  This statement is used to display partition information for tables in Internal catalog or Hive Catalog

grammar:

````SQL
  SHOW [TEMPORARY] PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT];
````

illustrate:

When used in Internal catalog:
1. Support the filtering of PartitionId, PartitionName, State, Buckets, ReplicationNum, LastConsistencyCheckTime and other columns
2. TEMPORARY specifies to list temporary partitions

<version since="2.0">

when used in Hive Catalog:
Will return all partitions' name. Support multilevel partition table

</version>

### Example

1. Display all non-temporary partition information of the specified table under the specified db

````SQL
  SHOW PARTITIONS FROM example_db.table_name;
````

2. Display all temporary partition information of the specified table under the specified db

    ````SQL
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name;
    ````

3. Display the information of the specified non-temporary partition of the specified table under the specified db

    ````SQL
     SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    ````

4. Display the latest non-temporary partition information of the specified table under the specified db

    ````SQL
    SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    ````

### Keywords

    SHOW, PARTITIONS

### Best Practice

