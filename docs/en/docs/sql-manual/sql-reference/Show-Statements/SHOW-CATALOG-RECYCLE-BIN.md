---
{
    "title": "SHOW-CATALOG-RECYCLE-BIN",
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

## SHOW-CATALOG-RECYCLE-BIN

### Name

SHOW CATALOG RECYCLE BIN

### Description

This statement is used to display the dropped meta informations that can be recovered

grammar:

```sql
SHOW CATALOG RECYCLE BIN [ WHERE NAME [ = "name" | LIKE "name_matcher"] ]
```

grammar: 

```
The meaning of each column is as follows:
        Type：                type of meta information:Database、Table、Partition
        Name：                name of meta information
        DbId：                id of database
        TableId：             id of table
        PartitionId：         id of partition
        DropTime：            drop time of meta information
```

### Example

 1. Display all meta informations that can be recovered
    
      ```sql
       SHOW CATALOG RECYCLE BIN;
      ```

 2. Display meta informations with name 'test'
    
      ```sql
       SHOW CATALOG RECYCLE BIN WHERE NAME = 'test';
      ```

### Keywords

    SHOW, CATALOG RECYCLE BIN

### Best Practice

