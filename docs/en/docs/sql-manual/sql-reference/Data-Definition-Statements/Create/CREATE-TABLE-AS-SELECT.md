---
{
    "title": "CREATE-TABLE-AS-SELECT",
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

## CREATE-TABLE-AS-SELECT

### Name

CREATE TABLE AS SELECT

### Description

This statement creates the table structure by returning the results from the Select statement and imports the data at the same time

grammar：

```sql
CREATE TABLE table_name [( column_name_list )]
opt_engine opt_partition opt_properties KW_AS query_stmt
 ```

illustrate: 

- The user needs to have`SELECT`permission for the source table and`CREATE`permission for the target database
- After a table is created, data is imported. If the import fails, the table is deleted

### Example

1. Using the field names in the SELECT statement

    ```sql
    create table `test`.`select_varchar` 
    PROPERTIES(\"replication_num\" = \"1\") 
    as select * from `test`.`varchar_table`
    ```

2. Custom field names (need to match the number of fields returned)
    ```sql
    create table `test`.`select_name`(user, testname, userstatus) 
    PROPERTIES(\"replication_num\" = \"1\") 
    as select vt.userId, vt.username, jt.status 
    from `test`.`varchar_table` vt join 
    `test`.`join_table` jt on vt.userId=jt.userId
    ```
   
### Keywords

    CREATE, TABLE, AS, SELECT

### Best Practice

