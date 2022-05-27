---
{
    "title": "SHOW-STREAM-LOAD",
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

## SHOW-STREAM-LOAD

### Name

SHOW STREAM LOAD

### Description

This statement is used to display the execution of the specified Stream Load task

grammar:

```sql
SHOW STREAM LOAD
[FROM db_name]
[
  WHERE
  [LABEL [ = "your_label" | LIKE "label_matcher"]]
  [STATUS = ["SUCCESS"|"FAIL"]]
]
[ORDER BY...]
[LIMIT limit][OFFSET offset];
````

illustrate:

1. If db_name is not specified, the current default db is used
2. If LABEL LIKE is used, it will match the tasks whose label of the Stream Load task contains label_matcher
3. If LABEL = is used, it will match the specified label exactly
4. If STATUS is specified, matches STREAM LOAD status
5. You can use ORDER BY to sort on any combination of columns
6. If LIMIT is specified, limit matching records are displayed. Otherwise show all
7. If OFFSET is specified, the query results are displayed starting at offset offset. By default the offset is 0.

### Example

1. Show all Stream Load tasks of the default db

   ```sql
     SHOW STREAM LOAD;
   ````

2. Display the Stream Load task of the specified db, the label contains the string "2014_01_02", and display the oldest 10

   ```sql
   SHOW STREAM LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;
   ````

3. Display the Stream Load task of the specified db and specify the label as "load_example_db_20140102"

   ```sql
   SHOW STREAM LOAD FROM example_db WHERE LABEL = "load_example_db_20140102";
   ````

4. Display the Stream Load task of the specified db, specify the status as "success", and sort by StartTime in descending order

   ```sql
   SHOW STREAM LOAD FROM example_db WHERE STATUS = "success" ORDER BY StartTime DESC;
   ````

5. Display the import tasks of the specified db and sort them in descending order of StartTime, and display 10 query results starting from offset 5

   ```sql
   SHOW STREAM LOAD FROM example_db ORDER BY StartTime DESC limit 5,10;
   SHOW STREAM LOAD FROM example_db ORDER BY StartTime DESC limit 10 offset 5;
   ````

### Keywords

    SHOW, STREAM, LOAD

### Best Practice

