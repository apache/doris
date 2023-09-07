---
{
    "title": "SHOW-EXPORT",
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

## SHOW-EXPORT

### Name

SHOW EXPORT

### Description

This statement is used to display the execution of the specified export task

grammar:

```sql
SHOW EXPORT
[FROM db_name]
  [
    WHERE
      [ID=your_job_id]
      [STATE = ["PENDING"|"EXPORTING"|"FINISHED"|"CANCELLED"]]
      [LABEL=your_label]
   ]
[ORDER BY...]
[LIMIT limit];
````

illustrate:
      1. If db_name is not specified, the current default db is used
      2. If STATE is specified, matches EXPORT state
      3. You can use ORDER BY to sort any combination of columns
      4. If LIMIT is specified, limit matching records are displayed. Otherwise show all

### Example

1. Show all export tasks of default db

   ```sql
   SHOW EXPORT;
   ````

2. Display the export tasks of the specified db, sorted by StartTime in descending order

   ```sql
    SHOW EXPORT FROM example_db ORDER BY StartTime DESC;
   ````

3. Display the export tasks of the specified db, the state is "exporting", and sort by StartTime in descending order

   ```sql
   SHOW EXPORT FROM example_db WHERE STATE = "exporting" ORDER BY StartTime DESC;
   ````

4. Display the export task of the specified db and specified job_id

   ```sql
     SHOW EXPORT FROM example_db WHERE ID = job_id;
   ````

5. Display the specified db and specify the export task of the label

   ```sql
    SHOW EXPORT FROM example_db WHERE LABEL = "mylabel";
   ````

### Keywords

    SHOW, EXPORT

### Best Practice

