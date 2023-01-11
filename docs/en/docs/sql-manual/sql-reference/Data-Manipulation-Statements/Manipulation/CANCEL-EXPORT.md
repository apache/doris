---
{
    "title": "CANCEL-EXPORT",
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

## CANCEL-EXPORT 

### Name

<version since="dev">

CANCEL EXPORT 

</version>

### Description

This statement is used to undo an export job for the specified label. Or batch undo export jobs via fuzzy matching

```sql
CANCEL EXPORT 
[FROM db_name]
WHERE [LABEL = "export_label" | LABEL like "label_pattern" | STATE = "PENDING/EXPORTING"]
```

### Example

1. Cancel the export job whose label is `example_db_test_export_label` on the database example_db

    ```sql
    CANCEL EXPORT
    FROM example_db
    WHERE LABEL = "example_db_test_export_label";
    ````

2. Cancel all export jobs containing example* on the database example*db.

    ```sql
    CANCEL EXPORT
    FROM example_db
    WHERE LABEL like "%example%";
    ````

3. Cancel all export jobs which state are "PENDING"

   ```sql
   CANCEL EXPORT
   FROM example_db
   WHERE STATE = "PENDING";
   ```

### Keywords

     CANCEL, EXPORT

### Best Practice

1. Only pending export jobs in PENDING, EXPORTING state can be canceled.
2. When performing batch undo, Doris does not guarantee the atomic undo of all corresponding export jobs. That is, it is possible that only some of the export jobs were successfully undone. The user can view the job status through the SHOW EXPORT statement and try to execute the CANCEL EXPORT statement repeatedly.
