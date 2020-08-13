---
{
    "title": "CANCEL LOAD",
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

# CANCEL LOAD
Description

This statement is used to undo the import job for the batch of the specified load label.
This is an asynchronous operation, which returns if the task is submitted successfully. After execution, you can use the SHOW LOAD command to view progress.
Grammar:
CANCEL LOAD
[FROM both names]
WHERE LABEL = "load_label";

'35;'35; example

1. Revoke the import job of example_db_test_load_label on the database example_db
CANCEL LOAD
FROM example_db
WHERE LABEL = "example_db_test_load_label";

## keyword
CANCEL,LOAD
