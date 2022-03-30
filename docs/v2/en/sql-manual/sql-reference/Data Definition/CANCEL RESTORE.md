---
{
    "title": "CANCEL RESTORE",
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

# CANCEL RESTORE
## Description
This statement is used to cancel an ongoing RESTORE task.
Grammar:
CANCEL RESTORE FROM db_name;

Be careful:
When the recovery is abolished around the COMMIT or later stage, the restored tables may be inaccessible. At this point, data recovery can only be done by performing the recovery operation again.

## example
1. Cancel the RESTORE task under example_db.
CANCEL RESTORE FROM example_db;

## keyword
CANCEL, RESTORE

