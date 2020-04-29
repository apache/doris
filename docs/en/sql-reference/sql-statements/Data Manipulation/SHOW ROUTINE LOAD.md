---
{
    "title": "SHOW ROUTINE LOAD",
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

# SHOW ROUTINE LOAD
## example

1. Show all routine import jobs named test 1 (including stopped or cancelled jobs). The result is one or more lines.

SHOW ALL ROUTINE LOAD FOR test1;

2. Show the current running routine import job named test1

SHOW ROUTINE LOAD FOR test1;

3. Display all routine import jobs (including stopped or cancelled jobs) under example_db. The result is one or more lines.

use example_db;
SHOW ALL ROUTINE LOAD;

4. Display all running routine import jobs under example_db

use example_db;
SHOW ROUTINE LOAD;

5. Display the current running routine import job named test1 under example_db

SHOW ROUTINE LOAD FOR example_db.test1;

6. Display all routine import jobs named test1 (including stopped or cancelled jobs) under example_db. The result is one or more lines.

SHOW ALL ROUTINE LOAD FOR example_db.test1;

## keyword
SHOW,ROUTINE,LOAD
