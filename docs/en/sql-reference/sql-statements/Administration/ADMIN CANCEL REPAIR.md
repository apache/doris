---
{
    "title": "ADMIN CANCEL REPAIR",
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

# ADMIN CANCEL REPAIR
## Description

This statement is used to cancel repairing a specified table or partition with high priority

Grammar:

ADMIN CANCEL REPAIR TABLE table_name[ PARTITION (p1,...)];

Explain:

1. This statement only indicates that the system no longer repairs fragmented copies of specified tables or partitions with high priority. The system will still repair the copy by default scheduling.

## example

1. Cancel High Priority Repair

ADMIN CANCEL REPAIR TABLE tbl PARTITION(p1);

## keyword
ADMIN,CANCEL,REPAIR
