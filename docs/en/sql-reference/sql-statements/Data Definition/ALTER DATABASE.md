---
{
    "title": "ALTER DATABASE",
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

# ALTER DATABASE
## description
This statement is used to set the properties of the specified database. (Administrators only)
Grammar:
1) Setting database data quota in B/K/KB/M/MB/G/GB/T/TB/P/PB
OTHER DATABASE dbu name SET DATA QUOTA quota;

2) Rename the database
ALTER DATABASE db_name RENAME new_db_name;

Explain:
After renaming the database, use REVOKE and GRANT commands to modify the corresponding user rights if necessary.
The database's default data quota is 1024GB, and the default replica quota is 1073741824.

## example
1. Setting the specified database data quota
ALTER DATABASE example_db SET DATA QUOTA 10995116277760;
The above units are bytes, equivalent to
ALTER DATABASE example_db SET DATA QUOTA 10T;

ALTER DATABASE example_db SET DATA QUOTA 100G;

ALTER DATABASE example_db SET DATA QUOTA 200M;

2. Rename the database example_db to example_db2
ALTER DATABASE example_db RENAME example_db2;

## keyword
ALTER,DATABASE,RENAME

