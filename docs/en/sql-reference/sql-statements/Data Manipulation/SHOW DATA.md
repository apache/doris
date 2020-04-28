---
{
    "title": "SHOW DATA",
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

# SHOW DATA
## Description
This statement is used to show the amount of data and the number of replica
Grammar:
SHOW DATA [FROM db_name[.table_name]];

Explain:
1. If you do not specify the FROM clause, use the amount of data and the number of replica that shows the current DB subdivided into tables
2. If the FROM clause is specified, the amount of data and the number of replica subdivided into indices under the table is shown.
3. If you want to see the size of individual Partitions, see help show partitions

## example
1. Display the data volume, replica size, aggregate data volume and aggregate replica count of each table of default DB
SHOW DATA;

2. Display the subdivision data volume and replica count of the specified table below the specified DB
SHOW DATA FROM example_db.table_name;

## keyword
SHOW,DATA
