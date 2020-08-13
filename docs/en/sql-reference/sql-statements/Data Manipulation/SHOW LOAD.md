---
{
    "title": "SHOW LOAD",
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

# SHOW LOAD
## Description
This statement is used to show the execution of the specified import task
Grammar:
SHOW LOAD
[FROM both names]
[
WHERE
[LABEL [ = "your_label" | LIKE "label_matcher"]]
[STATE = ["PENDING"|"ETL"|"LOADING"|"FINISHED"|"CANCELLED"|]]
]
[ORDER BY ...]
[LIMIT limit][OFFSET offset];

Explain:
1) If db_name is not specified, use the current default DB
2) If you use LABEL LIKE, the label that matches the import task contains the import task of label_matcher
3) If LABEL = is used, the specified label is matched accurately.
4) If STATE is specified, the LOAD state is matched
5) Arbitrary column combinations can be sorted using ORDER BY
6) If LIMIT is specified, the limit bar matching record is displayed. Otherwise, all of them will be displayed.
7) If OFFSET is specified, the query results are displayed from offset. By default, the offset is 0.
8) If broker/mini load is used, the connection in the URL column can be viewed using the following command:

SHOW LOAD WARNINGS ON 'url'

## example
1. Show all import tasks of default DB
SHOW LOAD;

2. Show the import task of the specified db. The label contains the string "2014_01_02", showing the oldest 10
SHOW LOAD FROM example_db WHERE LABEL LIKE "2014_01_02" LIMIT 10;

3. Show the import task of the specified db, specify label as "load_example_db_20140102" and sort it in descending order by LoadStartTime
SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" ORDER BY LoadStartTime DESC;

4. Show the import task of the specified db, specify label as "load_example_db_20140102" and state as "load", and sort it in descending order by LoadStartTime
SHOW LOAD FROM example_db WHERE LABEL = "load_example_db_20140102" AND STATE = "loading" ORDER BY LoadStartTime DESC;

5. Show the import task of the specified dB and sort it in descending order by LoadStartTime, and display 10 query results starting with offset 5
SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 5,10;
SHOW LOAD FROM example_db ORDER BY LoadStartTime DESC limit 10 offset 5;

6. Small batch import is a command to view the import status
curl --location-trusted -u {user}:{passwd} http://{hostname}:{port}/api/{database}/_load_info?label={labelname}

## keyword
SHOW,LOAD
