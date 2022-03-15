---
{
    "title": "SHOW EXPORT",
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

# SHOW EXPORT
## Description
This statement is used to show the execution of the specified export task
Grammar:
        SHOW EXPORT
        [FROM db_name]
        [
            WHERE
            [ID = your_job_id]
            [STATE = ["PENDING"|"EXPORTING"|"FINISHED"|"CANCELLED"]]
			[LABEL [ = "your_label" | LIKE "label_matcher"]]
        ]
        [ORDER BY ...]
        [LIMIT limit];

Explain:
1) If db_name is not specified, use the current default DB
2) If STATE is specified, the EXPORT state is matched
3) Any column combination can be sorted using ORDER BY
4) If LIMIT is specified, the limit bar matching record is displayed. Otherwise, all of them will be displayed.

## example
1. Show all export tasks of default DB
    SHOW EXPORT;

2. Show the export tasks of the specified db, sorted in descending order by StartTime
    SHOW EXPORT FROM example_db ORDER BY StartTime DESC;

3. Show the export task of the specified db, state is "exporting" and sorted in descending order by StartTime
    SHOW EXPORT FROM example_db WHERE STATE = "exporting" ORDER BY StartTime DESC;

4. Show the export task of specifying DB and job_id
    SHOW EXPORT FROM example_db WHERE ID = job_id;

5. Show the export task of specifying DB and label
    SHOW EXPORT FROM example_db WHERE LABEL = "mylabel";

6. Show the export task of specifying DB and label prefix is "labelprefix"
    SHOW EXPORT FROM example_db WHERE LABEL LIKE "labelprefix%";

## keyword

	SHOW,EXPORT

