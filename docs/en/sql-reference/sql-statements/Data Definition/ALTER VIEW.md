---
{
    "title": "ALTER VIEW",
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

# ALTER VIEW
## description
	This statement is used to modify the definition of a view
	Syntax:
		ALTER VIEW
        [db_name.]view_name
        (column1[ COMMENT "col comment"][, column2, ...])
        AS query_stmt
        
    Explain:
    	1. View is logical, it isn't stored in the physical medium. When we querying, view will be embed as subqueries in query statement. Therefore, modifying the definition of views is equivalent to modifying query_stmt which is defined in view.
    	2. query_stmt is arbitrarily supported SQL.
    	
## example

	1. Modify example_view on the example_db

		ALTER VIEW example_db.example_view
		(
			c1 COMMENT "column 1",
			c2 COMMENT "column 2",
			c3 COMMENT "column 3"
		)
		AS SELECT k1, k2, SUM(v1) FROM example_table 
		GROUP BY k1, k2