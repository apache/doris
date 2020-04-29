---
{
    "title": "CREATE VIEW",
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

# CREATE VIEW
## Description
    This statement is used to create a logical view
    Grammar:
    
        CREATE VIEW [IF NOT EXISTS]
        [db_name.]view_name
        (column1[ COMMENT "col comment"][, column2, ...])
        AS query_stmt

    Explain:

        1. Views are logical views without physical storage. All queries on views are equivalent to sub-queries corresponding to views.
        2. query_stmt is arbitrarily supported SQL.

## example

    1. Create view example_view on example_db

        CREATE VIEW example_db.example_view (k1, k2, k3, v1)
        AS
        SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
        WHERE k1 = 20160112 GROUP BY k1,k2,k3;
        
    2. Create view with comment
    
        CREATE VIEW example_db.example_view
        (
            k1 COMMENT "first key",
            k2 COMMENT "second key",
            k3 COMMENT "third key",
            v1 COMMENT "first value"
        )
        COMMENT "my first view"
        AS
        SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
        WHERE k1 = 20160112 GROUP BY k1,k2,k3;

## keyword

    CREATE,VIEW

