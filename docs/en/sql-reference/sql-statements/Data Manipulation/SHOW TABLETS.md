---
{
    "title": "SHOW TABLETS",
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

# SHOW TABLETS
## Description
    This statement is used to display tablet-related information (for administrators only)
    Grammar:
        SHOW TABLETS
        [FROM [db_name.]table_name] [partiton(partition_name_1, partition_name_1)]
        [where [version=1] [and backendid=10000] [and state="NORMAL|ROLLUP|CLONE|DECOMMISSION"]]
        [order by order_column]
        [limit [offset,]size]

## example
        // Display all tablets information in the specified table below the specified DB
        SHOW TABLETS FROM example_db.table_name;
        
        SHOW TABLETS FROM example_db.table_name partition(p1, p2);
        
        // display 10 tablets information in the table
        SHOW TABLETS FROM example_db.table_name limit 10;
        
        SHOW TABLETS FROM example_db.table_name limit 5,10;
        
        // display the tablets that fulfill some conditions
        SHOW TABLETS FROM example_db.table_name where backendid=10000 and version=1 and state="NORMAL";
        
        SHOW TABLETS FROM example_db.table_name where backendid=10000 order by version;
    
        SHOW TABLETS FROM example_db.table_name where indexname="t1_rollup";

## keyword
    SHOW,TABLETS,LIMIT
