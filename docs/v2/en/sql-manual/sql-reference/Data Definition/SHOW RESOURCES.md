---
{
    "title": "SHOW RESOURCES",
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

# SHOW RESOURCES
## description

    This statement is used to display the resources that the user has permission to use. Ordinary users can only display the resources with permission, while root or admin users can display all the resources.
    
    Grammar
    
        SHOW RESOURCES
        [
            WHERE 
            [NAME [ = "your_resource_name" | LIKE "name_matcher"]]
            [RESOURCETYPE = ["SPARK"]]
        ]
        [ORDER BY ...]
        [LIMIT limit][OFFSET offset];
        
    Explain:
        1) If use NAME LIKE, the name of resource is matched to show.
        2) If use NAME =, the specified name is exactly matched.
        3) RESOURCETYPE is specified, the corresponding rerouce type is matched.
        4) Use ORDER BY to sort any combination of columns.
        5) If LIMIT is specified, limit matching records are displayed. Otherwise, it is all displayed.
        6) If OFFSET is specified, the query results are displayed starting with the offset offset. The offset is 0 by default.

## example
    1. Display all resources that the current user has permissions on
        SHOW RESOURCES;
    
    2. Show the specified resource, the name contains the string "20140102", and displays 10 properties
        SHOW RESOURCES WHERE NAME LIKE "2014_01_02" LIMIT 10;
        
    3. Display the specified resource, specify the name as "20140102" and sort in descending order by key
        SHOW RESOURCES WHERE NAME = "20140102" ORDER BY `KEY` DESC;


## keyword
    SHOW, RESOURCES

