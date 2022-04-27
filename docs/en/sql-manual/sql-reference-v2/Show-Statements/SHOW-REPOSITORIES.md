---
{
    "title": "SHOW-REPOSITORIES",
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

## SHOW-REPOSITORIES

### Name

SHOW REPOSITORIES

### Description

This statement is used to view the currently created warehouse

grammar:

```sql
SHOW REPOSITORIES;
````

illustrate:

1. The meanings of the columns are as follows:
        RepoId: Unique repository ID
        RepoName: repository name
        CreateTime: The time when the repository was first created
        IsReadOnly: Whether it is a read-only repository
        Location: The root directory in the warehouse for backing up data
        Broker: Dependent Broker
        ErrMsg: Doris will regularly check the connectivity of the warehouse, if there is a problem, an error message will be displayed here

### Example

1. View the created repository:

```sql
  SHOW REPOSITORIES;
````

### Keywords

    SHOW, REPOSITORIES

### Best Practice

