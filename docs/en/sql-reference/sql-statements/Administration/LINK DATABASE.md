---
{
    "title": "LINK DATABASE",
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

# LINK DATABASE
## Description

This statement allows users to link a database of one logical cluster to another logical cluster. A database is only allowed to be linked once at the same time and the linked database is deleted.

It does not delete data, and the linked database cannot be deleted. Administrator privileges are required.

grammar

LINK DATABASE src u cluster name.src db name of the cluster name.des db name

## example

1. Link test_db in test_cluster A to test_cluster B and name it link_test_db

LINK DATABASE test_clusterA.test_db test_clusterB.link_test_db;

2. Delete linked database link_test_db

DROP DATABASE link_test_db;

## keyword
LINK,DATABASE
