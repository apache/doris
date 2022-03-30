---
{
    "title": "MIGRATE DATABASE",
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

# MIGRATE DATABASE
## Description

This statement is used to migrate a logical cluster database to another logical cluster. Before performing this operation, the database must be in a link state and need to be managed.

Membership authority

grammar

MIGRATE DATABASE src u cluster name.src db name of the cluster name.des db name

## example

1. 迁移test_clusterA中的test_db到test_clusterB

MIGRATE DATABASE test_clusterA.test_db test_clusterB.link_test_db;

## keyword
MIGRATE,DATABASE
