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

# ADMIN SHOW REPLICA DISTRIBUTION
## Description

This statement is used to show the distribution status of a table or partition replica

Grammar:

ADMIN SHOW REPLICA DISTRIBUTION FROM [db_name.]tbl_name [PARTITION (p1, ...)];

Explain:

The Graph column in the result shows the distribution ratio of replicas graphically

## example

1. View the distribution of replicas of tables

ADMIN SHOW REPLICA DISTRIBUTION FROM tbl1;

2. View the distribution of copies of partitions in the table

ADMIN SHOW REPLICA DISTRIBUTION FROM db1.tbl1 PARTITION(p1, p2);

## keyword
ADMIN,SHOW,REPLICA,DISTRIBUTION
