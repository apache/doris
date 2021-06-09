---
{
    "title": "ALTER CLUSTER",
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

# ALTER CLUSTER
## description

This statement is used to update the logical cluster. Administrator privileges are required

grammar

ALTER CLUSTER cluster_name PROPERTIES ("key"="value", ...);

1. Scaling, scaling (according to the number of be existing in the cluster, large is scaling, small is scaling), scaling for synchronous operation, scaling for asynchronous operation, through the state of backend can be known whether the scaling is completed. 

## example

1. Reduce the number of be of logical cluster test_cluster containing 3 be by 2.

ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="2");

2. Expansion, increase the number of be of logical cluster test_cluster containing 3 be to 4

ALTER CLUSTER test_cluster PROPERTIES ("instance_num"="4");

## keyword
ALTER,CLUSTER
