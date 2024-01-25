// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

global_conf.enable_test="false"
global_conf.url="jdbc:mysql://127.0.0.1:8030/hits?useSSL=false"
global_conf.username="root"
global_conf.password=""
global_conf.enable_pipe="true"
global_conf.enable_group="true"

/*
about query directory
1 All SQL can be placed in one file separated by ";"
2 Placing SQL in multiple files, just like
    q1.sql, q2.sql, q3.sql
*/

ckbench_query.label="ckbench query"
ckbench_query.dir="../query/ckbench_query/"
ckbench_query.c="1"
ckbench_query.i="1"
ckbench_query.group="normal"
ckbench_query.db="hits"

tpch_query.label="tpch query"
tpch_query.dir="../query/tpch_query/"
tpch_query.c="1"
tpch_query.i="1"
tpch_query.group="test"
tpch_query.db="tpch100"


