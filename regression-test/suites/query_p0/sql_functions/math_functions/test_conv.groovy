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

suite("test_conv", "arrow_flight_sql") {
    qt_select "SELECT CONV(15,10,2)"

    sql """ drop table if exists test_conv; """
    sql """ create table test_conv(
        k1 varchar(16),
        v1 int
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """
    sql """ insert into test_conv values
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1),
        ("100", 1)
    """

    qt_sql_conv1 """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/conv(k1, cast(null as bigint), cast(null as bigint)) from test_conv; """
}

