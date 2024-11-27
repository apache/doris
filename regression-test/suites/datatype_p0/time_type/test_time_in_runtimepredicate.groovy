
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

suite("test_time_in_runtimepredicate") {
    def tbName = "test_time_in_runtimepredicate"
    sql """ DROP TABLE IF EXISTS test_time_in_runtimepredicate """
    sql """
       create table test_time_in_runtimepredicate(a date, b datetime, c int) properties ("replication_allocation" = "tag.location.default: 1");
      """
    sql """insert into test_time_in_runtimepredicate values ("2023-12-18", "2023-12-18 01:47:22", 1),  ("2023-12-18", "2023-12-18 02:47:22", 2) ,  ("2023-12-17", "2023-12-18 00:47:22", 3), ("2023-12-20", "2023-12-18 00:47:22", 4) , ("2023-12-20", "2023-12-18 00:47:22", 5);"""


    qt_sql1 "select  timediff(a, b) as t, count(c) from test_time_in_runtimepredicate group by t order by t;"
    qt_sql2 "select  timediff(a, b) as t, count(c) from test_time_in_runtimepredicate group by t order by t limit 3;"
}
