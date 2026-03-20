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

suite("test_agg_state_schema_change") {
    sql "set global enable_agg_state=true"
    sql """ DROP TABLE IF EXISTS a_table; """
    sql """
          create table a_table(
                k1 int null,
                k2 agg_state<max_by(int not null,int)> generic not null,
                k3 agg_state<group_concat(string)> generic not null
          )
          aggregate key (k1)
          distributed BY hash(k1) buckets 1
          properties("replication_num" = "1");
        """

    sql "insert into a_table values(1,max_by_state(3,1),group_concat_state('a'));"
    sql "insert into a_table values(1,max_by_state(2,2),group_concat_state('bb'));"
    sql "insert into a_table values(2,max_by_state(1,3),group_concat_state('ccc'));"

    qt_sql1 """ select k1,max_by_merge(k2),group_concat_merge(k3) from a_table group by k1 order by k1; """

    sql """ ALTER TABLE a_table ADD COLUMN k4 agg_state<max_by(int not null,int)> GENERIC NULL; """

    sql """ insert into a_table values(3,max_by_state(7,5),group_concat_state('ceeee'),max_by_state(1,3)); """

    qt_sql2 """ select k1,max_by_merge(k2),group_concat_merge(k3),max_by_merge(k4) from a_table group by k1 order by k1; """

    sql """ DROP TABLE IF EXISTS a_table; """
}
