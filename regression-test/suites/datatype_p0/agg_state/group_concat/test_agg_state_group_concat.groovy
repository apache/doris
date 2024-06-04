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

suite("test_agg_state_group_concat") {
    sql "set global enable_agg_state=true"
    sql """ DROP TABLE IF EXISTS a_table; """
    sql """
            create table a_table(
                k1 int null,
                k2 agg_state<group_concat(string)> generic
            )
            aggregate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into a_table select 1,group_concat_state('a');"
    sql "insert into a_table select 1,group_concat_state('bb');"
    sql "insert into a_table values(1,group_concat_state('ccc'));"

    qt_length1 """select k1,length(k2) from a_table order by k1;"""
    qt_group1 """select k1,group_concat_merge(k2) from a_table group by k1 order by k1;"""
    qt_merge1 """select group_concat_merge(k2) from a_table;"""
    
    qt_length2 """select k1,length(k2) from a_table order by k1;"""
    qt_group2 """select k1,group_concat_merge(k2) from a_table group by k1 order by k1;"""
    qt_merge2 """select group_concat_merge(k2) from a_table;"""
    
    qt_union """ select group_concat_merge(kstate) from (select k1,group_concat_union(k2) kstate from a_table group by k1 order by k1) t; """
}
