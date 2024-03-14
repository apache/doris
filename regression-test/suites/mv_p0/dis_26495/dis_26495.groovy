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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("dis_26495") {
    sql "set enable_agg_state=true"
    sql """ DROP TABLE IF EXISTS doris_test; """

    sql """
        create table doris_test (a int,b int, agg_st_1 agg_state<max_by(int ,int)> generic)
            DISTRIBUTED BY HASH(a) BUCKETS 1 properties("replication_num" = "1");
        """

    sql """insert into doris_test values (1,2,max_by_state(1,2));"""

    streamLoad {
        table "doris_test"
        set 'column_separator', ','
        set 'columns', 'a, b, agg_st_1=max_by_state(a,b)'

        file './test.csv'
        time 10000 // limit inflight 10s
    }

    qt_select """select max_by_merge(agg_st_1) from doris_test"""
    qt_select """select a,b,max_by_merge(agg_st_1) from doris_test group by a,b order by a,b"""
}
