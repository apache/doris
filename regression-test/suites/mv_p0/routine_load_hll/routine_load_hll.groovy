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

suite ("routine_load_hll") {

    sql """ DROP TABLE IF EXISTS test; """

    sql """
        CREATE TABLE `test` (
            `mv_event_id` varchar(50) NULL,
            `mv_time_stamp` datetime NULL,
            `mv_device_id` hll hll_union
            ) ENGINE=OLAP
            AGGREGATE KEY(`mv_event_id`,`mv_time_stamp`)
            DISTRIBUTED BY HASH(`mv_event_id`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); 
        """

    sql """insert into test(mv_event_id,mv_time_stamp,mv_device_id) values('ad_sdk_request','2024-03-04 00:00:00',hll_hash('a'));"""

    createMV("""create materialized view m_view as select mv_time_stamp as m_view_mv_time_stamp, hll_union(mv_device_id) from test group by mv_time_stamp;""")

        sql """insert into test(mv_event_id,mv_time_stamp,mv_device_id) values('ad_sdk_request','2024-03-04 00:00:00',hll_hash('b'));"""

    streamLoad {
        table "test"
        set 'column_separator', ','
        set 'columns', 'mv_event_id,mv_time_stamp,mv_device_id,mv_device_id=hll_hash(mv_device_id)'

        file './test'
        time 10000 // limit inflight 10s
    }

    qt_select "select mv_event_id,mv_time_stamp,hll_cardinality(mv_device_id) from test order by 1,2;"

    sql "analyze table test with sync;"
    sql """alter table test modify column mv_event_id set stats ('row_count'='2');"""
    sql """set enable_stats=false;"""

    mv_rewrite_success("select mv_time_stamp, hll_union_agg(mv_device_id) from test group by mv_time_stamp order by 1;", "m_view")
    qt_select_mv "select mv_time_stamp, hll_union_agg(mv_device_id) from test group by mv_time_stamp order by 1;"

    sql """set enable_stats=true;"""
    mv_rewrite_success("select mv_time_stamp, hll_union_agg(mv_device_id) from test group by mv_time_stamp order by 1;", "m_view")
}
