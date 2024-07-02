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
            `event_id` varchar(50) NULL,
            `time_stamp` datetime NULL,
            `device_id` hll hll_union
            ) ENGINE=OLAP
            AGGREGATE KEY(`event_id`,`time_stamp`)
            DISTRIBUTED BY HASH(`event_id`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); 
        """

    sql """insert into test(event_id,time_stamp,device_id) values('ad_sdk_request','2024-03-04 00:00:00',hll_hash('a'));"""

    createMV("""create materialized view m_view as select time_stamp, hll_union(device_id) from test group by time_stamp;""")

        sql """insert into test(event_id,time_stamp,device_id) values('ad_sdk_request','2024-03-04 00:00:00',hll_hash('b'));"""

    streamLoad {
        table "test"
        set 'column_separator', ','
        set 'columns', 'event_id,time_stamp,device_id,device_id=hll_hash(device_id)'

        file './test'
        time 10000 // limit inflight 10s
    }

    qt_select "select event_id,time_stamp,hll_cardinality(device_id) from test order by 1,2;"

    sql "analyze table test with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("select time_stamp, hll_union_agg(device_id) from test group by time_stamp order by 1;")
        contains "(m_view)"
    }
    qt_select_mv "select time_stamp, hll_union_agg(device_id) from test group by time_stamp order by 1;"

    sql """set enable_stats=true;"""
    explain {
        sql("select time_stamp, hll_union_agg(device_id) from test group by time_stamp order by 1;")
        contains "(m_view)"
    }
}
