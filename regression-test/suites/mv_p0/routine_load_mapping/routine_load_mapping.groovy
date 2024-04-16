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

suite ("routine_load_mapping") {

    sql """ DROP TABLE IF EXISTS test; """

    sql """
        CREATE TABLE `test` (
            `event_id` varchar(50) NULL COMMENT '',
            `time_stamp` datetime NULL COMMENT '',
            `device_id` varchar(150) NULL DEFAULT "" COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`event_id`)
            DISTRIBUTED BY HASH(`device_id`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); 
        """

    sql """insert into test(event_id,time_stamp,device_id) values('ad_sdk_request','2024-03-04 00:00:00','a');"""

    createMV("""create materialized view m_view as select time_stamp, count(device_id) from test group by time_stamp;""")

    streamLoad {
        table "test"
        set 'column_separator', ','
        set 'columns', 'event_id,time_stamp,device_id,time_stamp=from_unixtime(`time_stamp`)'

        file './test'
        time 10000 // limit inflight 10s
    }

    qt_select "select * from test order by 1,2,3;"
    qt_select_mv "select * from test index m_view order by 1,2;"
}
