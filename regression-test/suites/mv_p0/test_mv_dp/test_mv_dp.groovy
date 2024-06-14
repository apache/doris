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

suite ("test_mv_dp") {

    sql """ DROP TABLE IF EXISTS dp; """

    sql """
        CREATE TABLE dp (
        `d` int NULL ,
        `status` text NULL ,
        `uid_list` array<text> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`d`)
        DISTRIBUTED BY HASH(`d`) BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """INSERT INTO `dp` VALUES (1,'success',["1","2"]),(2,'fail',["1"]);"""

    createMV("""CREATE MATERIALIZED VIEW view_2 as
                    select d,
                        bitmap_union(bitmap_from_array(cast(uid_list as array<bigint>))),
                        bitmap_union(bitmap_from_array(if(status='success', cast(uid_list as array<bigint>), array())))
                    from dp
                    group by d;""")

    sql """INSERT INTO `dp` VALUES (1,'success',["3","4"]),(2,'success',["5"]);"""
    sql "analyze table dp with sync;"
    sql """set enable_stats=false;"""
/*
    streamLoad {
        table "test"

        set 'columns', 'date'

        file './test'
        time 10000 // limit inflight 10s
    }
*/
    explain {
        sql("""select d,
                        bitmap_union_count(bitmap_from_array(cast(uid_list as array<bigint>))),
                        bitmap_union_count(bitmap_from_array(if(status='success', cast(uid_list as array<bigint>), array())))
                    from dp
                    group by d;""")
        contains "(view_2)"
    }

    qt_select_mv """select d,
                        bitmap_union_count(bitmap_from_array(cast(uid_list as array<bigint>))),
                        bitmap_union_count(bitmap_from_array(if(status='success', cast(uid_list as array<bigint>), array())))
                    from dp
                    group by d order by 1;"""
    sql """set enable_stats=true;"""
    explain {
        sql("""select d,
                        bitmap_union_count(bitmap_from_array(cast(uid_list as array<bigint>))),
                        bitmap_union_count(bitmap_from_array(if(status='success', cast(uid_list as array<bigint>), array())))
                    from dp
                    group by d;""")
        contains "(view_2)"
    }
}
