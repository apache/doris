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

suite ("test_base") {
    sql """set enable_nereids_planner=true"""
    sql """SET enable_fallback_to_original_planner=false"""
    sql """ drop table if exists dwd;"""

    sql """
        CREATE TABLE `dwd` (
            `id` bigint(20) NULL COMMENT 'id',
            `created_at` datetime NULL,
            `dt` date NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """insert into dwd(id) values(1);"""

    createMV ("""
            create materialized view dwd_mv as SELECT created_at, id FROM dwd;
    """)

    sql """insert into dwd(id) values(2);"""

    sql "analyze table dwd with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("SELECT created_at, id  FROM dwd where created_at = '2020-09-09 00:00:00' order by 1, 2;")
        contains "(dwd_mv)"
    }
    qt_select_mv "SELECT created_at, id FROM dwd order by 1, 2;"

    explain {
        sql("SELECT id,created_at  FROM dwd where id is not null order by 1, 2;")
        contains "(dwd)"
    }
    qt_select_mv "SELECT id,created_at FROM dwd order by 1, 2;"

    sql """set enable_stats=true;"""
    explain {
        sql("SELECT created_at, id  FROM dwd where created_at = '2020-09-09 00:00:00' order by 1, 2;")
        contains "(dwd_mv)"
    }

    explain {
        sql("SELECT id,created_at  FROM dwd where id is not null order by 1, 2;")
        contains "(dwd)"
    }
}
