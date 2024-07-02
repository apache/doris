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

suite ("test_dup_mv_repeat") {

    sql """ DROP TABLE IF EXISTS db1; """

    sql """
            CREATE TABLE `db1` (
            `dt` date NULL,
            `s` varchar(128) NULL,
            `n` bigint NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`dt`)
            DISTRIBUTED BY HASH(`dt`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
        """

    sql "insert into db1 values('2020-01-01','abc',123),('2020-01-02','def',456);"

    createMV ("create materialized view dbviwe as select dt,s,sum(n) as n from db1 group by dt,s;")

    sql "analyze table db1 with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("SELECT s AS s, sum(n) / count(DISTINCT dt) AS n FROM  db1 GROUP BY  GROUPING SETS((s)) order by 1;")
        contains "(dbviwe)"
    }
    qt_select_mv "SELECT s AS s, sum(n) / count(DISTINCT dt) AS n FROM  db1 GROUP BY  GROUPING SETS((s)) order by 1;"

    sql """set enable_stats=true;"""
    explain {
        sql("SELECT s AS s, sum(n) / count(DISTINCT dt) AS n FROM  db1 GROUP BY  GROUPING SETS((s)) order by 1;")
        contains "(dbviwe)"
    }
}
