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

suite ("test_upper_alias") {
    sql """set enable_nereids_planner=true"""
    sql """SET enable_fallback_to_original_planner=false"""
    sql """ drop table if exists test_0401;"""

    sql """
       CREATE TABLE test_0401 (
        `d_b` varchar(128) NULL,
        `d_a` varchar(128) NULL,
        `amt_b0` double NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`d_b`)
        DISTRIBUTED BY HASH(`d_b`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """insert into test_0401 values('xxx', 'wfsdf', 9.30 );"""

    createMV ("""
        create materialized view test_0401_mv as 
        select d_b, sum(amt_b0) as amt_b0 from test_0401 group by d_b;
    """)

    createMV ("""
        create materialized view test_0401_mv2 as 
        select d_a,d_b from test_0401;
    """)

    sql """insert into test_0401 values('yyy', 'wfsdf', 91.310 );"""

    explain {
        sql("SELECT upper(d_b) AS d_b FROM test_0401 GROUP BY upper(d_b) order by 1;")
        contains "(test_0401_mv)"
    }
    qt_select_mv "SELECT upper(d_b) AS d_b FROM test_0401 GROUP BY upper(d_b) order by 1;"

    explain {
        sql("SELECT upper(d_b) AS d_bb FROM test_0401 GROUP BY upper(d_b) order by 1;")
        contains "(test_0401_mv)"
    }
    qt_select_mv "SELECT upper(d_b) AS d_bb FROM test_0401 GROUP BY upper(d_b) order by 1;"

    explain {
        sql("SELECT d_a AS d_b FROM test_0401 order by 1;")
        contains "(test_0401_mv2)"
    }
    qt_select_mv "SELECT d_a AS d_b FROM test_0401 order by 1;"
}
