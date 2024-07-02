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

suite ("dup_mv_year") {
    sql """ DROP TABLE IF EXISTS dup_mv_year; """

    sql """
            create table dup_mv_year(
                k1 int null,
                k2 dateV2 null,
                k3 datetime null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into dup_mv_year select 1,'2003-12-31','2003-12-31 01:02:03';"
    sql "insert into dup_mv_year select 2,'2013-12-31','2013-12-31 01:02:03';"
    sql "insert into dup_mv_year select 3,'2023-12-31','2023-12-31 01:02:03';"

    createMV "create materialized view k12y as select k1,year(k2) from dup_mv_year;"
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "analyze table dup_mv_year with sync;"
    sql """set enable_stats=false;"""


    explain {
        sql("select k1,year(k2) from dup_mv_year order by k1;")
        contains "(k12y)"
    }
    order_qt_select_mv "select k1,year(k2) from dup_mv_year order by k1;"

    sql """set enable_stats=true;"""
    explain {
        sql("select k1,year(k2) from dup_mv_year order by k1;")
        contains "(k12y)"
    }

    createMV "create materialized view k13y as select k1,year(k3) from dup_mv_year;"

    sql "insert into dup_mv_year select 4,'2033-12-31','2033-12-31 01:02:03';"
    Thread.sleep(1000)

    order_qt_select_star "select * from dup_mv_year order by k1;"

    explain {
        sql("select year(k3) from dup_mv_year order by k1;")
        contains "(k13y)"
    }
    order_qt_select_mv_sub "select year(k3) from dup_mv_year order by k1;"

    sql """set enable_stats=false;"""
    explain {
        sql("select year(k3) from dup_mv_year order by k1;")
        contains "(k13y)"
    }
}
