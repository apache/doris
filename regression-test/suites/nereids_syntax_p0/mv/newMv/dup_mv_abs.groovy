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

suite ("dup_mv_abs") {
    sql """ DROP TABLE IF EXISTS dup_mv_abs; """

    sql """
            create table dup_mv_abs(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into dup_mv_abs select 1,1,1,'a';"
    sql "insert into dup_mv_abs select 2,2,2,'b';"
    sql "insert into dup_mv_abs select 3,-3,null,'c';"

    createMV ("create materialized view k12a as select k1,abs(k2) from dup_mv_abs;")
    sleep(3000)

    sql "insert into dup_mv_abs select -4,-4,-4,'d';"

    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"


    order_qt_select_star "select * from dup_mv_abs order by k1;"

    explain {
        sql("select k1,abs(k2) from dup_mv_abs order by k1;")
        contains "(k12a)"
    }
    order_qt_select_mv "select k1,abs(k2) from dup_mv_abs order by k1;"

    explain {
        sql("select abs(k2) from dup_mv_abs order by k1;")
        contains "(k12a)"
    }
    order_qt_select_mv_sub "select abs(k2) from dup_mv_abs order by k1;"

    explain {
        sql("select abs(k2)+1 from dup_mv_abs order by k1;")
        contains "(k12a)"
    }
    order_qt_select_mv_sub_add "select abs(k2)+1 from dup_mv_abs order by k1;"

    explain {
        sql("select sum(abs(k2)) from dup_mv_abs group by k1 order by k1;")
        contains "(k12a)"
    }
    order_qt_select_group_mv "select sum(abs(k2)) from dup_mv_abs group by k1 order by k1;"

    explain {
        sql("select sum(abs(k2)+1) from dup_mv_abs group by k1 order by k1;")
        contains "(k12a)"
    }
    order_qt_select_group_mv_add "select sum(abs(k2)+1) from dup_mv_abs group by k1 order by k1;"

    explain {
        sql("select sum(abs(k2)) from dup_mv_abs group by k3;")
        contains "(dup_mv_abs)"
    }
    order_qt_select_group_mv_not "select sum(abs(k2)) from dup_mv_abs group by k3 order by k3;"
}
