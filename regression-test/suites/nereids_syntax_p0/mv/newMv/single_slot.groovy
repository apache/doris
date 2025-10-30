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

suite ("single_slot") {
    // this mv rewrite would not be rewritten in RBO phase, so set TRY_IN_RBO explicitly to make case stable
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"
    sql """ DROP TABLE IF EXISTS single_slot; """

    sql """
            create table single_slot(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into single_slot select 1,2,1,'a';"
    sql "insert into single_slot select 1,3,2,'b';"
    sql "insert into single_slot select 2,5,null,'c';"

    createMV("create materialized view k1ap2spa as select abs(k1)+1,sum(abs(k2+1)) from single_slot group by abs(k1)+1;")

    sleep(3000)

    sql "insert into single_slot select 2,-4,-4,'d';"

    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "analyze table single_slot with sync;"
    sql """alter table single_slot modify column k1 set stats ('row_count'='4');"""

    sql """set enable_stats=false;"""

    order_qt_select_star "select * from single_slot order by k1;"

    mv_rewrite_success("select abs(k1)+1 t,sum(abs(k2+1)) from single_slot group by t order by t;",
            "k1ap2spa", true, [TRY_IN_RBO, NOT_IN_RBO])
    mv_rewrite_success_without_check_chosen("select abs(k1)+1 t,sum(abs(k2+1)) from single_slot group by t order by t;",
            "k1ap2spa", [FORCE_IN_RBO])

    mv_rewrite_success("select abs(k1)+1 t,sum(abs(k2+1)) from single_slot group by t order by t;",
            "k1ap2spa", true, [TRY_IN_RBO, NOT_IN_RBO])
    mv_rewrite_success_without_check_chosen("select abs(k1)+1 t,sum(abs(k2+1)) from single_slot group by t order by t;",
            "k1ap2spa", [FORCE_IN_RBO])
    order_qt_select_mv "select abs(k1)+1 t,sum(abs(k2+1)) from single_slot group by t order by t;"

    sql """set enable_stats=true;"""
    mv_rewrite_success("select abs(k1)+1 t,sum(abs(k2+1)) from single_slot group by t order by t;",
            "k1ap2spa", true, [TRY_IN_RBO, NOT_IN_RBO])
    mv_rewrite_success_without_check_chosen("select abs(k1)+1 t,sum(abs(k2+1)) from single_slot group by t order by t;",
            "k1ap2spa", [FORCE_IN_RBO])
}
