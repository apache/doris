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

suite ("case_ignore") {
    sql """ DROP TABLE IF EXISTS case_ignore; """

    sql """
            create table case_ignore(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into case_ignore select 1,1,1,'a';"
    sql "insert into case_ignore select 2,2,2,'b';"
    sql "insert into case_ignore select 3,-3,null,'c';"


    createMV ("create materialized view k12a as select K1,abs(K2) from case_ignore;")
    sleep(3000)

    sql "insert into case_ignore select -4,-4,-4,'d';"
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "analyze table case_ignore with sync;"
    sql """set enable_stats=false;"""

    qt_select_star "select * from case_ignore order by k1;"

    mv_rewrite_success("select k1,abs(k2) from case_ignore order by k1;", "k12a")
    order_qt_select_mv "select k1,abs(k2) from case_ignore order by k1;"

    mv_rewrite_success("select K1,abs(K2) from case_ignore order by K1;", "k12a")
    order_qt_select_mv "select K1,abs(K2) from case_ignore order by K1;"

    sql """set enable_stats=true;"""
    sql """alter table case_ignore modify column k1 set stats ('row_count'='4');"""
    mv_rewrite_success("select k1,abs(k2) from case_ignore order by k1;", "k12a")

    mv_rewrite_success("select K1,abs(K2) from case_ignore order by K1;", "k12a")

}
