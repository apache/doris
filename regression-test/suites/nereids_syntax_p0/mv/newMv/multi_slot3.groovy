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

suite ("multi_slot3") {
    sql """ DROP TABLE IF EXISTS multi_slot3; """

    sql """
            create table multi_slot3(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into multi_slot3 select 1,1,1,'a';"
    sql "insert into multi_slot3 select 2,2,2,'b';"
    sql "insert into multi_slot3 select 3,-3,null,'c';"

    createMV ("create materialized view k1p2ap3p as select k1+1,abs(k2+2)+k3+3 from multi_slot3;")

    sleep(3000)

    sql "insert into multi_slot3 select -4,-4,-4,'d';"
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "analyze table multi_slot3 with sync;"
    sql """set enable_stats=false;"""

    order_qt_select_star "select * from multi_slot3 order by k1;"

    mv_rewrite_success("select k1+1,abs(k2+2)+k3+3 from multi_slot3 order by k1+1;", "k1p2ap3p")
    order_qt_select_mv "select k1+1,abs(k2+2)+k3+3 from multi_slot3 order by k1+1;"

    sql """set enable_stats=true;"""
    sql """alter table multi_slot3 modify column k1 set stats ('row_count'='4');"""
    mv_rewrite_success("select k1+1,abs(k2+2)+k3+3 from multi_slot3 order by k1+1;", "k1p2ap3p")
}
