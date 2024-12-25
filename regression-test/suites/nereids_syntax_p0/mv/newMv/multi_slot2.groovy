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

suite ("multi_slot2") {
    sql """ DROP TABLE IF EXISTS multi_slot2; """

    sql """
            create table multi_slot2(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into multi_slot2 select 1,1,1,'a';"
    sql "insert into multi_slot2 select 2,2,2,'b';"
    sql "insert into multi_slot2 select 3,-3,null,'c';"

    boolean createFail = false;
    try {
        sql "create materialized view k1a2p2ap3ps as select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from multi_slot2 group by abs(k1)+k2;"
    } catch (Exception e) {
        createFail = true;
    }
    assertTrue(createFail);

    createMV ("create materialized view k1a2p2ap3ps as select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from multi_slot2 group by abs(k1)+k2+1;")

    sleep(3000)
    sql "insert into multi_slot2 select -4,-4,-4,'d';"
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "analyze table multi_slot2 with sync;"
    sql """set enable_stats=false;"""


    order_qt_select_star "select * from multi_slot2 order by k1;"

    mv_rewrite_success("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from multi_slot2 group by abs(k1)+k2+1 order by abs(k1)+k2+1",
            "k1a2p2ap3ps")
    order_qt_select_mv "select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from multi_slot2 group by abs(k1)+k2+1 order by abs(k1)+k2+1;"

    mv_rewrite_fail("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from multi_slot2 group by abs(k1)+k2 order by abs(k1)+k2", "k1a2p2ap3ps")
    order_qt_select_base "select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from multi_slot2 group by abs(k1)+k2 order by abs(k1)+k2;"

    sql """set enable_stats=true;"""
    sql """alter table multi_slot2 modify column k1 set stats ('row_count'='4');"""
    mv_rewrite_success("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from multi_slot2 group by abs(k1)+k2+1 order by abs(k1)+k2+1",
            "k1a2p2ap3ps")

    mv_rewrite_fail("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from multi_slot2 group by abs(k1)+k2 order by abs(k1)+k2", "k1a2p2ap3ps")

}
