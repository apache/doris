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

suite ("test_doc_e4") {

    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 date null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'2020-02-20';"
    sql "insert into d_table select 2,2,2,'2021-02-20';"

    createMV ("create materialized view k1a2p2ap3ps as select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1;")
    createMV ("create materialized view kymd as select year(k4),month(k4) from d_table where year(k4) = 2020;")

    sql "insert into d_table select 3,-3,null,'2022-02-20';"

    sql """analyze table d_table with sync;"""
    sql """set enable_stats=false;"""

    mv_rewrite_success("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1 order by 1,2;", "k1a2p2ap3ps")
    qt_select_mv "select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1 order by 1,2;"

    mv_rewrite_success("select bin(abs(k1)+k2+1),sum(abs(k2+2)+k3+3) from d_table group by bin(abs(k1)+k2+1);", "k1a2p2ap3ps")
    qt_select_mv "select bin(abs(k1)+k2+1),sum(abs(k2+2)+k3+3) from d_table group by bin(abs(k1)+k2+1) order by 1,2;"

    mv_rewrite_all_fail("select year(k4),month(k4) from d_table;", ["k1a2p2ap3ps", "kymd"])
    qt_select_mv "select year(k4),month(k4) from d_table order by 1,2;"

    mv_rewrite_success("select year(k4)+month(k4) from d_table where year(k4) = 2020;", "kymd")
    qt_select_mv "select year(k4)+month(k4) from d_table where year(k4) = 2020 order by 1;"

    sql """set enable_stats=true;"""
    sql """alter table d_table modify column k1 set stats ('row_count'='3');"""
    mv_rewrite_success("select abs(k1)+k2+1,sum(abs(k2+2)+k3+3) from d_table group by abs(k1)+k2+1 order by 1,2;", "k1a2p2ap3ps")

    mv_rewrite_success("select bin(abs(k1)+k2+1),sum(abs(k2+2)+k3+3) from d_table group by bin(abs(k1)+k2+1);", "k1a2p2ap3ps")

    mv_rewrite_all_fail("select year(k4),month(k4) from d_table;", ["k1a2p2ap3ps", "kymd"])

    mv_rewrite_success("select year(k4)+month(k4) from d_table where year(k4) = 2020;", "kymd")
}
