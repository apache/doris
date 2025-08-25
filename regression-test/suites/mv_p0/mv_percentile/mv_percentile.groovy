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

suite ("mv_percentile") {
    sql "set enable_fallback_to_original_planner = false"

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    // this mv rewrite would not be rewritten in RBO phase, so set TRY_IN_RBO explicitly to make case stable
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"

    sql """DROP TABLE IF EXISTS d_table;"""

    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,-3,null,'c';"
    sql "insert into d_table select 3,-3,null,'c';"
    sql "insert into d_table select 3,-3,null,'c';"


    create_sync_mv(db,
            "d_table", "kp",
            "select k1 as a1,k2 as a2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by k1,k2;")

    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table(k4,k2) values('d',4);"

    sql "analyze table d_table with sync;"
    sql """alter table d_table modify column k1 set stats ('row_count'='15');"""
    sql """set enable_stats=false;"""

    qt_select_star "select * from d_table order by k1;"

    mv_rewrite_success("select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by k1,k2 order by k1,k2;",
            "kp", true, [TRY_IN_RBO, FORCE_IN_RBO])
    mv_rewrite_fail("select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by k1,k2 order by k1,k2;",
            "kp", [NOT_IN_RBO])
    qt_select_mv "select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by k1,k2 order by k1,k2;"

    mv_rewrite_success("select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by grouping sets((k1),(k1,k2),()) order by 1,2;",
            "kp", true, [TRY_IN_RBO, FORCE_IN_RBO])
    mv_rewrite_fail("select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by grouping sets((k1),(k1,k2),()) order by 1,2;",
            "kp", [NOT_IN_RBO])
    qt_select_mv "select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by grouping sets((k1),(k1,k2),()) order by 1,2,3;"

    mv_rewrite_success("select percentile(k3, 0.1) from d_table group by grouping sets((k1),()) order by 1;", "kp")
    qt_select_mv "select percentile(k3, 0.1) from d_table group by grouping sets((k1),()) order by 1;"

    sql """set enable_stats=true;"""

    mv_rewrite_success("select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by k1,k2 order by k1,k2;",
            "kp", true, [TRY_IN_RBO, FORCE_IN_RBO])
    mv_rewrite_fail("select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by k1,k2 order by k1,k2;",
            "kp", [NOT_IN_RBO])

    mv_rewrite_success("select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by grouping sets((k1),(k1,k2),()) order by 1,2;",
            "kp", true, [TRY_IN_RBO, FORCE_IN_RBO])
    mv_rewrite_fail("select k1,k2,percentile(k3, 0.1),percentile(k3, 0.9) from d_table group by grouping sets((k1),(k1,k2),()) order by 1,2;",
            "kp", [NOT_IN_RBO])

    mv_rewrite_success("select percentile(k3, 0.1) from d_table group by grouping sets((k1),()) order by 1;", "kp")
}
