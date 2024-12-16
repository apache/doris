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

suite ("test_agg_state_max_by") {

    sql """set enable_nereids_planner=true"""

    sql """ DROP TABLE IF EXISTS d_table; """

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
    sql "insert into d_table select 1,2,2,'b';"
    sql "insert into d_table select 1,-3,null,'c';"
    sql "insert into d_table(k4,k2) values('d',4);"

    createMV("create materialized view k1mb as select k1,max_by(k2,k3) from d_table group by k1;")

    sql "insert into d_table select 1,-4,-4,'d';"


    streamLoad {
        table "d_table"
        set 'column_separator', ','
        file './test'
        time 10000 // limit inflight 10s
    }

    streamLoad {
        set 'version', '1'
        set 'sql', """
                    insert into regression_test_mv_p0_agg_state.d_table select * from http_stream
                    ("format"="csv", "column_separator"=",")
                """
        file './test'

        time 10000 // limit inflight 10s
    }

    sql "analyze table d_table with sync;"
    sql """set enable_stats=false;"""

    qt_select_star "select * from d_table order by 1,2;"
    mv_rewrite_success("select k1,max_by(k2,k3) from d_table group by k1 order by 1,2;", "k1mb")
    sql """set enable_stats=true;"""
    sql """alter table d_table modify column k4 set stats ('row_count'='8');"""
    mv_rewrite_success("select k1,max_by(k2,k3) from d_table group by k1 order by 1,2;", "k1mb")
    qt_select_mv "select k1,max_by(k2,k3) from d_table group by k1 order by 1,2;"

    createMV("create materialized view k1mbcp1 as select k1,max_by(k2+k3,abs(k3)) from d_table group by k1;")
    createMV("create materialized view k1mbcp2 as select k1,max_by(k2+k3,k3) from d_table group by k1;")
    createMV("create materialized view k1mbcp3 as select k1,max_by(k2,abs(k3)) from d_table group by k1;")

    sql "insert into d_table(k4,k2) values('d',4);"
    sql "set enable_nereids_dml = true"
    sql "insert into d_table(k4,k2) values('d',4);"

    streamLoad {
        table "d_table"
        set 'column_separator', ','
        file './test'
        time 10000 // limit inflight 10s
    }

    streamLoad {
        set 'version', '1'
        set 'sql', """
                    insert into regression_test_mv_p0_agg_state.d_table select * from http_stream
                    ("format"="csv", "column_separator"=",")
                """
        file './test'

        time 10000 // limit inflight 10s
    }

    qt_select_star "select * from d_table order by 1,2;"

    sql """set enable_stats=true;"""
    sql """alter table d_table modify column k4 set stats ('row_count'='8');"""
    sql "analyze table d_table with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_success("select k1,max_by(k2+k3,abs(k3)) from d_table group by k1 order by 1,2;", "k1mbcp1")
    qt_select_mv "select k1,max_by(k2+k3,k3) from d_table group by k1 order by 1,2;"

    mv_rewrite_success("select k1,max_by(k2+k3,k3) from d_table group by k1 order by 1,2;", "k1mbcp2")
    qt_select_mv "select k1,max_by(k2+k3,k3) from d_table group by k1 order by 1,2;"

    mv_rewrite_success("select k1,max_by(k2,abs(k3)) from d_table group by k1 order by 1,2;", "k1mbcp3")
    qt_select_mv "select k1,max_by(k2,abs(k3)) from d_table group by k1 order by 1,2;"

    sql """set enable_stats=true;"""
    sql """alter table d_table modify column k4 set stats ('row_count'='8');"""
    mv_rewrite_success("select k1,max_by(k2+k3,abs(k3)) from d_table group by k1 order by 1,2;", "k1mbcp1")
    mv_rewrite_success("select k1,max_by(k2+k3,k3) from d_table group by k1 order by 1,2;", "k1mbcp2")
    mv_rewrite_success("select k1,max_by(k2,abs(k3)) from d_table group by k1 order by 1,2;", "k1mbcp3")
}
