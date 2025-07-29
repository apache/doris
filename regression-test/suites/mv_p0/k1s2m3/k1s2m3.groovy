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

suite ("k1s2m3") {
    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table(
                mv_k1 int null,
                mv_k2 int not null,
                mv_k3 bigint null,
                mv_k4 varchar(100) null
            )
            duplicate key (mv_k1,mv_k2,mv_k3)
            distributed BY hash(mv_k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,-3,null,'c';"

    sql """alter table d_table modify column mv_k1 set stats ('row_count'='6');"""
    createMV("create materialized view k1s2m3 as select mv_k1,sum(mv_k2*mv_k3) from d_table group by mv_k1;")

    sql "insert into d_table select -4,-4,-4,'d';"
    sql "insert into d_table(mv_k4,mv_k2) values('d',4);"

    sql "analyze table d_table with sync;"

    qt_select_star "select * from d_table order by mv_k1;"

    // support lower case and upper case: mv_k1, K1
    mv_rewrite_success("select mv_k1,sum(mv_K2*mv_k3) from d_table group by mv_K1 order by mv_K1;", "k1s2m3")
    
    qt_select_mv1 "select mv_K1,sum(mv_k2*mv_k3) from d_table group by mv_K1 order by mv_k1;"
    

    sql "delete from d_table where mv_k1=1;"

    sql "analyze table d_table"

    mv_rewrite_success("select mv_k1,sum(mv_k2*mv_k3) from d_table group by mv_k1 order by mv_k1;", "k1s2m3")

    qt_select_mv2 "select mv_k1,sum(mv_k2*mv_k3) from d_table group by mv_k1 order by mv_k1;"

    createMV("create materialized view kdup321 as select mv_k3,mv_k2,mv_k1 from d_table;")
    
    sql "analyze table d_table"
    // kdup321 prefix index
    mv_rewrite_success("select count(mv_k2) from d_table where mv_k3 = 1;", "kdup321")
    
    qt_select_mv6 "select count(mv_k2) from d_table where mv_k3 = 1;"

    qt_select_star "select * from d_table order by mv_k1;"

    test {
        sql "create materialized view k1s2m3 as select mv_K1,sum(mv_k2*mv_k3)+1 from d_table group by mv_k1;"
        exception "cannot be included outside aggregate"
    }
    test {
        sql "create materialized view k1s2m3 as select mv_K1,abs(sum(mv_k2*mv_k3)+1) from d_table group by mv_k1;"
        exception "cannot be included outside aggregate"
    }
    test {
        sql "create materialized view k1s2m3 as select mv_K1,sum(abs(sum(mv_k2*mv_k3)+1)) from d_table group by mv_k1;"
        exception "aggregate function cannot contain aggregate parameters"
    }
}
