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

suite ("test_dup_mv_year") {

    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table(
                k1 int null,
                k2 date null,
                k3 datetime null
            )
            duplicate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,'2003-12-31','2003-12-31 01:02:03';"
    sql "insert into d_table select 2,'2013-12-31','2013-12-31 01:02:03';"
    sql "insert into d_table select 3,'2023-12-31','2023-12-31 01:02:03';"

    createMV "create materialized view k12y as select k1,year(k2) from d_table;"

    sql """analyze table d_table with sync;"""
    sql """set enable_stats=false;"""

    mv_rewrite_success("select k1,year(k2) from d_table order by k1;", "k12y")
    qt_select_mv "select k1,year(k2) from d_table order by k1;"

    sql """set enable_stats=true;"""
    sql """alter table d_table modify column k1 set stats ('row_count'='4');"""
    mv_rewrite_success("select k1,year(k2) from d_table order by k1;", "k12y")

    createMV "create materialized view k13y as select k1,year(k3) from d_table;"

    sql "insert into d_table select 4,'2033-12-31','2033-12-31 01:02:03';"

    qt_select_star "select * from d_table order by k1;"

    mv_rewrite_success("select year(k3) from d_table order by k1;", "k13y")
    qt_select_mv_sub "select year(k3) from d_table order by k1;"

    sql """set enable_stats=false;"""
    mv_rewrite_success("select year(k3) from d_table order by k1;", "k13y")
}
