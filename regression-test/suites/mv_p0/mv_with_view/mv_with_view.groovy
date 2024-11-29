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

suite ("mv_with_view") {

    sql """ DROP TABLE IF EXISTS d_table; """

    sql """
            create table d_table (
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql """insert into d_table select 1,1,1,'a';"""
    sql """insert into d_table select 2,2,2,'b';"""

    createMV("create materialized view k312 as select k3,k1,k2 from d_table;")

    sql """insert into d_table select 3,-3,null,'c';"""

    sql "analyze table d_table with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_fail("select * from d_table order by k1;", "k312")
    qt_select_star "select * from d_table order by k1;"

    sql """
        drop view if exists v_k312;
    """

    sql """
        create view v_k312 as select k1,k3,k2 from d_table where k3 = 1;
    """
    mv_rewrite_success("select * from v_k312 order by k1;", "k312")
    qt_select_mv "select * from v_k312 order by k1;"

    sql """
        drop view if exists v_k124;
    """

    sql """
        create view v_k124 as select k1,k2,k4 from d_table where k1 = 1;
    """
    mv_rewrite_fail("select * from v_k124 order by k1;", "k312")
    qt_select_mv "select * from v_k124 order by k1;"
}
