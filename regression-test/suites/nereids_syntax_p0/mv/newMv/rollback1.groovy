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

suite ("rollback1") {
    sql """ DROP TABLE IF EXISTS rollback1; """

    sql """
            create table rollback1(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into rollback1 select 1,1,1,'a';"
    sql "insert into rollback1 select 2,2,2,'b';"
    sql "insert into rollback1 select 3,-3,null,'c';"

    createMV("create materialized view k123p as select k1,k2+k3 from rollback1;")

    sleep(3000)

    sql "insert into rollback1 select -4,-4,-4,'d';"
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "analyze table rollback1 with sync;"
    sql """set enable_stats=false;"""


    order_qt_select_star "select * from rollback1 order by k1;"

    qt_select_mv "select k1 p from rollback1 as t where t.k1 in (select k1 from rollback1) order by p;"
    qt_select_mv "select k1 p from rollback1 as t where t.k1 in (select k2 from rollback1) order by p;"
    qt_select_mv "select k1 p from rollback1 as t where t.k2 in (select k1 from rollback1) order by p;"
    qt_select_mv "select k1 p from rollback1 as t where t.k2 in (select k2 from rollback1) order by p;"
    qt_select_mv "select k2 p from rollback1 as t where t.k1 in (select k1 from rollback1) order by p;"
    qt_select_mv "select k2 p from rollback1 as t where t.k1 in (select k2 from rollback1) order by p;"
    qt_select_mv "select k2 p from rollback1 as t where t.k2 in (select k1 from rollback1) order by p;"
    qt_select_mv "select k2 p from rollback1 as t where t.k2 in (select k2 from rollback1) order by p;"
}
