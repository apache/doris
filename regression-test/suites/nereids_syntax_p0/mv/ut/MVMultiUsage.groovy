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

suite ("MVMultiUsage") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS MVMultiUsage; """

    sql """
            create table MVMultiUsage (
                time_col dateV2, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into MVMultiUsage values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into MVMultiUsage values("2020-01-02",2,"b",2,2,2);"""
    sql """insert into MVMultiUsage values("2020-01-03",3,"c",3,3,3);"""


    createMV("create materialized view MVMultiUsage_mv as select deptno, empid, salary from MVMultiUsage order by deptno;")

    sleep(3000)

    sql """insert into MVMultiUsage values("2020-01-01",1,"a",1,1,1);"""

    sql "analyze table MVMultiUsage with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_fail("select * from MVMultiUsage order by empid;", "MVMultiUsage_mv")
    order_qt_select_star "select * from MVMultiUsage order by empid;"


    explain {
        sql("select * from (select deptno, empid from MVMultiUsage where deptno>100) A join (select deptno, empid from MVMultiUsage where deptno >200) B using (deptno);")
        contains "(MVMultiUsage_mv)"
        notContains "(MVMultiUsage)"
    }
    order_qt_select_mv "select * from (select deptno, empid from MVMultiUsage where deptno>100) A join (select deptno, empid from MVMultiUsage where deptno >200) B using (deptno) order by 1;"

    sql """set enable_stats=true;"""
    sql """alter table MVMultiUsage modify column time_col set stats ('row_count'='4');"""

    mv_rewrite_fail("select * from MVMultiUsage order by empid;", "MVMultiUsage_mv")
    explain {
        sql("select * from (select deptno, empid from MVMultiUsage where deptno>100) A join (select deptno, empid from MVMultiUsage where deptno >200) B using (deptno);")
        contains "(MVMultiUsage_mv)"
        notContains "(MVMultiUsage)"
    }

}
