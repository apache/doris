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

suite ("onStar") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS onStar; """

    sql """
            create table onStar (
                time_col dateV2, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into onStar values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into onStar values("2020-01-02",2,"b",2,2,2);"""

    createMV("create materialized view onStar_mv as select time_col, deptno,empid, name, salary, commission from onStar order by time_col, deptno, empid;")

    sleep(3000)

    sql """insert into onStar values("2020-01-01",1,"a",1,1,1);"""

    sql "analyze table onStar with sync;"
    sql """set enable_stats=false;"""

    order_qt_select_star "select * from onStar order by empid;"
    order_qt_select_mv "select * from onStar where deptno = 1 order by empid;"

    sql """ DROP TABLE IF EXISTS onStar_tpch; """
    sql """
        CREATE TABLE IF NOT EXISTS onStar_tpch (
            r_regionkey  INTEGER NOT NULL,
            r_name       CHAR(25) NOT NULL,
            r_comment    VARCHAR(152)
        )
        DUPLICATE KEY(r_regionkey)
        DISTRIBUTED BY HASH(r_regionkey) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        )
        """
    sql """insert into onStar_tpch values(1,'a','a');"""

    order_qt_select_mv "select ref_1.`empid` as c0 from onStar_tpch as ref_0 left join onStar as ref_1 on (ref_0.`r_comment` = ref_1.`name` ) where true order by ref_0.`r_regionkey`,ref_0.`r_regionkey` desc ,ref_0.`r_regionkey`,ref_0.`r_regionkey`;"
}
