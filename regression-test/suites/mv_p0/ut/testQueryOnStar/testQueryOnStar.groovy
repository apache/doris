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

suite ("testQueryOnStar") {
    sql """ DROP TABLE IF EXISTS emps; """

    sql """
            create table emps (
                time_col date, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into emps values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into emps values("2020-01-02",2,"b",2,2,2);"""

    createMV("create materialized view emps_mv as select time_col, deptno,empid, name, salary, commission from emps order by time_col, deptno, empid;")

    sql """insert into emps values("2020-01-01",1,"a",1,1,1);"""

    explain {
        sql("select * from emps order by empid;")
        contains "(emps_mv)"
    }
    qt_select_star "select * from emps order by empid;"

    explain {
        sql("select * from emps where deptno = 1;")
        contains "(emps_mv)"
    }
    qt_select_mv "select * from emps where deptno = 1 order by empid;"

    sql """ DROP TABLE IF EXISTS tpch_tiny_region; """
    sql """
        CREATE TABLE IF NOT EXISTS tpch_tiny_region (
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
    sql """insert into tpch_tiny_region values(1,'a','a');"""

    explain {
        sql("select ref_1.`empid` as c0 from tpch_tiny_region as ref_0 left join emps as ref_1 on (ref_0.`r_comment` = ref_1.`name` ) where true order by ref_0.`r_regionkey`,ref_0.`r_regionkey` desc ,ref_0.`r_regionkey`,ref_0.`r_regionkey`;")
        contains "(emps_mv)"
    }
    qt_select_mv "select ref_1.`empid` as c0 from tpch_tiny_region as ref_0 left join emps as ref_1 on (ref_0.`r_comment` = ref_1.`name` ) where true order by ref_0.`r_regionkey`,ref_0.`r_regionkey` desc ,ref_0.`r_regionkey`,ref_0.`r_regionkey`;"    

}
