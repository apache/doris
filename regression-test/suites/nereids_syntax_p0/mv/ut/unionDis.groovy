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

suite ("unionDis") {
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS unionDis; """

    sql """
            create table unionDis (
                time_col dateV2, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into unionDis values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into unionDis values("2020-01-02",2,"b",2,2,2);"""
    sql """insert into unionDis values("2020-01-03",3,"c",3,3,3);"""

    createMV("create materialized view unionDis_mv as select empid, deptno from unionDis order by empid, deptno;")

    sleep(3000)

    sql """insert into unionDis values("2020-01-01",1,"a",1,1,1);"""

    explain {
        sql("select * from unionDis order by empid;")
        contains "(unionDis)"
    }
    order_qt_select_star "select * from unionDis order by empid;"


    explain {
        sql("select empid, deptno from unionDis where empid >1 union select empid, deptno from unionDis where empid <0 order by empid;")
        contains "(unionDis_mv)"
        notContains "(unionDis)"
    }
    order_qt_select_mv "select * from (select empid, deptno from unionDis where empid >1 union select empid, deptno from unionDis where empid <0) t order by 1;"

}
