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

suite ("testProjectionMV4") {
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

    def result = "null"

    createMV("create materialized view emps_mv as select name, deptno, salary from emps;")

    sql """insert into emps values("2020-01-01",1,"a",1,1,1);"""

    sql "analyze table emps with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("select * from emps order by empid;")
        contains "(emps)"
    }
    qt_select_star "select * from emps order by empid;"


    qt_select_mv "select name from emps where deptno > 1 and salary > 1 order by name;"

    explain {
        sql("select empid from emps where deptno > 1 and empid > 1 order by empid;")
        contains "(emps)"
    }
    qt_select_base "select empid from emps where deptno > 1 and empid > 1 order by empid;"

    sql """set enable_stats=true;"""
    explain {
        sql("select * from emps order by empid;")
        contains "(emps)"
    }

    explain {
        sql("select empid from emps where deptno > 1 and empid > 1 order by empid;")
        contains "(emps)"
    }
}
