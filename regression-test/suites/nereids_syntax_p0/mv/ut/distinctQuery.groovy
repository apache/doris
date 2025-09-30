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

suite ("distinctQuery") {
    // this mv rewrite would not be rewritten in RBO phase, so set TRY_IN_RBO explicitly to make case stable
    sql "set pre_materialized_view_rewrite_strategy = TRY_IN_RBO"
    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """ DROP TABLE IF EXISTS distinctQuery; """

    sql """
            create table distinctQuery (
                time_col dateV2, 
                empid int, 
                name varchar, 
                deptno int, 
                salary int, 
                commission int)
            partition by range (time_col) (partition p1 values less than MAXVALUE) distributed by hash(time_col) buckets 3 properties('replication_num' = '1');
        """

    sql """insert into distinctQuery values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into distinctQuery values("2020-01-02",2,"b",2,2,2);"""
    sql """insert into distinctQuery values("2020-01-03",3,"c",3,3,3);"""

    sql """alter table distinctQuery modify column time_col set stats ('row_count'='5');"""

    sql """insert into distinctQuery values("2020-01-01",1,"a",1,1,1);"""
    sql """insert into distinctQuery values("2020-01-01",2,"a",1,1,1);"""

    createMV("create materialized view distinctQuery_mv as select deptno as a1, count(salary) as a2 from distinctQuery group by deptno;")

    createMV("create materialized view distinctQuery_mv2 as select empid as a3, deptno as a4, count(salary) as a5 from distinctQuery group by empid, deptno;")

    sql "analyze table distinctQuery with sync;"
    
    mv_rewrite_any_success("select distinct deptno from distinctQuery;", ["distinctQuery_mv", "distinctQuery_mv2"])

    mv_rewrite_success("select deptno, count(distinct empid) from distinctQuery group by deptno;", "distinctQuery_mv2")
    
}
