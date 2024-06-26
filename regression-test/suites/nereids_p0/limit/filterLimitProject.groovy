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

suite("test_filter_limit_project_translate", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        drop table if exists t1;
    """

    sql """
        create table t1 (
            pk int,
            c1 int   ,
            c2 varchar(10)   
        ) engine=olap
        UNIQUE KEY(pk)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into t1(pk,c1,c2) values (0,null,'k'),(1,2,'him'),(2,2,'think'),(3,null,'not'),(4,1,'k'),(5,null,'so'),(6,null,'j'),(7,5,'p'),(8,0,'did');
    """

    qt_test_translator """
        WITH cte1 AS (
            SELECT FIRST_VALUE (t1.`pk`) OVER(
                    ORDER BY t1.`pk`
                ) as pk
            FROM t1
            LIMIT 66666666
        ), cte2 AS (
            SELECT MIN (t1.`pk`) AS `pk`
            FROM t1
        )
        SELECT cte1.`pk` AS pk1
        FROM cte1
        WHERE cte1.`pk` < 4
        ORDER BY 1 ASC
        LIMIT 100 OFFSET 4;
    """
}
