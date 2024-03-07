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

// this suite is for creating table with timestamp datatype in defferent
// case. For example: 'year' and 'Year' datatype should also be valid in definition

suite("test_set_operation_colocate") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'

    sql """
        drop table if exists test1
    """

    sql """
        drop table if exists test2
    """

    sql """
        drop table if exists test3
    """

    sql """
        create table test1 (
            `c1` int   ,
            `c2` varchar(10)   ,
            `pk` int
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into test1(pk,c1,c2) values (0,8,"yes"),(1,3,'y'),(2,3,"on"),(3,5,'f'),(4,null,'m'),(5,null,"really"),(6,null,"get");
    """

    sql """
        create table test2 (
            `c1` int  ,
            `c2` varchar(10)   ,
            `pk` int
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into test2(pk,c1,c2) values (0,null,"okay"),(1,8,'h'),(2,null,'x'),(3,3,'c'),(4,4,'i'),(5,null,"because"),(6,6,"yeah"),(7,7,'i'),(8,1,'z'),(9,null,'h'),(10,null,'x'),(11,7,'k'),(12,null,'n'),(13,8,'s'),(14,null,"I'll"),(15,null,"yeah"),(16,6,"was"),(17,null,"can"),(18,1,'a'),(19,null,'s'),(20,2,'n'),(21,4,"right"),(22,4,"were");
    """

    sql """
        create table test3 (
            `c1` int,
            `c2` varchar(10)   ,
            `pk` int
        ) engine=olap
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """
        insert into test3(pk,c1,c2) values (0,7,"have");
    """

    sql """
        sync
    """

    qt_test """
        SELECT COUNT () AS pk1
        FROM (
                (
                    SELECT t1.`pk`
                    FROM test1 AS t1
                        FULL OUTER JOIN test1 AS alias1 ON t1.`pk` = alias1.`pk`
                )
                UNION
                (
                    SELECT t1.`pk`
                    FROM test1 AS t1,
                        test2 AS alias2
                        INNER JOIN test3 AS alias3
                    ORDER BY t1.pk
                )
            ) subq1
        GROUP BY subq1.`pk`
        ORDER BY 1;
    """
}
