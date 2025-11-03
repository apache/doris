/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("test_view_with_with_clause") {

    sql """
        CREATE TABLE IF NOT EXISTS test_view_with_with_clause (`a` date, `b` varchar(30) ,`c` varchar(30) )
            DUPLICATE KEY(`a`)
            DISTRIBUTED BY HASH(`a`)
            BUCKETS 10
            PROPERTIES("replication_allocation" = "tag.location.default:1"); 
    """

    sql """insert into test_view_with_with_clause values ('2022-12-01','001','001001');"""
    sql """insert into test_view_with_with_clause values ('2022-12-02','001','001002');"""
    sql """insert into test_view_with_with_clause values ('2022-12-01','001','001003');"""
    sql """insert into test_view_with_with_clause values ('2022-12-01','002','002001');"""
    sql """insert into test_view_with_with_clause values ('2022-12-02','002','002001');"""

    sql """
        create view IF NOT EXISTS viewtest_test_view_with_with_clause (b,cnt) as 
            with aaa as (
                select b,count(distinct c) cnt 
                from  test_view_with_with_clause
                group by b
                order by cnt desc
                limit 10
            )
            select * from aaa;
    """

    qt_sql """
        select * from viewtest_test_view_with_with_clause;
    """
}
