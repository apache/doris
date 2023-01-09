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

suite ("sub_query_correlated") {
    // enable nereids and vectorized engine

    sql """
        SET enable_nereids_planner=true
    """

    sql """
        SET enable_bucket_shuffle_join=false
    """

    sql """
        SET disable_colocate_plan=true
    """

    sql """
        DROP TABLE IF EXISTS `subquery1`
    """

    sql """
        DROP TABLE IF EXISTS `subquery2`
    """

    sql """
        DROP TABLE IF EXISTS `subquery3`
    """

    sql """
        DROP TABLE IF EXISTS `subquery4`
    """

    sql """
        create table if not exists subquery1
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1') 
    """

    sql """
        create table if not exists subquery2
        (k1 varchar(10), k2 bigint)
        partition by range(k2)
        (partition p1 values less than("10"))
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists subquery3
        (k1 int not null, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists subquery4
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        insert into subquery1 values (1,2), (1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4)
    """

    sql """
        insert into subquery2 values ("abc",2),("abc",3),("abcd",2),("abcde",4),("abcdef",5)
    """

    sql """
        insert into subquery3 values (1,"abc",2,3,4), (1,"abcd",3,3,4), (2,"xyz",2,4,2),
                                     (2,"uvw",3,4,2), (2,"uvw",3,4,2), (3,"abc",4,5,3), (3,"abc",4,5,3)
    """

    sql """
        insert into subquery4 values (5,4), (5,2), (8,3), (5,4), (6,7), (8,9)
    """

    sql "SET enable_fallback_to_original_planner=false"

    //------------------Correlated-----------------
    qt_scalar_less_than_corr """
        select * from subquery1 where subquery1.k1 < (select sum(subquery3.k3) from subquery3 where subquery3.v2 = subquery1.k2) order by k1, k2
    """
    
    qt_scalar_not_equal_corr """
        select * from subquery1 where subquery1.k1 != (select sum(subquery3.k3) from subquery3 where subquery3.v2 = subquery1.k2) order by k1, k2
    """
    
    qt_scalar_equal_to_corr """
        select * from subquery1 where subquery1.k1 = (select sum(subquery3.k3) from subquery3 where subquery3.v2 = subquery1.k2) order by k1, k2
    """
    
    qt_not_in_corr """
        select * from subquery1 where subquery1.k1 not in (select subquery3.k3 from subquery3 where subquery3.v2 = subquery1.k2) order by k1, k2
    """
    
    qt_in_subquery_corr """
        select * from subquery1 where subquery1.k1 in (select subquery3.k3 from subquery3 where subquery3.v2 = subquery1.k2) order by k1, k2
    """
    
    qt_not_exist_corr """
        select * from subquery1 where not exists (select subquery3.k3 from subquery3 where subquery1.k2 = subquery3.v2) order by k1, k2
    """

    qt_exist_corr """
        select * from subquery1 where exists (select subquery3.k3 from subquery3 where subquery1.k2 = subquery3.v2) order by k1, k2
    """
    
    qt_in_with_in_and_scalar """
        select * from subquery1 where subquery1.k1 in (
             select subquery3.k3 from subquery3 where 
                subquery3.k3 in (select subquery4.k1 from subquery4 where subquery4.k1 = 3)
                and subquery3.v2 > (select sum(subquery2.k2) from subquery2 where subquery2.k2 = subquery3.v1)) order by k1, k2
    """
    
    qt_exist_and_not_exist """
        select * from subquery1 where exists (select subquery3.k3 from subquery3 where subquery1.k2 = subquery3.v2)
                               and not exists (select subquery4.k2 from subquery4 where subquery1.k2 = subquery4.k2) order by k1, k2
    """

    //------------------Correlated----nonEqual-------------------

    qt_not_in_non_equal_corr """
        select * from subquery1 where subquery1.k1 not in (select subquery3.k3 from subquery3 where subquery3.v2 > subquery1.k2) order by k1, k2
    """

    qt_in_subquery_non_equal_corr """
        select * from subquery1 where subquery1.k1 in (select subquery3.k3 from subquery3 where subquery3.v2 < subquery1.k2) order by k1, k2
    """

    qt_in_subquery_non_equal_corr """
        select * from subquery1 where subquery1.k1 in (select subquery3.k3 from subquery3 where subquery3.v2 != subquery1.k2) order by k1, k2
    """

    qt_not_exist_non_equal_corr """
        select * from subquery1 where not exists (select subquery3.k3 from subquery3 where subquery1.k2 != subquery3.v2) order by k1, k2
    """

    qt_exist_non_equal_corr """
        select * from subquery1 where exists (select subquery3.k3 from subquery3 where subquery1.k2 > subquery3.v2) order by k1, k2
    """
    //------------------unCorrelated-----------------
    qt_scalar_unCorrelated """
        select * from subquery1 where subquery1.k1 < (select sum(subquery3.k3) from subquery3 where subquery3.v2 = 2) order by k1, k2
    """

    qt_scalar_equal_to_uncorr """
        select * from subquery1 where subquery1.k1 = (select sum(subquery3.k3) from subquery3) order by k1, k2
    """

    qt_not_scalar_unCorrelated """
        select * from subquery1 where subquery1.k1 != (select sum(subquery3.k3) from subquery3 where subquery3.v2 = 2) order by k1, k2
    """

    qt_scalar_not_equal_uncorr """
        select * from subquery1 where subquery1.k1 != (select sum(subquery3.k3) from subquery3) order by k1, k2
    """

    qt_in_unCorrelated """
        select * from subquery1 where subquery1.k1 in (select subquery3.k3 from subquery3 where subquery3.v2 = 2) order by k1, k2
    """

    qt_in_subquery_uncorr """
        select * from subquery1 where subquery1.k1 in (select subquery3.k3 from subquery3) order by k1, k2
    """

    qt_not_in_unCorrelated """
        select * from subquery1 where subquery1.k1 not in (select subquery3.k3 from subquery3 where subquery3.v2 = 2) order by k1, k2
    """

    qt_not_in_uncorr """
        select * from subquery1 where subquery1.k1 not in (select subquery3.k3 from subquery3) order by k1, k2
    """

    qt_exist_unCorrelated """
        select * from subquery1 where exists (select subquery3.k3 from subquery3 where subquery3.v2 = 2) order by k1, k2
    """

    qt_exist_uncorr """
        select * from subquery1 where exists (select subquery3.k3 from subquery3) order by k1, k2
    """

    //----------complex subqueries----------
    qt_scalar_subquery """
        select * from subquery1
            where k1 = (select sum(k1) from subquery3 where subquery1.k1 = subquery3.v1 and subquery3.v2 = 2)
            order by k1, k2
    """

    qt_in_subquery """
        select * from subquery3
            where (k1 = 1 or k1 = 2 or k1 = 3) and v1 in (select k1 from subquery1 where subquery1.k2 = subquery3.v2 and subquery1.k1 = 3)
            order by k1, k2
    """

    qt_exist_subquery """
        select * from subquery3
            where k1 = 2 and exists (select * from subquery1 where subquery1.k1 = subquery3.v2 and subquery1.k2 = 4)
            order by k1, k2
    """

    //----------complex nonEqual subqueries----------

    qt_in_subquery """
        select * from subquery3
            where (k1 = 1 or k1 = 2 or k1 = 3) and v1 in (select k1 from subquery1 where subquery1.k2 > subquery3.v2 and subquery1.k1 = 3)
            order by k1, k2
    """

    qt_exist_subquery """
        select * from subquery3
            where k1 = 2 and exists (select * from subquery1 where subquery1.k1 < subquery3.v2 and subquery1.k2 = 4)
            order by k1, k2
    """
}
