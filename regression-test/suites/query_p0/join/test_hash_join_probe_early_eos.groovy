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

suite("test_hash_join_probe_early_eos", "query") {

    sql """ DROP TABLE IF EXISTS test_hash_join_probe_early_eos_t0 """
    sql """ DROP TABLE IF EXISTS test_hash_join_probe_early_eos_t1 """
    sql """ DROP TABLE IF EXISTS test_hash_join_probe_early_eos_t2 """
    sql """ DROP TABLE IF EXISTS test_hash_join_probe_early_eos_t3 """

    // t0
    sql """
           CREATE TABLE test_hash_join_probe_early_eos_t0 (
              `t0_k0` int,
              `t0_v0` varchar(10)
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`t0_k0`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
         """
    sql """insert into test_hash_join_probe_early_eos_t0 values (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');"""

    // t1
    sql """
           CREATE TABLE test_hash_join_probe_early_eos_t1 (
              `t1_k0` int,
              `t1_v0` varchar(10)
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`t1_k0`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
         """
    sql """insert into test_hash_join_probe_early_eos_t1 values (1,'a'),(2,'b'),(3,'c');"""

    // t2
    sql """
           CREATE TABLE test_hash_join_probe_early_eos_t2 (
              `t2_k0` int,
              `t2_v0` varchar(10)
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`t2_k0`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
         """

    // t3
    sql """
           CREATE TABLE test_hash_join_probe_early_eos_t3 (
              `t3_k0` int,
              `t3_v0` varchar(10)
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`t3_k0`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
         """
    sql """insert into test_hash_join_probe_early_eos_t3 values (6,'f'),(7,'g');"""

    sql """ set disable_join_reorder = true; """

    qt_sanity_check0 """
        select * from test_hash_join_probe_early_eos_t0 order by t0_k0; 
    """
    qt_sanity_check1 """
        select * from test_hash_join_probe_early_eos_t1 order by t1_k0; 
    """
    qt_sanity_check2 """
        select * from test_hash_join_probe_early_eos_t2 order by t2_k0; 
    """
    qt_sanity_check3 """
        select * from test_hash_join_probe_early_eos_t3 order by t3_k0; 
    """
    qt_sanity_check4 """
         select
             *
         from
             test_hash_join_probe_early_eos_t1
             inner join test_hash_join_probe_early_eos_t3 on t1_k0 = t3_k0
    """

    qt_inner_join0 """
         select
             *
         from
             test_hash_join_probe_early_eos_t2
             inner join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
    """
    qt_inner_join1 """
        select
            *
        from
            test_hash_join_probe_early_eos_t0
            inner join (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    inner join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 on t0_k0 = t1_k0;
    """
    qt_inner_join2 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t1
                    inner join test_hash_join_probe_early_eos_t3 on t1_k0 = t3_k0
            ) tmp0 inner join test_hash_join_probe_early_eos_t0 on t1_k0 = t0_k0;
    """

    qt_left_join0 """
         select
             *
         from
             test_hash_join_probe_early_eos_t2
             left join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
         order by t2_k0, t1_k0;
    """
    qt_left_join1 """
        select
            *
        from
            test_hash_join_probe_early_eos_t0
            left join (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 on t0_k0 = t1_k0
         order by t0_k0;
    """
    qt_left_join2 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 left join test_hash_join_probe_early_eos_t0 on t1_k0 = t0_k0;
    """
    qt_left_join3 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t1
                    inner join test_hash_join_probe_early_eos_t3 on t1_k0 = t3_k0
            ) tmp0 left join test_hash_join_probe_early_eos_t0 on t1_k0 = t0_k0;
            
    """

    qt_left_semi_join0 """
       select
           *
       from
           test_hash_join_probe_early_eos_t2
           left semi join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
    """
    qt_left_semi_join1 """
        select
            *
        from
            test_hash_join_probe_early_eos_t0
            left semi join (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left semi join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 on t0_k0 = t2_k0;
    """
    qt_left_semi_join2 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left semi join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 left semi join test_hash_join_probe_early_eos_t0 on t2_k0 = t0_k0;
    """

    qt_left_anti_join0 """
       select
           *
       from
           test_hash_join_probe_early_eos_t2
           left anti join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
    """
    qt_left_anti_join1 """
        select
            *
        from
            test_hash_join_probe_early_eos_t0
            left anti join (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left anti join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 on t0_k0 = t2_k0
         order by t0_k0;
    """
    qt_left_anti_join2 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left anti join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 left anti join test_hash_join_probe_early_eos_t0 on t2_k0 = t0_k0;
    """

    qt_inner_join_probe_eos0 """
         select
             *
         from
             test_hash_join_probe_early_eos_t2
             inner join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
    """
    qt_inner_join_join_probe_eos1 """
        select
            *
        from
            test_hash_join_probe_early_eos_t0
            inner join (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    inner join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 on t0_k0 = t1_k0
         order by t0_k0;
    """
    qt_inner_join_join_probe_eos2 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t1
                    inner join test_hash_join_probe_early_eos_t3 on t1_k0 = t3_k0
            ) tmp0 inner join test_hash_join_probe_early_eos_t0 on t1_k0 = t0_k0;
    """

    qt_left_join_join_probe_eos0 """
         select
             *
         from
             test_hash_join_probe_early_eos_t2
             left join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
    """
    qt_left_join_join_probe_eos1 """
        select
            *
        from
            test_hash_join_probe_early_eos_t0
            left join (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 on t0_k0 = t1_k0
         order by t0_k0;
    """
    qt_left_join_join_probe_eos2 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 left join test_hash_join_probe_early_eos_t0 on t1_k0 = t0_k0;
    """
    qt_left_join_join_probe_eos3 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t1
                    inner join test_hash_join_probe_early_eos_t3 on t1_k0 = t3_k0
            ) tmp0 left join test_hash_join_probe_early_eos_t0 on t1_k0 = t0_k0;
            
    """

    qt_left_semi_join_join_probe_eos0 """
       select
           *
       from
           test_hash_join_probe_early_eos_t2
           left semi join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
    """
    qt_left_semi_join_join_probe_eos1 """
        select
            *
        from
            test_hash_join_probe_early_eos_t0
            left semi join (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left semi join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 on t0_k0 = t2_k0;
    """
    qt_left_semi_join_join_probe_eos2 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left semi join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 left semi join test_hash_join_probe_early_eos_t0 on t2_k0 = t0_k0;
    """

    qt_left_anti_join_join_probe_eos0 """
       select
           *
       from
           test_hash_join_probe_early_eos_t2
           left anti join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
    """
    qt_left_anti_join_join_probe_eos1 """
        select
            *
        from
            test_hash_join_probe_early_eos_t0
            left anti join (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left anti join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 on t0_k0 = t2_k0
         order by t0_k0;
    """
    qt_left_anti_join_join_probe_eos2 """
        select
            *
        from
            (
                select
                    *
                from
                    test_hash_join_probe_early_eos_t2
                    left anti join test_hash_join_probe_early_eos_t1 on t2_k0 = t1_k0
            ) tmp0 left anti join test_hash_join_probe_early_eos_t0 on t2_k0 = t0_k0;
    """
}
