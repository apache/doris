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

suite("eliminate_nullaware_anti_join") {
    multi_sql """
    SET enable_nereids_planner=true;
    SET enable_fallback_to_original_planner=false;
    set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
    """

    sql """drop table if exists eliminate_nullaware_anti_join_A;"""
    sql """drop table if exists eliminate_nullaware_anti_join_B;"""
    sql """drop table if exists eliminate_nullaware_anti_join_C;"""
    sql """
        create table eliminate_nullaware_anti_join_A ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table eliminate_nullaware_anti_join_B ( b int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(b) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table eliminate_nullaware_anti_join_C ( c int )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(c) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    explain {
        sql("select * from eliminate_nullaware_anti_join_A where eliminate_nullaware_anti_join_A.a not in (select b from eliminate_nullaware_anti_join_B);")
        contains "LEFT ANTI JOIN"
    }

    explain {
        sql("select * from eliminate_nullaware_anti_join_C where eliminate_nullaware_anti_join_C.c not in (select b from eliminate_nullaware_anti_join_B);")
        contains "NULL AWARE LEFT ANTI JOIN"
    }

    explain {
        sql("select * from eliminate_nullaware_anti_join_A where eliminate_nullaware_anti_join_A.a not in (select c from eliminate_nullaware_anti_join_C);")
        contains "NULL AWARE LEFT ANTI JOIN"
    }

    // The nullable probe key is a non-null constant in the filter's Uniform trait.
    explain {
        sql """
            select c from eliminate_nullaware_anti_join_C
            where c = 7 and c not in (
                select b from eliminate_nullaware_anti_join_B
                group by b having count(*) > 1
            )
        """
        contains "LEFT ANTI JOIN"
        notContains "NULL AWARE LEFT ANTI JOIN"
    }

    // The probe's proof does not establish that the build key is non-null.
    explain {
        sql """
            select c from eliminate_nullaware_anti_join_C
            where c = 7 and c not in (
                select c from eliminate_nullaware_anti_join_C
                group by c having count(*) > 1
            )
        """
        contains "NULL AWARE LEFT ANTI JOIN"
    }

    sql "insert into eliminate_nullaware_anti_join_B values (1), (1), (2);"
    // Duplicate NULLs make the NULL group survive HAVING in the nullable-build case.
    sql "insert into eliminate_nullaware_anti_join_C values (null), (null), (7), (8), (8);"

    order_qt_uniform_nonnull_build """
        select c from eliminate_nullaware_anti_join_C
        where c = 7 and c not in (
            select b from eliminate_nullaware_anti_join_B
            group by b having count(*) > 1
        )
    """

    order_qt_nullable_build """
        select c from eliminate_nullaware_anti_join_C
        where c = 7 and c not in (
            select c from eliminate_nullaware_anti_join_C
            group by c having count(*) > 1
        )
    """

    order_qt_null_probe """
        select c from eliminate_nullaware_anti_join_C
        where c is null and c not in (select b from eliminate_nullaware_anti_join_B)
    """

    // The right table is NOT NULL, but the left outer join still produces NULL build keys.
    order_qt_outer_join_null_extension """
        select c from eliminate_nullaware_anti_join_C
        where c = 7 and c not in (
            select b from eliminate_nullaware_anti_join_C
            left join eliminate_nullaware_anti_join_B on eliminate_nullaware_anti_join_C.c =
                    eliminate_nullaware_anti_join_B.b
        )
    """
}

