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

suite("test_subquery_grouping") {

    sql """
        drop table if exists grouping_subquery_table;
    """

    sql """
        create table grouping_subquery_table ( a int not null, b int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into grouping_subquery_table values( 1, 2 );
    """

    qt_sql """
        SELECT
        a
        FROM
        (
            with base_table as (
            SELECT
                `a`,
                sum(`b`) as `sum(b)`
            FROM
                (
                SELECT
                    inv.a,
                    sum(inv.b) as b
                FROM
                    grouping_subquery_table inv
                group by
                    inv.a
                ) T
            GROUP BY
                `a`
            ),
            grouping_sum_table as (
            select
                `a`,
                sum(`sum(b)`) as `sum(b)`
            from
                base_table
            group by
                grouping sets (
                (`base_table`.`a`)
                )
            )
            select
            *
            from
            (
                select
                `a`,
                `sum(b)`
                from
                base_table
                union all
                select
                `a`,
                `sum(b)`
                from
                grouping_sum_table
            ) T
        ) T2;
    """

    sql """
        drop table if exists grouping_subquery_table;
    """
}
