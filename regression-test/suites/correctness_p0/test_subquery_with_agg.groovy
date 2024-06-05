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

suite("test_subquery_with_agg") {
    sql """
        drop table if exists agg_subquery_table;
    """
    
    sql """
        CREATE TABLE IF NOT EXISTS agg_subquery_table
        (
            gid       varchar(50)  NOT NULL,
            num       int(11) SUM NOT NULL DEFAULT "0",
            id_bitmap bitmap BITMAP_UNION NOT NULL
        ) ENGINE = OLAP 
        AGGREGATE KEY(gid)
        DISTRIBUTED BY HASH(gid) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO agg_subquery_table VALUES
        ('1',4,to_bitmap(7)),
        ('2',5,to_bitmap(8)),
        ('3',6,to_bitmap(9));
    """

    qt_select """
        SELECT
        subq_1.gid AS c0
        FROM
        agg_subquery_table AS subq_1
        WHERE
        EXISTS (
            SELECT
            ref_2.amt AS c2
            FROM
            (
                SELECT
                bitmap_union_count(id_bitmap) AS unt,
                sum(num) AS amt
                FROM
                agg_subquery_table
            ) AS ref_2
        )
        order by
        subq_1.gid;
    """

    qt_select2 """
    select
        0 as row_number,
        round(sum(num) / 10, 0) as org_id
    from
    (
        select
            num
        from
            agg_subquery_table
    ) t
    group by
        1;
    """

    sql """
        drop table if exists agg_subquery_table;
    """

    sql """drop table if exists subquery_table_xyz;"""
    sql """CREATE TABLE `subquery_table_xyz` (
            `phone`bigint(20) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`phone`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`phone`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
    sql """WITH tmp1 AS 
            (SELECT DISTINCT phone
            FROM subquery_table_xyz oua
            WHERE (NOT EXISTS 
                (SELECT 1
                FROM subquery_table_xyz o1
                WHERE oua.phone = o1.phone
                        AND phone IS NOT NULL))), 
            tmp2 AS 
                (SELECT DISTINCT phone
                FROM subquery_table_xyz oua
                WHERE (NOT EXISTS 
                    (SELECT 1
                    FROM subquery_table_xyz o1
                    WHERE oua.phone = o1.phone
                            and phone IS NOT NULL))), 
            tmp3 AS 
                    (SELECT DISTINCT phone
                    FROM subquery_table_xyz oua
                    WHERE (NOT EXISTS 
                        (SELECT 1
                        FROM subquery_table_xyz o1
                        WHERE oua.phone = o1.phone and 
                                phone IS NOT NULL)))
                    SELECT COUNT(DISTINCT tmp1.phone)
                FROM tmp1
            JOIN tmp2
            ON tmp1.phone = tmp2.phone
        JOIN tmp3
            ON tmp2.phone = tmp3.phone;"""

}
