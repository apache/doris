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

suite("test_outer_join_with_grouping") {
    sql """
        drop table if exists table_a;
    """

    sql """
        drop table if exists table_b;
    """

    sql """
        CREATE TABLE IF NOT EXISTS `table_a` (
        `id` bigint(20) NOT NULL COMMENT '',
        `moid` int(11) REPLACE_IF_NOT_NULL NULL COMMENT '',
        `sid` int(11) REPLACE_IF_NOT_NULL NULL COMMENT ''
        ) ENGINE=OLAP
        AGGREGATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `table_b` (
        `id` bigint(20) NOT NULL COMMENT '',
        `name` varchar(192) NOT NULL COMMENT ''
        ) ENGINE=OLAP
        AGGREGATE KEY(`id`, `name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`, `name`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        INSERT INTO table_a (id,moid,sid) VALUES
        (2,8,9),
        (3,5,7),
        (6,3,6),
        (8,3,6),
        (1,6,1),
        (4,3,7),
        (7,3,2),
        (9,3,9),
        (5,6,4);
    """

    sql """
        INSERT INTO table_b (id,name) VALUES
        (1,'e'),
        (2,'b'),
        (4,'b'),
        (4,'c'),
        (7,'d'),
        (8,'c'),
        (2,'a'),
        (2,'c'),
        (3,'e'),
        (5,'e'),
        (6,'a'),
        (3,'d'),
        (6,'b'),
        (3,'a'),
        (5,'b'),
        (7,'a'),
        (9,'c'),
        (5,'c'),
        (6,'e'),
        (8,'e'),
        (1,'d'),
        (4,'a'),
        (9,'e'),
        (1,'b'),
        (1,'c'),
        (3,'b'),
        (9,'b'),
        (2,'d'),
        (4,'d'),
        (5,'a'),
        (7,'b'),
        (9,'a'),
        (1,'a'),
        (2,'e'),
        (6,'d'),
        (7,'c'),
        (5,'d'),
        (6,'c'),
        (7,'e'),
        (8,'a'),
        (8,'b'),
        (9,'d');
    """

    sql """
            with tmp_view AS 
            (SELECT moid,
                `name`
            FROM table_a m
            LEFT JOIN table_b s
                ON s.id = m.sid), t6 AS 
            (SELECT moid,
                GROUP_CONCAT(distinct `name`) nlist
            FROM tmp_view
            GROUP BY  moid)
        SELECT nlist
        FROM tmp_view
        INNER JOIN t6
            ON t6.moid=tmp_view.moid
        ORDER BY nlist;
    """

    sql """
            with tmp_view AS 
            (SELECT moid,
                `name`
            FROM table_a m
            LEFT JOIN table_b s
                ON s.id = m.sid), t6 AS 
            (SELECT moid,
                GROUP_CONCAT(distinct `name`) nlist
            FROM tmp_view
            GROUP BY  moid)
        SELECT nlist
        FROM tmp_view
        INNER JOIN t6
            ON TRUE
        ORDER BY nlist;
    """

    sql """
        drop table if exists table_a;
    """

    sql """
        drop table if exists table_b;
    """
}
