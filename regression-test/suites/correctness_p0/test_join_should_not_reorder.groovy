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

suite("test_join_should_not_reorder") {
    sql """
        drop table if exists reorder_a;
    """

    sql """
        drop table if exists reorder_b;
    """

    sql """
        drop table if exists reorder_c;
    """
    
    sql """
        CREATE TABLE IF NOT EXISTS `reorder_a` (
        `id` varchar(10),
        `name` varchar(10),
        `dt` date
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `reorder_b` (
        `id` varchar(10),
        `name` varchar(10),
        `dt1` date,
        `dt2` date
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE IF NOT EXISTS `reorder_c` (
        `id` varchar(10),
        `name` varchar(10),
        `dt` date
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
    """

    qt_select """
        select *
        from reorder_a 
        join reorder_b
        on reorder_a.dt between reorder_b.dt1 and reorder_b.dt2
        join reorder_c
        on reorder_c.id = reorder_a.id and reorder_c.id = reorder_b.id;
    """

    sql """
        drop table if exists reorder_a;
    """

    sql """
        drop table if exists reorder_b;
    """

    sql """
        drop table if exists reorder_c;
    """
}
