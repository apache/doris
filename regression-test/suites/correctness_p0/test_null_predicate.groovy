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

suite("test_null_predicate") {
    def tableName = "test_null_predicate"


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            `id` INT,
            `name` STRING NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    // bucket1: 102 106 108 114 116 120
    // bucket2: 101 105 111 113 119 123  -- all null
    // bucket3: 100 104 110 112 118 122  -- no null
    // bucket4: 103 107 109 115 117 121

    sql """ INSERT INTO ${tableName} VALUES 
            (100, "name#100"),
            (101, null),
            (102, "name#102"),
            (103, null),

            (104, "name#104"),
            (105, null),
            (106, "name#106"),
            (107, null),

            (108, null),
            (109, "name#109"),
            (110, "name#110"),
            (111, null),

            (112, "name#112"),
            (113, null),
            (114, null),
            (115, null),

            (116, "name#116"),
            (117, "name#117"),
            (118, "name#118"),
            (119, null),

            (120, "name#120"),
            (121, "name#121"),
            (122, "name#122"),
            (123, null); """

    qt_select1 """ select id, name from ${tableName} order by id, name; """
    qt_select2 """ select count(1) from ${tableName}; """
    qt_select3 """ select id, name from ${tableName} where name is null order by id; """
    qt_select4 """ select id, name from ${tableName} where id < 110 and name is null order by id; """
    qt_select5 """ select id, name from ${tableName} where id > 109 and name is null order by id; """
    qt_select6 """ select id, name from ${tableName} where id < 110 or name is null order by id; """
    qt_select7 """ select id, name from ${tableName} where id > 109 or name is null order by id; """
    qt_select8 """ select count(1) from ${tableName} where name is null; """
    qt_select9 """ select id, name from ${tableName} where name is not null order by id, name; """
    qt_select10 """ select id, name from ${tableName} where id < 110 and name is not null order by id, name; """
    qt_select11 """ select id, name from ${tableName} where id > 109 and name is not null order by id, name; """
    qt_select12 """ select id, name from ${tableName} where id < 110 or name is not null order by id, name; """
    qt_select13 """ select id, name from ${tableName} where id > 109 or name is not null order by id, name; """
    qt_select14 """ select count(1) from ${tableName} where name is not null; """

    sql """ DROP TABLE IF EXISTS test_null_predicate2 """
    // Here, create a table and make the "value" column nullable before the "name" column.
    // Via: https://github.com/apache/doris/issues/17462
    sql """
            CREATE TABLE IF NOT EXISTS test_null_predicate2 (
            `id` INT,
            `value` double NULL,
            `name` varchar(30)
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """
        INSERT INTO test_null_predicate2 values
            (1, null, "abc"),
            (2, 2.0, "efg"),
            (3, null, "ccc"),
            (4, 4.0, "ddd"),
            (5, null, "eeee");
    """

    qt_select15 """ select * from test_null_predicate2 where `value` is null order by id; """

    sql """ DROP TABLE IF EXISTS test_null_predicate"""
    sql """
        create table test_null_predicate (
            id boolean null,
            value int null
        ) duplicate key(id)
        DISTRIBUTED BY HASH(id) buckets 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        );
    """
    sql """
        insert into test_null_predicate values(1, null), (1,1), (null, 2), (1,2), (0, 3), (0, 4);
    """

    sql """
        insert into test_null_predicate values(1, null), (1,1), (null, 2), (1,2), (0, 3), (0, 4);
    """

    sql """
        delete from test_null_predicate where id is null;
    """

    sql """
        insert into test_null_predicate values (null, 99), (null, 101);
    """

    qt_select16 """
        select * from test_null_predicate where id is null order by id, value;
    """
}
