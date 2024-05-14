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

suite("select_with_tablets") {
    sql 'set enable_nereids_planner=true;'
    sql 'set enable_fallback_to_original_planner=false;'


    def table_name1 = "test_table"
    sql """ DROP TABLE IF EXISTS ${table_name1} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_name1} (
        `id` int(11) NULL,
        `name` string NULL,
        `age` int(11) NULL
        )
        PARTITION BY RANGE(id)
        (
            PARTITION less_than_20 VALUES LESS THAN ("20"),
            PARTITION between_20_70 VALUES [("20"),("70")),
            PARTITION more_than_70 VALUES LESS THAN ("151")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO ${table_name1} VALUES (1, 'doris', 19), (2, 'nereids', 18) """
    def insert_res = sql "show last insert;"

    logger.info("insert result: " + insert_res.toString())
    order_qt_select1 """ SELECT * FROM ${table_name1} """

    def res = sql_return_maparray """ show tablets from ${table_name1}"""
    log.info("res: " + res.toString())
    res = deduplicate_tablets(res)
    log.info("res: " + res.toString())

    res = sql_return_maparray """ show tablets from ${table_name1} where version = 2 """
    log.info("res: " + res.toString())
    res = deduplicate_tablets(res)
    log.info("res: " + res.toString())
    assertEquals(res.size(), 1)
    assertEquals("2", res[0].Version)

    order_qt_select2 """ SELECT * FROM ${table_name1} TABLET(${res[0].TabletId}) """
    order_qt_select3 """ SELECT * FROM ${table_name1} PARTITION less_than_20 TABLET(${res[0].TabletId}) """
    // result should be empty because TABLET(${res[0].TabletId}) is not belonged to partition between_20_70.
    order_qt_select4 """ SELECT * FROM ${table_name1} PARTITION between_20_70 TABLET(${res[0].TabletId}) """

    order_qt_select5 """ SELECT * FROM ${table_name1} where id < 2 """
    order_qt_select6 """ SELECT * FROM ${table_name1} TABLET(${res[0].TabletId}) where id = 2 """
    order_qt_select7 """ SELECT * FROM ${table_name1} TABLET(${res[0].TabletId}) where id < 2 """
    order_qt_select8 """ SELECT * FROM ${table_name1} PARTITION less_than_20 TABLET(${res[0].TabletId}) where id < 2 """
    // result of order_qt_select9 should be empty
    order_qt_select9 """ SELECT * FROM ${table_name1} PARTITION between_20_70 TABLET(${res[0].TabletId}) where id < 2"""
    order_qt_select10 """ SELECT * FROM ${table_name1} PARTITION less_than_20 where id < 2"""
    // result of order_qt_select11 should be empty
    order_qt_select11 """ SELECT * FROM ${table_name1} PARTITION between_20_70 where id < 2"""

    res = sql_return_maparray """ show tablets from ${table_name1} where version = 1 """
    log.info("res: " + res.toString())
    res = deduplicate_tablets(res)
    log.info("res: " + res.toString())
    assertEquals(res.size(), 2)
    assertEquals("1", res[0].Version)
    assertEquals("1", res[1].Version)
    // result should be empty because TABLET(${res[0].TabletId}) does not have data.
    order_qt_select12 """ SELECT * FROM ${table_name1} TABLET(${res[0].TabletId}) """
    // result should be empty because TABLET(${res[1].TabletId}) does not have data.
    order_qt_select13 """ SELECT * FROM ${table_name1} TABLET(${res[1].TabletId}) """

    // Test non-partitioned table
    def table_no_partition = "table_no_partition"
    sql """ DROP TABLE IF EXISTS ${table_no_partition} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_no_partition} (
        `id` int(11) NULL,
        `name` string NULL,
        `age` int(11) NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO ${table_no_partition} VALUES (1, 'doris', 19), (2, 'nereids', 18) """
    insert_res = sql "show last insert;"

    logger.info("insert result: " + insert_res.toString())
    order_qt_no_partition_1 """ SELECT * FROM ${table_no_partition} """

    res = sql_return_maparray """ show tablets from ${table_no_partition} where version = 2 """
    res = deduplicate_tablets(res)

    order_qt_no_partition_2 """ SELECT * FROM ${table_no_partition} TABLET(${res[0].TabletId}) """
    order_qt_no_partition_3 """ SELECT * FROM ${table_no_partition} TABLET(${res[1].TabletId}) """
    order_qt_no_partition_4 """ SELECT * FROM ${table_no_partition} TABLET(${res[2].TabletId}) """

    order_qt_no_partition_5 """ SELECT * FROM ${table_no_partition} where id = 2 """
    order_qt_no_partition_6 """ SELECT * FROM ${table_no_partition} TABLET(${res[0].TabletId}) where id = 2 """
    order_qt_no_partition_7 """ SELECT * FROM ${table_no_partition} TABLET(${res[0].TabletId}) where id > 2 """
}
