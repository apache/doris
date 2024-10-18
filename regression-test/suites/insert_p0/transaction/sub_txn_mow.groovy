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

suite("sub_txn_mow") {
    logger.info("mow tables with sub txn visible is not supported")
    return

    sql """ set enable_query_in_transaction_load = true """
    // case 1
    def table_txn = "sub_txn_mow"
    def table_normal = "sub_txn_mow_n"
    for (def i in 1..3) {
        for (def prefix: [table_normal, table_txn]) {
            sql """ drop table if exists ${prefix}_${i} """
            sql """
            CREATE TABLE ${prefix}_${i} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_mow_light_delete" = "true"
            );
            """
            if (i == 1) {
                sql """ insert into ${prefix}_${i} values(1, "a", 1), (2, "b", 2); """
            }
        }
    }
    for (def prefix: [table_normal, table_txn]) {
        if (prefix == table_txn) {
            sql """ begin; """
        }

        sql """ insert into ${prefix}_3 select * from ${prefix}_2; """
        order_qt_select_1 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_3 select * from ${prefix}_1; """
        order_qt_select_2 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_2 select * from ${prefix}_3; """
        order_qt_select_3 """ select * from ${prefix}_2; """

        sql """ insert into ${prefix}_1 select * from ${prefix}_2; """
        order_qt_select_4 """ select * from ${prefix}_1; """

        sql """ insert into ${prefix}_2 select * from ${prefix}_1; """
        order_qt_select_5 """ select * from ${prefix}_2; """

        if (prefix == table_txn) {
            sql """ commit; """
        }
        order_qt_select_61 """ select * from ${prefix}_1; """
        order_qt_select_62 """ select * from ${prefix}_2; """
        order_qt_select_63 """ select * from ${prefix}_3; """
    }

    // case 2: insert with partition
    table_txn = "sub_txn_mow_p"
    table_normal = "sub_txn_mow_pn"
    for (def i in 1..3) {
        for (def prefix: [table_normal, table_txn]) {
            sql """ drop table if exists ${prefix}_${i} """
            sql """
            CREATE TABLE ${prefix}_${i} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            PARTITION BY RANGE(id)
            (
                FROM (1) TO (30) INTERVAL 10
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "false",
                "replication_num" = "1"
            );
            """
            if (i == 1) {
                sql """ insert into ${prefix}_${i} values(1, "a", 1), (2, "b", 2), (10, "a", 10), (20, "b", 2); """
            }
        }
    }
    sql """ set enable_insert_strict = false """
    for (def prefix: [table_normal, table_txn]) {
        if (prefix == table_txn) {
            sql """ begin; """
        }

        sql """ insert into ${prefix}_3 PARTITION(p_1_11) select * from ${prefix}_2; """
        order_qt_par_1 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_2 PARTITION(p_1_11) select * from ${prefix}_3; """
        order_qt_par_2 """ select * from ${prefix}_2; """

        sql """ insert into ${prefix}_3 select * from ${prefix}_1; """
        order_qt_par_3 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_2 select * from ${prefix}_3; """
        order_qt_par_4 """ select * from ${prefix}_2; """

        sql """ insert into ${prefix}_3 PARTITION(p_1_11) select * from ${prefix}_2; """
        order_qt_par_5 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_2 PARTITION(p_11_21) select * from ${prefix}_3; """
        order_qt_par_6 """ select * from ${prefix}_2; """

        if (prefix == table_txn) {
            sql """ commit; """
        }
        order_qt_par_71 """ select * from ${prefix}_1; """
        order_qt_par_72 """ select * from ${prefix}_2; """
        order_qt_par_73 """ select * from ${prefix}_3; """
    }
    sql """ set enable_insert_strict = true """

    // case 3: delete command
    table_txn = "sub_txn_mow"
    table_normal = "sub_txn_mow_n"
    for (def prefix: [table_normal, table_txn]) {
        if (prefix == table_txn) {
            sql """ begin; """
        }

        sql """ delete from ${prefix}_3 where id > 1; """
        order_qt_del_1 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_2 select * from ${prefix}_3; """
        order_qt_del_2 """ select * from ${prefix}_2; """

        sql """ insert into ${prefix}_3 select * from ${prefix}_2; """
        order_qt_del_3 """ select * from ${prefix}_3; """

        sql """ update ${prefix}_3 set score = score + 100 where id = 1; """
        order_qt_del_4 """ select * from ${prefix}_3; """

        sql """ delete from ${prefix}_3 where id < 2; """
        order_qt_del_5 """ select * from ${prefix}_3; """

        sql """ insert into ${prefix}_2 select * from ${prefix}_3; """
        order_qt_del_6 """ select * from ${prefix}_2; """

        if (prefix == table_txn) {
            sql """ commit; """
        }
        order_qt_del_71 """ select * from ${prefix}_1; """
        order_qt_del_72 """ select * from ${prefix}_2; """
        order_qt_del_73 """ select * from ${prefix}_3; """
    }
}
