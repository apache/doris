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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("insert_with_null") {
    def table = "insert_with_null"
    sql """ DROP TABLE IF EXISTS $table """
    sql """
        CREATE TABLE ${table} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${table}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    def write_modes = ["insert", "txn_insert", "group_commit_legacy", "group_commit_nereids"]

    for (def write_mode : write_modes) {
        sql """ DROP TABLE IF EXISTS ${table} """
        sql """
        CREATE TABLE ${table} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `desc` array<String> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """
        if (write_mode == "txn_insert") {
            sql "begin"
        } else if (write_mode == "group_commit_legacy") {
            sql """ set group_commit = async_mode; """
            sql """ set enable_nereids_dml = false; """
        } else if (write_mode == "group_commit_nereids") {
            sql """ set group_commit = async_mode; """
            sql """ set enable_nereids_dml = true; """
            sql """ set enable_nereids_planner=true; """
            sql """ set enable_fallback_to_original_planner=false; """
        }

        sql """ insert into ${table} values(1, '"b"', ["k1=v1, k2=v2"]); """
        sql """ insert into ${table} values(2, "\\N", ['k3=v3, k4=v4']); """
        sql """ insert into ${table} values(3, 'null', []); """
        sql """ insert into ${table} values(4, 'NULL', ['k5, k6']); """
        sql """ insert into ${table} values(5, null, ["k7", "k8"]); """
        sql """ insert into ${table} values(6, "\\n", ["k7", "k8"]); """
        sql """ insert into ${table} values(7, "", ["k7", "k8"]); """
        sql """ insert into ${table} values(8, '', ["k7", "k8"]); """
        sql """ insert into ${table} values(9, '"a', ["k7", "k8"]); """
        sql """ insert into ${table} values(10, 'a"', ["k7", "k8"]); """
        // sql """ insert into ${table} values(21, "\\\\N", []); """
        if (write_mode != "txn_insert") {
            sql """ insert into ${table}(id) values(22); """
            getRowCount(11)
        } else {
            sql "commit"
            getRowCount(10)
        }

        qt_sql """ select * from ${table} order by id asc; """
        qt_sql """ select * from ${table} where name is null order by id asc; """
        qt_sql """ select * from ${table} where `desc` is null order by id asc; """
    }
}
