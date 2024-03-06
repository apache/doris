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

suite("test_drop_catalog_recycle_bin") {
    sql "CREATE DATABASE IF NOT EXISTS `test_drop_catalog_recycle_bin_db`"

    sql """
        CREATE TABLE IF NOT EXISTS `test_drop_catalog_recycle_bin_db`.`test_drop_catalog_recycle_bin_tb` (
            `k1` int(11) NULL,
            `k2` datetime NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        PARTITION BY RANGE(`k1`)
        (
            PARTITION p111 VALUES [('-1000'), ('111')),
            PARTITION p222 VALUES [('111'), ('222')),
            PARTITION p333 VALUES [('222'), ('333')),
            PARTITION p1000 VALUES [('333'), ('1000')))
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
    """

    // test drop partition in catalog recycle bin
    res = sql "SHOW CREATE TABLE `test_drop_catalog_recycle_bin_db`.`test_drop_catalog_recycle_bin_tb`;"
    assertTrue(res.size() != 0)

    sql "ALTER TABLE `test_drop_catalog_recycle_bin_db`.`test_drop_catalog_recycle_bin_tb` DROP PARTITION p1000;"

    pre_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "p1000" """
    assertTrue(pre_res.size() > 0)
    partition_id = pre_res[0][4]
    sql "DROP CATALOG RECYCLE BIN WHERE 'PartitionId' = ${partition_id};"
    cur_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "p1000" """
    assertTrue(pre_res.size() - cur_res.size() == 1)

    // test drop table in catalog recycle bin
    sql "DROP TABLE `test_drop_catalog_recycle_bin_db`.`test_drop_catalog_recycle_bin_tb`;"

    pre_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "test_drop_catalog_recycle_bin_tb" """
    assertTrue(pre_res.size() > 0)
    table_id = pre_res[0][3]
    sql "DROP CATALOG RECYCLE BIN WHERE 'TableId' = ${table_id};"
    cur_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "test_drop_catalog_recycle_bin_tb" """
    assertTrue(pre_res.size() - cur_res.size() == 1)

    // test drop db in catalog recycle bin
    sql """ DROP DATABASE `test_drop_catalog_recycle_bin_db` """

    pre_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "test_drop_catalog_recycle_bin_db" """
    assertTrue(pre_res.size() > 0)
    db_id = pre_res[0][2]
    sql "DROP CATALOG RECYCLE BIN WHERE 'DbId' = ${db_id};"
    cur_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "test_drop_catalog_recycle_bin_db" """
    assertTrue(pre_res.size() - cur_res.size() == 1)
}
