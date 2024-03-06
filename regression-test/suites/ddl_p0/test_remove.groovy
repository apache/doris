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

suite("test_remove") {
    sql "CREATE DATABASE IF NOT EXISTS `test_remove_db`"

    sql """
        CREATE TABLE IF NOT EXISTS `test_remove_db`.`test_remove_tb` (
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

    // test drop/remove partition
    def res = sql "SHOW CREATE TABLE `test_remove_db`.`test_remove_tb`;"
    assertTrue(res.size() != 0)

    sql "ALTER TABLE `test_remove_db`.`test_remove_tb` DROP PARTITION p1000;"

    res = sql "SHOW CREATE TABLE `test_remove_db`.`test_remove_tb`;"
    assertTrue(res.size() != 0)

    pre_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "p1000" """
    sql "REMOVE PARTITION p1000 FROM `test_remove_db`.`test_remove_tb`;"
    cur_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "p1000" """
    assertTrue(pre_res.size() - cur_res.size() == 1)

    // test drop/remove table
    sql "DROP TABLE `test_remove_db`.`test_remove_tb`;"

    pre_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "test_remove_tb" """
    sql "REMOVE TABLE `test_remove_db`.`test_remove_tb`;"
    cur_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "test_remove_tb" """
    assertTrue(pre_res.size() - cur_res.size() == 1)

    // test drop/remove db
    sql """ DROP DATABASE `test_remove_db` """

    pre_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "test_remove_db" """
    sql """ REMOVE DATABASE `test_remove_db` """
    cur_res = sql """ SHOW CATALOG RECYCLE BIN WHERE NAME = "test_remove_db" """
    assertTrue(pre_res.size() - cur_res.size() == 1)
}
