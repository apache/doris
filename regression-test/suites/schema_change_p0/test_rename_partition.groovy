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

suite("test_rename_partition") {
    String tblName = "test_rename_partition"
    sql """DROP TABLE IF EXISTS ${tblName} FORCE; """

    sql """
        CREATE TABLE `${tblName}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(`siteid`, `citycode`, `username`)
        PARTITION BY RANGE(`siteid`)
                (
                    partition `old_p1` values [("1"), ("2")),
                    partition `old_p2` values [("2"), ("3"))
                )
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """INSERT INTO ${tblName}(siteid, citycode, username, pv) VALUES (1, 1, "xxx", 1), (2, 1, "yyy", 1);"""
    sql """SYNC;"""
    order_qt_sql """SELECT * from ${tblName};"""

    sql """ALTER TABLE ${tblName} RENAME PARTITION old_p1 new_p1;"""
    order_qt_sql """SELECT * from ${tblName};"""

    def results = sql """SHOW PARTITIONS FROM ${tblName};"""
    for (def result : results) {
        logger.info("result:${result}")
        if (["new_p1", "old_p2"].contains(result[1])) {
            continue;
        }
        assertTrue(false);
    }
    sql """DROP TABLE IF EXISTS ${tblName} FORCE; """
}
