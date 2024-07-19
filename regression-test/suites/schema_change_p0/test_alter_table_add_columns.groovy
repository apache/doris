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

suite("test_alter_table_add_columns") {
    def tbName = "test_alter_table_add_columns"

    sql "DROP TABLE IF EXISTS ${tbName} FORCE"
    sql """
        CREATE TABLE `${tbName}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(`siteid`, `citycode`, `username`)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ INSERT INTO ${tbName} VALUES
            (1, 1, "xxx", 1),
            (2, 2, "xxx", 2),
            (3, 3, "xxx", 3);
        """

    qt_order """ select * from ${tbName} order by siteid"""

    // Add two value column light schema change is true
    sql """ alter table ${tbName} ADD COLUMN (new_v1 INT MAX default "1" , new_v2 INT MAX default "2");"""

    qt_order """ select * from ${tbName} order by siteid"""

    sql """ INSERT INTO ${tbName} VALUES
            (4, 4, "yyy", 4, 4, 4),
            (5, 5, "yyy", 5, 5, 5),
            (6, 6, "yyy", 6, 6, 6);
        """

    qt_order """ select * from ${tbName} order by siteid"""

    // Add one value column light schema change is false

    sql """ alter table ${tbName} ADD COLUMN (new_k1 INT DEFAULT '1', new_k2 INT DEFAULT '2');"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    qt_sql """ DESC ${tbName}"""

    sql """ INSERT INTO ${tbName} VALUES
            (7, 7, "zzz", 7, 7, 7, 7, 7),
            (8, 8, "zzz", 8, 8, 8, 8, 8),
            (9, 9, "zzz", 9, 9, 9, 9, 9);
        """
    qt_order """ select * from ${tbName} order by siteid"""
    sql "DROP TABLE IF EXISTS ${tbName} FORCE"
}