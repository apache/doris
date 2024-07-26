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

suite("test_alter_table_drop_column") {
    // unique model table
    def uniqueTableName = "test_alter_table_drop_column_unique"
    def uniqueTableRollupName = "test_alter_table_drop_column_rollup_unique"

    sql "DROP TABLE IF EXISTS ${uniqueTableName} FORCE"
    sql """
        CREATE TABLE `${uniqueTableName}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT DEFAULT '0'
        )
        UNIQUE KEY(`siteid`, `citycode`, `username`)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "pv",
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    // only mor table can use roll up

    sql "ALTER TABLE ${uniqueTableName} ADD ROLLUP ${uniqueTableRollupName}(`citycode`,`siteid`,`username`,`pv`);"
    def waitRollupJob = { String tableName /* param */ ->
        int tryTimes = 30
        while (tryTimes-- > 0) {
            def jobResult = sql """SHOW ALTER TABLE ROLLUP WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1"""
            def jobState = jobResult[0][8].toString()
            if ('cancelled'.equalsIgnoreCase(jobState)) {
                logger.info("jobResult:{}", jobResult)
                throw new IllegalStateException("${tableName}'s job has been cancelled")
            }
            if ('finished'.equalsIgnoreCase(jobState)) {
                logger.info("jobResult:{}", jobResult)
                return;
            }
            sleep(10000)
        }
        assertTrue(false)
    }

    waitRollupJob(uniqueTableName)

    sql """ INSERT INTO ${uniqueTableName} VALUES
            (1, 1, "xxx", 1),
            (2, 2, "xxx", 2),
            (3, 3, "xxx", 3);
        """

    qt_order """ select * from ${uniqueTableName} order by siteid"""
    qt_order """ select * from ${uniqueTableName} order by citycode"""

    test {
        sql """ alter table ${uniqueTableName} drop COLUMN siteid;"""
        // check exception message contains
        exception "Can not drop key column in Unique data model table"
    }

    sql """ alter table ${uniqueTableName} drop COLUMN pv from ${uniqueTableRollupName};"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${uniqueTableName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    qt_order """ select * from ${uniqueTableName} order by siteid"""
    qt_order """ select * from ${uniqueTableName} order by citycode"""

    sql "DROP TABLE IF EXISTS ${uniqueTableName} FORCE"

    // aggregage model table
    def aggTableName = "test_alter_table_drop_column_agg"
    def aggTableRollupName = "test_alter_table_drop_column_rollup_agg"

    sql "DROP TABLE IF EXISTS ${aggTableName} FORCE"
    sql """
        CREATE TABLE `${aggTableName}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT REPLACE DEFAULT '0'
        )
        AGGREGATE KEY(`siteid`, `citycode`, `username`)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql "ALTER TABLE ${aggTableName} ADD ROLLUP ${aggTableRollupName}(`citycode`,`siteid`,`username`,`pv`);"
    waitRollupJob(aggTableName)
    sql """ INSERT INTO ${aggTableName} VALUES
            (1, 1, "xxx", 1),
            (2, 2, "xxx", 2),
            (3, 3, "xxx", 3);
        """

    qt_order """ select * from ${aggTableName} order by siteid"""
    qt_order """ select * from ${aggTableName} order by citycode"""

    test {
        sql """ alter table ${aggTableName} drop COLUMN citycode from ${aggTableRollupName};"""
        // check exception message contains
        exception "Can not drop key column when rollup has value column with REPLACE aggregation method"
    }

    sql """ alter table ${aggTableName} drop COLUMN pv from ${aggTableRollupName};"""

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${aggTableName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    qt_order """ select * from ${aggTableName} order by siteid"""
    qt_order """ select * from ${aggTableName} order by citycode"""

    test {
        sql """ alter table ${aggTableName} drop COLUMN pv from ${aggTableRollupName};"""
        // check exception message contains
        exception "Column does not exists"
    }

    // duplicate model table
    def dupTableName = "test_alter_table_drop_column_dup"

    sql "DROP TABLE IF EXISTS ${dupTableName} FORCE"
    sql """
        CREATE TABLE `${dupTableName}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT DEFAULT '0'
        )
        DUPLICATE KEY(`siteid`, `citycode`, `username`)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    test {
        sql """alter table ${dupTableName} drop COLUMN siteid;"""
        // check exception message contains
        exception "Distribution column[siteid] cannot be dropped"
    }

    sql "DROP TABLE IF EXISTS ${dupTableName} FORCE"
}