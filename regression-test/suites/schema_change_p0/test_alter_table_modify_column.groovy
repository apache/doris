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

suite("test_alter_table_modify_column") {
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

    // unique model table
    def uniqueTableName = "test_alter_table_modify_column_unique"

    sql "DROP TABLE IF EXISTS ${uniqueTableName} FORCE;"
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
            "replication_num" = "1"
        );
    """

    sql """ INSERT INTO ${uniqueTableName} VALUES
            (1, 1, "xxx", 1),
            (2, 2, "xxx", 2),
            (3, 3, "xxx", 3);
        """

    qt_order """select * from ${uniqueTableName} order by siteid"""

    test {
        sql """alter table ${uniqueTableName} modify COLUMN siteid INT SUM DEFAULT '0';"""
        // check exception message contains
        exception "Can not assign aggregation method on column in Unique data model table"
    }

    sql "DROP TABLE IF EXISTS ${uniqueTableName} FORCE"

    // aggregate model table
    def aggTableName = "test_alter_table_modify_column_agg"

    sql "DROP TABLE IF EXISTS ${aggTableName} FORCE"
    sql """
        CREATE TABLE `${aggTableName}`
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

    sql """ INSERT INTO ${aggTableName} VALUES
            (1, 1, "xxx", 1),
            (2, 2, "xxx", 2),
            (3, 3, "xxx", 3);
        """

    qt_order """select * from ${aggTableName} order by siteid"""

    test {
        sql """alter table ${aggTableName} modify COLUMN siteid INT key SUM DEFAULT '0';"""
        // check exception message contains
        exception "Key column can not set aggregation type"
    }

    test {
        sql """alter table ${aggTableName} modify COLUMN pv BIGINT DEFAULT '0';"""
        // check exception message contains
        exception "Can not change aggregation typ"
    }

    sql "DROP TABLE IF EXISTS ${aggTableName} FORCE"

    // duplicate model table
    def dupTableName = "test_alter_table_modify_column_dup"
    def dupTableRollupName = "test_alter_table_modify_column_dup_rollup"

    sql "DROP TABLE IF EXISTS ${dupTableName} FORCE"
    sql """
        CREATE TABLE `${dupTableName}`
        (
            `citycode` SMALLINT DEFAULT '10',
            `siteid` INT DEFAULT '10',
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT DEFAULT '0'
        )
        DUPLICATE KEY(`citycode`, `siteid`, `username`)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql "ALTER TABLE ${dupTableName} ADD ROLLUP ${dupTableRollupName}(`siteid`,`citycode`,`username`,`pv`);"
    waitRollupJob(dupTableName)

    sql """ INSERT INTO ${dupTableName} VALUES
            (1, 1, "xxx", 1),
            (2, 2, "xxx", 2),
            (3, 3, "xxx", 3);
        """

    qt_order """select * from ${dupTableName} order by siteid"""

    test {
        sql """alter table ${dupTableName} modify COLUMN siteid INT SUM DEFAULT '0';"""
        // check exception message contains
        exception "Can not assign aggregation method on column in Duplicate data model table"
    }

    test {
        sql """alter table ${dupTableName} modify COLUMN siteid BIGINT from not_exist_rollup;"""
        // check exception message contains
        exception "Index[not_exist_rollup] does not exist in table"
    }

    test {
        sql """alter table ${dupTableName} modify COLUMN not_exist_column BIGINT;"""
        // check exception message contains
        exception "Column[not_exist_column] does not exists"
    }

    test {
        sql """alter table ${dupTableName} modify COLUMN not_exist_column BIGINT from ${dupTableRollupName};"""
        // check exception message contains
        exception "Do not need to specify index name when just modifying column type"
    }

    test {
        sql """alter table ${dupTableName} modify COLUMN siteid BIGINT after not_exist_column;"""
        // check exception message contains
        exception "Column[not_exist_column] does not exists"
    }

    test {
        sql """alter table ${dupTableName} modify COLUMN citycode BIGINT DEFAULT '10' first;"""
        // check exception message contains
        exception "Invalid column order. value should be after key"
    }

    test {
        sql """alter table ${dupTableName} modify COLUMN siteid BIGINT key DEFAULT '10' first;"""
        // check exception message contains
        exception "Can not modify distribution column"
    }

    sql """alter table ${dupTableName} modify COLUMN username VARCHAR(32) key DEFAULT 'test' first;"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${dupTableName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    sql """ INSERT INTO ${dupTableName} VALUES
            ("yyy", 4, 4, 4)
        """
    qt_order """select * from ${dupTableName} order by siteid"""

    sql """alter table ${dupTableName} order by(citycode, siteid, username, pv);"""
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${dupTableName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }
    sql """ INSERT INTO ${dupTableName} VALUES
            (5, 5, "zzz", 5)
        """
    qt_order """select * from ${dupTableName} order by siteid"""
    sql "DROP TABLE IF EXISTS ${dupTableName} FORCE"
}