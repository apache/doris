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

suite('test_alter_add_column_default_value_str') {
    def tbl = 'test_alter_add_column_default_value_str_tbl'
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql """
        CREATE TABLE ${tbl} (
            `k1` BIGINT NOT NULL,
            `v1` BIGINT NULL,
            `v2` INT NULL,
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO ${tbl} VALUES (1,1,1),(2,2,2),(3,3,3)
    """

    sql """ SYNC """

    // Check data before ALTER TABLE
    qt_select1 """ SELECT * FROM ${tbl} ORDER BY k1 """
    def tb1 = sql """ show create table ${tbl}"""
    logger.info("tb1:{}", tb1[0][1])
    assertFalse(tb1[0][1].contains("varchar(20) NOT NULL DEFAULT \"CURRENT_TIMESTAMP\""))
    assertFalse(tb1[0][1].contains("varchar(20) NOT NULL DEFAULT \"BITMAP_EMPTY\""))

    sql """
        ALTER TABLE ${tbl} add column timestamp_default varchar(20) NOT NULL default "CURRENT_TIMESTAMP";
    """

    sql """
        ALTER TABLE ${tbl} add column bitmap_default varchar(20) NOT NULL default "BITMAP_EMPTY";
    """

    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }

    // Check table structure after ALTER TABLE
    def tb2 = sql """ show create table ${tbl}"""
    logger.info("tb2:{}", tb2[0][1])
    assertTrue(tb2[0][1].contains("varchar(20) NOT NULL DEFAULT \"CURRENT_TIMESTAMP\""))
    assertTrue(tb2[0][1].contains("varchar(20) NOT NULL DEFAULT \"BITMAP_EMPTY\""))
    def resultCreate = sql """ SHOW CREATE TABLE ${tbl} """
    sql """insert into ${tbl} values (4,4,4,"4","4")"""
    sql """insert into ${tbl} (k1,v1,v2,timestamp_default,bitmap_default) values (5,5,5,5,"5")"""
    sql """insert into ${tbl} (k1,v1,v2,timestamp_default,bitmap_default) values (6,6,6,6,"6")"""
    qt_select4 """ SELECT k1,v1,v2,timestamp_default,bitmap_default FROM ${tbl} ORDER BY k1 """

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
}
