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

suite("txn_insert_with_specify_columns", "p0") {
    def table = "txn_insert_with_specify_columns"

    sql """ DROP TABLE IF EXISTS ${table}"""
    sql """
        CREATE TABLE ${table}(
            c1 INT NULL,
            c2 INT NULL,
            c3 INT NULL default 1
        ) ENGINE=OLAP
        UNIQUE KEY(c1)
        DISTRIBUTED BY HASH(c1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """begin"""
    sql """insert into ${table} (c1, c3, c2) values(1, 3, 2),(111,333,222),(1111,3333,2222);"""
    sql """insert into ${table} (c3, c1) values(31, 11);"""
    sql """insert into ${table} (c2, c1) values(22, 12);"""
    sql """insert into ${table} (c2, c3, c1) values(23, 33, 13);"""
    sql """insert into ${table} (c1) values(14);"""
    sql """insert into ${table} (c3, c2, c1) values(35, 25, 15);"""
    sql """insert into ${table} (__DORIS_DELETE_SIGN__, c3, c2, c1) values(1, 35, 25, 15);"""
    sql """commit"""
    sql """set show_hidden_columns=true"""
    qt_select_unique """select c1,c2,c3,__DORIS_DELETE_SIGN__ from ${table} order by c1,c2,c3"""

    sql """ DROP TABLE IF EXISTS ${table}"""
    sql """
        CREATE TABLE ${table}(
            c1 INT NULL,
            c2 INT NULL,
            c3 INT NULL default 1
        ) ENGINE=OLAP
        DUPLICATE KEY(c1)
        DISTRIBUTED BY HASH(c1) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    sql """begin"""
    sql """insert into ${table} (c1, c3, c2) values(1, 3, 2),(111,333,222),(1111,3333,2222);"""
    sql """insert into ${table} (c3, c1) values(31, 11);"""
    sql """insert into ${table} (c2, c1) values(22, 12);"""
    sql """insert into ${table} (c2, c3, c1) values(23, 33, 13);"""
    sql """insert into ${table} (c1) values(14);"""
    sql """insert into ${table} (c3, c2, c1) values(35, 25, 15);"""
    sql """commit"""
    qt_select_unique """select c1,c2,c3 from ${table} order by c1,c2,c3"""

    try {
        sql """set enable_nereids_planner=false"""
        sql """ DROP TABLE IF EXISTS ${table}"""
        sql """
            CREATE TABLE ${table}(
                c1 INT NULL,
                c2 INT NULL,
                c3 INT NULL default 1
            ) ENGINE=OLAP
            UNIQUE KEY(c1)
            DISTRIBUTED BY HASH(c1) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        sql """begin"""
        sql """insert into ${table} (c1, c3, c2) values(10, 30, 20);"""
        logger.info(failed)
        assertFalse(true);
    } catch (Exception e) {
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains("The legacy planner does not support specifying column names"))
    } finally {
        sql "commit"
        sql """ DROP TABLE IF EXISTS ${table}"""
    }

    try {
        sql """set enable_nereids_planner=false"""
        sql """ DROP TABLE IF EXISTS ${table}"""
        sql """
            CREATE TABLE ${table}(
                c1 INT NULL,
                c2 INT NULL,
                c3 INT NULL default 1
            ) ENGINE=OLAP
            DUPLICATE KEY(c1)
            DISTRIBUTED BY HASH(c1) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        sql """begin"""
        sql """insert into ${table} (c1, c3, c2) values(10, 30, 20);"""
        logger.info(failed)
        assertFalse(true);
    } catch (Exception e) {
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains("The legacy planner does not support specifying column names"))
    } finally {
        sql "commit"
        sql """ DROP TABLE IF EXISTS ${table}"""
    }
}
