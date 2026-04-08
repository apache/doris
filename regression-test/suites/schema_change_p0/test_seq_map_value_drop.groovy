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

suite('test_seq_map_value_drop', 'p0') {
    def tbName = 'test_seq_map_value_drop'
    sql """ DROP TABLE IF EXISTS ${tbName} """
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1  "
    def initTable = """
        CREATE TABLE IF NOT EXISTS `$tbName` (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT "",
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    def initTableData = """
    INSERT INTO ${tbName} VALUES
    (1, 1, 1, 1, 1, 1, 1),
    (2, 2, 2, 2, 2, 2, 2),
    (3, 3, 3, 3, 3, 3, 3);
    """
    def insertSql = ''
    def errorMessage = 'errCode = 2, detailMessage = Can not drop sequence column that has column group'
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} drop column s2"""
        insertSql = "INSERT INTO ${tbName} VALUES (4, 4, 4, 4, 4, 4);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    sql """ ALTER TABLE ${tbName} DROP COLUMN e """
    insertSql = "INSERT INTO ${tbName} VALUES (4, 4, 4, 4, 4, 4);"
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, false,"${tbName}")

    sql """ ALTER TABLE ${tbName} DROP COLUMN s2 """
    insertSql = "INSERT INTO ${tbName} VALUES (5, 5, 5, 5, 5);"
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, true,"${tbName}")

    sql """ DROP TABLE IF EXISTS ${tbName} """
}
