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

suite('test_seq_map_value_add', 'p0') {
    def tbName = 'test_seq_map_value_add'
    //Test the unique model by adding a value column
    sql """ DROP TABLE IF EXISTS ${tbName} """
    def initTable = """
                    CREATE TABLE IF NOT EXISTS `$tbName` (
                            `a` bigint(20) NULL COMMENT "",
                            `b` int(11) NULL COMMENT "",
                            `c` int(11) NULL COMMENT "",
                            `d` int(11) NULL COMMENT "",
                            `s1` int(11) NULL COMMENT "",
                            ) ENGINE=OLAP
                            UNIQUE KEY(`a`, `b`)
                            COMMENT "OLAP"
                            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
                            PROPERTIES (
                            "enable_unique_key_merge_on_write" = "false",
                            "light_schema_change"="false",
                            "replication_num" = "1",
                            "sequence_mapping.s1" = "c,d"
                            );
    """

    def initTableData = "insert into ${tbName} values(1,1,1,1,1)," +
            '               (2,2,2,2,2),' +
            '               (3,3,3,3,3);'

    // Test add column without set sequence mapping property
    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1  "
    def errorMessage = 'errCode = 2, detailMessage = Sequence mapping table needs mapping info in properties when add column.'
    def insertSql = ''
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column e int(11) NULL"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test set wrong format sequence mapping property
    errorMessage = 'errCode = 2, detailMessage = The sequence column of column group should be specified when add column'
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column e int(11) NULL PROPERTIES('sequence_mapping.' = 'e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test sequence mapping value column can not overlap
    errorMessage = 'errCode = 2, detailMessage = columns must belong to exact one column group'
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column (e int(11) NULL, s2 bigint, s3 bigint) PROPERTIES('sequence_mapping.s2' = 'e', 'sequence_mapping.s3' = 'e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test sequence column not exits
    errorMessage = 'errCode = 2, detailMessage = sequence column [s4] in column_group does not belong to current schema and new added columns'
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column e int(11) NULL PROPERTIES('sequence_mapping.s4' = 'e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test sequence column already be mapping column
    errorMessage = 'errCode = 2, detailMessage = column [d] exists but belong to other column group'
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column e int(11) NULL PROPERTIES('sequence_mapping.d' = 'e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test sequence column can not be key column
    errorMessage = "errCode = 2, detailMessage = sequence column [s2] in column_group can't be key column"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column s2 bigint key after b PROPERTIES('sequence_mapping.s2' = 'e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test mapping column not exits
    errorMessage = 'errCode = 2, detailMessage = value column [e] in column_group [s2] does not belong to current schema and new added columns'
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column s2 bigint PROPERTIES('sequence_mapping.s2' = 'e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test mapping column can not be other sequence column
    errorMessage = "errCode = 2, detailMessage = value column [s1] in column_group [s2] exists but it's the sequence column of other column group"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column s2 bigint PROPERTIES('sequence_mapping.s2' = 's1')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test can not change mapping column to other mapping
    errorMessage = "errCode = 2, detailMessage = value column [d] belongs to other column group, can't change to the sequence group [s2]"
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column (e int(11) NULL, s2 bigint) PROPERTIES('sequence_mapping.s2' = 'd,e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test add column must belong to one column mapping
    errorMessage = 'errCode = 2, detailMessage = new column must be a sequence column or belong to a column group: f'
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column (e int(11) NULL, f int(11) NULL, s2 bigint) PROPERTIES('sequence_mapping.s2' = 'e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test sequence column data type should correct
    errorMessage = 'errCode = 2, detailMessage = unsupported data type in sequence column'
    expectException({
        sql initTable
        sql initTableData
        sql """ alter table ${tbName} add column (e int(11) NULL, s2 float) PROPERTIES('sequence_mapping.s2' = 'e')"""
        insertSql = "insert into ${tbName} values(9,9,9,9,9,9);"
        waitForSchemaChangeDone({
            sql getTableStatusSql
            time 600
        }, insertSql, true,"${tbName}")
    }, errorMessage)

    // Test add ok
    sql """ alter table ${tbName} add column (e int(11) NULL, s2 bigint) PROPERTIES('sequence_mapping.s2' = 'e')"""
    insertSql = "insert into ${tbName} values(9,9,9,9,9,9,9), (9,9,10,10,10,10,10), (1,1,2,2,2,2,2), (2,2,3,3,3,3,3), (3,3,4,4,4,4,4);"
    waitForSchemaChangeDone({
        sql getTableStatusSql
        time 600
    }, insertSql, true,"${tbName}")

    sql """ DROP TABLE IF EXISTS ${tbName} """
}
