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

suite("test_uniq_seq_col_schema_change", "schema_change") {
    def tbName1 = "test_uniq_seq_col_schema_change"
    def columnWithHidden_1 = "(k1,value1,value2,value3,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__)"
    def columnWithHidden_2 = "(k1,value1,value2,value3,value4,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__)"
    def columnWithHidden_3 = "(k1,value2,value4,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__)"
    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                value1 INT,
                value2 INT,
                value3 INT
            )
            UNIQUE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 8
            properties("replication_num" = "1",
                       "light_schema_change" = "false",
                       "function_column.sequence_type" = 'INT');
        """
        // sequence col equal value3
        sql "insert into ${tbName1} ${columnWithHidden_1} values(1,1,1,2,0,2);"
        sql "insert into ${tbName1} ${columnWithHidden_1} values(1,1,1,1,0,1);"
        sql "insert into ${tbName1} ${columnWithHidden_1} values(2,2,2,2,0,2);"
        sql "insert into ${tbName1} ${columnWithHidden_1} values(3,3,3,3,0,3);"
        sql "insert into ${tbName1} ${columnWithHidden_1} values(4,4,4,4,0,4);"
        qt_sql "select * from ${tbName1} order by k1;"

        // alter and test light schema change
        if (!isCloudMode()) {
            sql """ALTER TABLE ${tbName1} SET ("light_schema_change" = "true");"""
        }

        sql "ALTER TABLE ${tbName1} ADD COLUMN value4 INT;"
        qt_sql "select * from ${tbName1} order by k1;"

        sql "insert into ${tbName1} ${columnWithHidden_2}values(5,5,5,5,5,0,5);"
        sql "insert into ${tbName1} ${columnWithHidden_2}values(5,6,6,6,6,0,6);"
        sql "insert into ${tbName1} ${columnWithHidden_2}values(5,6,6,7,6,0,4);"
        qt_sql "select * from ${tbName1} order by k1;"
        sql "insert into ${tbName1} ${columnWithHidden_2}values(5,6,6,7,6,0,7);"
        qt_sql "select * from ${tbName1} order by k1;"

        sql "ALTER TABLE ${tbName1} DROP COLUMN value1;"
        sql "ALTER TABLE ${tbName1} DROP COLUMN value3;"
        qt_sql "select * from ${tbName1} order by k1;"

        // sequence col equal value2
        sql "insert into ${tbName1} ${columnWithHidden_3} values(6,6,6,0,6);"
        sql "insert into ${tbName1} ${columnWithHidden_3} values(6,5,6,0,5);"
        sql "insert into ${tbName1} ${columnWithHidden_3} values(7,1,1,0,1);"
        sql "insert into ${tbName1} ${columnWithHidden_3} values(7,2,1,0,2);"
        qt_sql "select * from ${tbName1} order by k1;"

        sql "insert into ${tbName1} ${columnWithHidden_3} values(2,1,2,0,1);"
        qt_sql "select * from ${tbName1} order by k1;"
        sql "insert into ${tbName1} ${columnWithHidden_3} values(2,3,2,0,3);"
        qt_sql "select * from ${tbName1} order by k1;"
}
