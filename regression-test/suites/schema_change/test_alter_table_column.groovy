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

suite("test_alter_table_column", "schema_change") {
    def tbName1 = "alter_table_column_dup"
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                value1 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """
    String res = "null"
    sql "ALTER TABLE ${tbName1} ADD COLUMN k2 INT KEY AFTER k1"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        Thread.sleep(1000)
    }
    res = "null"
    sql "ALTER TABLE ${tbName1} ADD COLUMN value2 VARCHAR(255) AFTER value1"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        Thread.sleep(1000)
    }
    res = "null"
    sql "ALTER TABLE ${tbName1} ADD COLUMN value3 VARCHAR(255) AFTER value2"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        Thread.sleep(1000)
    }
    res = "null"
    sql "ALTER TABLE ${tbName1} MODIFY COLUMN value2 INT AFTER value3;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        Thread.sleep(1000)
    }
    res = "null"
    sql "ALTER TABLE ${tbName1} ORDER BY(k1,k2,value1,value2,value3);"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        Thread.sleep(1000)
    }
    res = "null"
    sql "ALTER TABLE ${tbName1} DROP COLUMN value3;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        Thread.sleep(1000)
    }
    sql "SHOW ALTER TABLE COLUMN"
    sql "insert into ${tbName1} values(1,1,10,20);"
    sql "insert into ${tbName1} values(1,1,30,40);"
    qt_sql "desc ${tbName1}"
    qt_sql "select * from ${tbName1}"
    sql "DROP TABLE ${tbName1}"

    def tbName2 = "alter_table_column_agg"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2} (
                k1 INT,
                value1 INT SUM
            )
            AGGREGATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "ALTER TABLE ${tbName2} ADD COLUMN k2 INT KEY AFTER k1"
    res = "null"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        Thread.sleep(1000)
    }
    res = "null"
    sql "ALTER TABLE ${tbName2} ADD COLUMN value2 INT SUM AFTER value1"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        Thread.sleep(1000)
    }
    sql "SHOW ALTER TABLE COLUMN"
    sql "insert into ${tbName2} values(1,1,10,20);"
    sql "insert into ${tbName2} values(1,1,30,40);"
    qt_sql "desc ${tbName2}"
    qt_sql "select * from ${tbName2}"
    sql "DROP TABLE ${tbName2}"
}


