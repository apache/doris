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

suite("test_alter_table_column_with_delete_drop_column_dup_key", "schema_change") {
    def tbName1 = "alter_table_column_dup_with_delete_drop_column_dup_key"
    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

//=========================Test Normal Schema Change
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                value1 INT,
                value2 INT,
                value3 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1", "light_schema_change" = "false", "disable_auto_compaction" = "true");
        """
    // delete value3 = 2
    sql "insert into ${tbName1} values(1,1,1,1);"
    sql "insert into ${tbName1} values(2,2,2,2);"
    sql "delete from ${tbName1} where value3 = 2;"
    sql "insert into ${tbName1} values(3,3,3,3);"
    sql "insert into ${tbName1} values(4,4,4,4);"
    qt_sql "select * from ${tbName1} order by k1;"

    // drop value3
    sql """
            ALTER TABLE ${tbName1} 
            DROP COLUMN value3;
        """
    int max_try_secs = 1200
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(100)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    qt_sql "select * from ${tbName1} order by k1;"

     // drop value3
    sql """
            ALTER TABLE ${tbName1} 
            ADD COLUMN value3 CHAR(100) DEFAULT 'A';
        """
    max_try_secs = 1200
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(100)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    qt_sql "select * from ${tbName1} order by k1;"

    sql "insert into ${tbName1} values(5,5,5,'B');"
    qt_sql "select * from ${tbName1} order by k1;"
    sql "DROP TABLE ${tbName1} FORCE;"

//======================= Test Light Weight Schema Change 
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                value1 INT,
                value2 INT,
                value3 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1", "light_schema_change" = "false", "disable_auto_compaction" = "true");
        """

    sql "insert into ${tbName1} values(1,1,1,1);"
    sql "insert into ${tbName1} values(2,2,2,2);"
    qt_sql "select * from ${tbName1} where value2=2 order by k1;"

    // test alter light schema change by the way
    if (!isCloudMode()) {
        sql """ALTER TABLE ${tbName1} SET ("light_schema_change" = "true");"""
    }

    // delete value3 = 2
    sql "delete from ${tbName1} where value3 = 2;"
    sql "insert into ${tbName1} values(3,3,3,3);"
    sql "insert into ${tbName1} values(4,4,4,4);"
    qt_sql "select * from ${tbName1} where value3=3 order by k1;"

    // drop value3
    sql """
            ALTER TABLE ${tbName1} 
            DROP COLUMN value3;
        """
    max_try_secs = 1200
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(100)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    qt_sql "select * from ${tbName1} where value1=3 order by k1;"

    // drop value3
    sql """
            ALTER TABLE ${tbName1} 
            ADD COLUMN value3 CHAR(100) DEFAULT 'A';
        """
    max_try_secs = 1200
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(100)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    qt_sql "select * from ${tbName1} where value1=4 order by k1;"

    sql "insert into ${tbName1} values(5,5,5,'B');"
    qt_sql "select * from ${tbName1} order by k1;"

    // Do schema change that not do light weight schema change
    sql """
            ALTER TABLE ${tbName1} 
            ADD COLUMN k2 CHAR(10) KEY DEFAULT 'A';
        """
    max_try_secs = 1200
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(100)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    qt_sql "select * from ${tbName1} where value1=4 order by k1;"
    sql "DROP TABLE ${tbName1} FORCE;"

//======================= Test Light Weight Schema Change  with Compaction
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                value1 INT,
                value2 INT,
                value3 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1", "light_schema_change" = "false", "disable_auto_compaction" = "false");
        """

    sql "insert into ${tbName1} values(1,1,1,1);"
    sql "insert into ${tbName1} values(2,2,2,2);"
    qt_sql "select * from ${tbName1} order by k1;"

    // test alter light schema change by the way
    if (!isCloudMode()) {
        sql """ALTER TABLE ${tbName1} SET ("light_schema_change" = "true");"""
    }

    // delete value3 = 2
    sql "delete from ${tbName1} where value3 = 2;"
    sql "insert into ${tbName1} values(3,3,3,3);"
    sql "insert into ${tbName1} values(4,4,4,4);"
    qt_sql "select * from ${tbName1} order by k1;"

    // drop value3
    sql """
            ALTER TABLE ${tbName1} 
            DROP COLUMN value3;
        """
    max_try_secs = 1200
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(100)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    qt_sql "select * from ${tbName1} order by k1;"

     // drop value3
    sql """
            ALTER TABLE ${tbName1} 
            ADD COLUMN value3 CHAR(100) DEFAULT 'A';
        """
    max_try_secs = 1200
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(100)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    qt_sql "select * from ${tbName1} order by k1;"

    sql "insert into ${tbName1} values(5,5,5,'B');"
    sql "insert into ${tbName1} values(5,5,5,'B');"
    sql "insert into ${tbName1} values(5,5,5,'B');"
    sql "insert into ${tbName1} values(5,5,5,'B');"
    sql "insert into ${tbName1} values(5,5,5,'B');"
    sql "insert into ${tbName1} values(5,5,5,'B');"
    sql "insert into ${tbName1} values(5,5,5,'B');"

    Thread.sleep(5000)
    qt_sql "select * from ${tbName1} order by k1;"
    sql "DROP TABLE ${tbName1} FORCE;"

}
