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
suite("test_bitmap_index", "inverted_index") {
    def tbName1 = "test_bitmap_index_dup"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR(1),
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
               k10 DECIMAL,
               k11 BOOLEAN
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """

    sql """
            ALTER TABLE ${tbName1}
                ADD INDEX index1 (k1) USING BITMAP,
                ADD INDEX index2 (k2) USING BITMAP,
                ADD INDEX index3 (k3) USING BITMAP,
                ADD INDEX index4 (k4) USING BITMAP,
                ADD INDEX index5 (k5) USING BITMAP,
                ADD INDEX index6 (k6) USING BITMAP,
                ADD INDEX index7 (k7) USING BITMAP,
                ADD INDEX index8 (k8) USING BITMAP,
                ADD INDEX index9 (k9) USING BITMAP,
                ADD INDEX index10 (k10) USING BITMAP,
                ADD INDEX index11 (k11) USING BITMAP;
        """
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(1000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED", res)
            }
        }
    }

    sql "insert into ${tbName1} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1);"
    qt_sql "desc ${tbName1};"
    qt_sql "SHOW INDEX FROM ${tbName1};"
    qt_sql "select * from ${tbName1};"

    sql "DROP INDEX IF EXISTS index1 ON ${tbName1};"
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(1000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED", res)
            }
        }
    }
    sql "DROP TABLE ${tbName1} FORCE;"


    def tbName2 = "test_bitmap_index_agg"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR(1),
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
               k10 DECIMAL,
               k11 BOOLEAN,
                v1 INT SUM
            )
            AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """

    sql """
            ALTER TABLE ${tbName2}
                ADD INDEX index1 (k1) USING BITMAP,
                ADD INDEX index2 (k2) USING BITMAP,
                ADD INDEX index3 (k3) USING BITMAP,
                ADD INDEX index4 (k4) USING BITMAP,
                ADD INDEX index5 (k5) USING BITMAP,
                ADD INDEX index6 (k6) USING BITMAP,
                ADD INDEX index7 (k7) USING BITMAP,
                ADD INDEX index8 (k8) USING BITMAP,
                ADD INDEX index9 (k9) USING BITMAP,
                ADD INDEX index10 (k10) USING BITMAP,
                ADD INDEX index11 (k11) USING BITMAP;
        """
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName2)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(1000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    test{
        sql "ALTER TABLE ${tbName2} ADD INDEX index12 (v1) USING BITMAP;"
        exception "errCode = 2, detailMessage = BITMAP index only used in columns of DUP_KEYS/UNIQUE_KEYS table or key columns of AGG_KEYS table. invalid column: v1"
    }

    sql "insert into ${tbName2} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1,1);"
    qt_sql "desc ${tbName2};"
    qt_sql "SHOW INDEX FROM ${tbName2};"
    qt_sql "select * from ${tbName2};"

    sql "DROP INDEX IF EXISTS index1 ON ${tbName2};"
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName2)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(1000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    sql "DROP TABLE ${tbName2} FORCE;"

    def tbName3 = "test_bitmap_index_unique"
    sql "DROP TABLE IF EXISTS ${tbName3}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName3} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR(1),
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
               k10 DECIMAL,
               k11 BOOLEAN,
               v1  INT
            )
            UNIQUE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
        """

    sql """
            ALTER TABLE ${tbName3}
                ADD INDEX index1 (k1) USING BITMAP,
                ADD INDEX index2 (k2) USING BITMAP,
                ADD INDEX index3 (k3) USING BITMAP,
                ADD INDEX index4 (k4) USING BITMAP,
                ADD INDEX index5 (k5) USING BITMAP,
                ADD INDEX index6 (k6) USING BITMAP,
                ADD INDEX index7 (k7) USING BITMAP,
                ADD INDEX index8 (k8) USING BITMAP,
                ADD INDEX index9 (k9) USING BITMAP,
                ADD INDEX index10 (k10) USING BITMAP,
                ADD INDEX index11 (k11) USING BITMAP,
                ADD INDEX index12 (v1) USING BITMAP;
        """
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(1000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql "insert into ${tbName3} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1,1);"
    qt_sql "desc ${tbName3};"
    qt_sql "SHOW INDEX FROM ${tbName3};"
    qt_sql "select * from ${tbName3};"

    sql "DROP INDEX IF EXISTS index1 ON ${tbName3};"
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName3)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(1000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    sql "DROP TABLE ${tbName3} FORCE;"
}
