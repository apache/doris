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
suite("test_bitmap_index") {
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
               k11 BOOLEAN,
               k12 DATEV2,
               k13 DATETIMEV2,
               k14 DATETIMEV2(3),
               k15 DATETIMEV2(6)
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
                ADD INDEX index11 (k11) USING BITMAP,
                ADD INDEX index12 (k12) USING BITMAP,
                ADD INDEX index13 (k13) USING BITMAP,
                ADD INDEX index14 (k14) USING BITMAP,
                ADD INDEX index15 (k15) USING BITMAP;
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

    sql "insert into ${tbName1} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1,'2022-05-31','2022-05-31 10:00:00.111111','2022-05-31 10:00:00.111111','2022-05-31 10:00:00.111111');"
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
               k12 DATEV2,
               k13 DATETIMEV2,
               k14 DATETIMEV2(3),
               k15 DATETIMEV2(6),
                v1 INT SUM
            )
            AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13,k14,k15)
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
                ADD INDEX index11 (k11) USING BITMAP,
                ADD INDEX index12 (k12) USING BITMAP,
                ADD INDEX index13 (k13) USING BITMAP,
                ADD INDEX index14 (k14) USING BITMAP,
                ADD INDEX index15 (k15) USING BITMAP;
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
        sql "ALTER TABLE ${tbName2} ADD INDEX index16 (v1) USING BITMAP;"
        exception "errCode = 2, detailMessage = BITMAP index only used in columns of DUP_KEYS/UNIQUE_KEYS table"
    }

    sql "insert into ${tbName2} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1,'2022-05-31','2022-05-31 10:00:00.111111','2022-05-31 10:00:00.111111','2022-05-31 10:00:00.111111',1);"
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
               k12 DATEV2,
               k13 DATETIMEV2,
               k14 DATETIMEV2(3),
               k15 DATETIMEV2(6),
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
                ADD INDEX index12 (k12) USING BITMAP,
                ADD INDEX index13 (k13) USING BITMAP,
                ADD INDEX index14 (k14) USING BITMAP,
                ADD INDEX index15 (k15) USING BITMAP,
                ADD INDEX index16 (v1) USING BITMAP;
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

    sql "insert into ${tbName3} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1,'2022-05-31','2022-05-31 10:00:00.111111','2022-05-31 10:00:00.111111','2022-05-31 10:00:00.111111',1);"
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

    // test bitmap index on MOR, and delete row after insert
    def tbName4 = "test_bitmap_index_unique_mor_delete"
        sql "DROP TABLE IF EXISTS ${tbName4}"
        sql """
                CREATE TABLE ${tbName4} (
                    create_time datetime NOT NULL COMMENT '',
                    vid varchar(64) NOT NULL COMMENT '',
                    report_time datetime NULL COMMENT '',
                    block_version int(11) NULL COMMENT '',
                    vehicle_mode int(11) NULL COMMENT '',
                    usage_mode int(11) NULL COMMENT ''
                ) ENGINE=OLAP
                UNIQUE KEY(create_time, vid, report_time)
                DISTRIBUTED BY HASH(vid) BUCKETS AUTO
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "is_being_synced" = "false",
                "storage_format" = "V2",
                "enable_unique_key_merge_on_write" = "false",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "false",
                "enable_single_replica_compaction" = "false"
                );
            """
            // test mor table

        sql """
                ALTER TABLE ${tbName4} ADD INDEX vid_bitmap_index (vid) USING BITMAP;
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

        sql "insert into ${tbName4}(create_time,vid,report_time,block_version,vehicle_mode,usage_mode) values('2023-08-25 10:00:00','123','2023-08-25 10:00:00',1,1,1);"
        sql "insert into ${tbName4}(create_time,vid,report_time,block_version,vehicle_mode,usage_mode) values('2023-08-25 11:00:00','123','2023-08-25 11:00:00',2,2,2);"
        sql "insert into ${tbName4}(create_time,vid,report_time,block_version,vehicle_mode,usage_mode) values('2023-08-25 12:00:00','123','2023-08-25 12:00:00',3,3,3);"
        qt_sql "desc ${tbName4};"
        qt_sql "SHOW INDEX FROM ${tbName4};"
        sql "delete from ${tbName4} where vid='123' and report_time='2023-08-25 12:00:00' and create_time='2023-08-25 12:00:00';"
        qt_sql "select count(*) from ${tbName4}; "
        qt_sql "select count(*) from ${tbName4} where vid='123'; "
        qt_sql "select count(*) from ${tbName4} where create_time>='2023-08-25 10:00:00';"
        qt_sql "select count(CASE when vid='123' then 1 else null end) from ${tbName4} where vid='123';"
        qt_sql "select * from ${tbName4} where vid='123' order by create_time;"

        sql "DROP INDEX IF EXISTS index1 ON ${tbName4};"
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
        sql "DROP TABLE ${tbName4} FORCE;"

        // test bitmap index on MOW, and delete row after insert
        def tbName5 = "test_bitmap_index_unique_mow_delete"
        sql "DROP TABLE IF EXISTS ${tbName5}"
        sql """
                CREATE TABLE ${tbName5} (
                    create_time datetime NOT NULL COMMENT '',
                    vid varchar(64) NOT NULL COMMENT '',
                    report_time datetime NULL COMMENT '',
                    block_version int(11) NULL COMMENT '',
                    vehicle_mode int(11) NULL COMMENT '',
                    usage_mode int(11) NULL COMMENT ''
                ) ENGINE=OLAP
                UNIQUE KEY(create_time, vid, report_time)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(vid) BUCKETS AUTO
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "is_being_synced" = "false",
                "storage_format" = "V2",
                "light_schema_change" = "true",
                "disable_auto_compaction" = "false",
                "enable_unique_key_merge_on_write" = "true",
                "enable_single_replica_compaction" = "false"
                );
            """

        sql """
                ALTER TABLE ${tbName5} ADD INDEX vid_bitmap_index (vid) USING BITMAP;
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

        sql "insert into ${tbName5}(create_time,vid,report_time,block_version,vehicle_mode,usage_mode) values('2023-08-25 10:00:00','123','2023-08-25 10:00:00',1,1,1);"
        sql "insert into ${tbName5}(create_time,vid,report_time,block_version,vehicle_mode,usage_mode) values('2023-08-25 11:00:00','123','2023-08-25 11:00:00',2,2,2);"
        sql "insert into ${tbName5}(create_time,vid,report_time,block_version,vehicle_mode,usage_mode) values('2023-08-25 12:00:00','123','2023-08-25 12:00:00',3,3,3);"
        qt_sql "desc ${tbName5};"
        qt_sql "SHOW INDEX FROM ${tbName5};"
        sql "delete from ${tbName5} where vid='123' and report_time='2023-08-25 12:00:00' and create_time='2023-08-25 12:00:00';"
        qt_sql "select count(*) from ${tbName5}; "
        qt_sql "select count(*) from ${tbName5} where vid='123'; "
        qt_sql "select count(*) from ${tbName5} where create_time>='2023-08-25 10:00:00';"
        qt_sql "select count(CASE when vid='123' then 1 else null end) from ${tbName5} where vid='123';"
        qt_sql "select * from ${tbName5} where vid='123' order by create_time;"

        sql "DROP INDEX IF EXISTS index1 ON ${tbName5};"
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
        sql "DROP TABLE ${tbName5} FORCE;"
}
