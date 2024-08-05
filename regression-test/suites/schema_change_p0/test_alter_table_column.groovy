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

suite("test_alter_table_column") {
    def tbName1 = "alter_table_column_dup"

    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                value1 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1", "light_schema_change" = "false");
        """

    // alter and test light schema change
    if (!isCloudMode()) {
        sql """ALTER TABLE ${tbName1} SET ("light_schema_change" = "true");"""
    }

    sql """
            ALTER TABLE ${tbName1} 
            ADD COLUMN k2 INT KEY AFTER k1,
            ADD COLUMN value2 VARCHAR(255) AFTER value1,
            ADD COLUMN value3 VARCHAR(255) AFTER value2,
            MODIFY COLUMN value2 INT AFTER value3;
        """

    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    sql """
            ALTER TABLE ${tbName1}   
            ORDER BY(k1,k2,value1,value2,value3),
            DROP COLUMN value3;
        """

    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName1}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    sql "SHOW ALTER TABLE COLUMN;"
    sql "insert into ${tbName1} values(1,1,10,20);"
    sql "insert into ${tbName1} values(1,1,30,40);"
    qt_sql "desc ${tbName1};"
    qt_sql "select * from ${tbName1} order by value1;"
    sql "DROP TABLE ${tbName1} FORCE;"

    def tbName2 = "alter_table_column_agg"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2} (
                k1 INT,
                value1 INT SUM
            )
            AGGREGATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1", "light_schema_change" = "true");
        """
    sql """
            ALTER TABLE ${tbName2} 
            ADD COLUMN k2 INT KEY AFTER k1,
            ADD COLUMN value2 INT SUM AFTER value1;
        """

    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName2}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    sql "SHOW ALTER TABLE COLUMN"
    sql "insert into ${tbName2} values(1,1,10,20);"
    sql "insert into ${tbName2} values(1,1,30,40);"
    qt_sql "desc ${tbName2};"
    qt_sql "select * from ${tbName2} order by value2;"
    sql "DROP TABLE ${tbName2} FORCE;"

    def tbNameAddArray = "alter_table_add_array_column_dup"
    sql "DROP TABLE IF EXISTS ${tbNameAddArray}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbNameAddArray} (
                k1 INT,
                value1 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties(
                "replication_num" = "1", 
                "light_schema_change" = "true",
                "disable_auto_compaction" = "true");
        """

    sql "insert into ${tbNameAddArray} values(1,2)"
    sql "insert into ${tbNameAddArray} values(3,4)"
    sql """
            ALTER TABLE ${tbNameAddArray} 
            ADD COLUMN value2 ARRAY<INT> DEFAULT '[]' AFTER value1,
            ADD COLUMN value3 ARRAY<INT> AFTER value2,
            ADD COLUMN value4 ARRAY<INT> NOT NULL DEFAULT '[]' AFTER value3;
        """

    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbNameAddArray}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }
    qt_sql "desc ${tbNameAddArray};"
    qt_sql "select * from ${tbNameAddArray} order by k1;"
    sql "DROP TABLE ${tbNameAddArray} FORCE;"

    // vector search
    def check_load_result = {checklabel, testTablex ->
        Integer max_try_milli_secs = 10000
        while (max_try_milli_secs) {
            def result = sql "show load where label = '${checklabel}'"
            if(result[0][2] == "FINISHED") {
                qt_select "select * from ${testTablex} order by k1"
                break
            } else {
                sleep(1000) // wait 1 second every time
                max_try_milli_secs -= 1000
                if(max_try_milli_secs <= 0) {
                    assertEquals(1, 2)
                }
            }
        }
    }

    sql "DROP TABLE IF EXISTS baseall"
    sql """
        CREATE TABLE IF NOT EXISTS `baseall` (
            `k0` boolean null comment "",
            `k1` tinyint(4) null comment "",
            `k2` smallint(6) null comment "",
            `k3` int(11) null comment "",
            `k4` bigint(20) null comment "",
            `k5` decimal(9, 3) null comment "",
            `k6` char(5) null comment "",
            `k10` date null comment "",
            `k11` datetime null comment "",
            `k7` varchar(20) null comment "",
            `k8` double max null comment "",
            `k9` float sum null comment "",
            `k12` string replace null comment "",
            `k13` largeint(40) replace null comment ""
        ) engine=olap
        DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
        """

    streamLoad {
        table "baseall"
        set 'column_separator', ','
        file "baseall.txt"
    }
    sql "sync"

    def tbName3 = "p_test"
    sql "DROP TABLE IF EXISTS ${tbName3};"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName3} (
                `k1` int(11) NULL COMMENT "",
                `k2` int(11) NULL COMMENT "",
                `v1` int(11) SUM NULL COMMENT ""
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
                "storage_type" = "COLUMN",
                "replication_num" = "1"
            );
        """
    def label = UUID.randomUUID().toString()
    sql """
            INSERT INTO ${tbName3} WITH LABEL `${label}` SELECT k1, k2, k3 FROM baseall;
        """
    check_load_result.call(label, tbName3)

    def res1 = sql "select * from ${tbName3} order by k1"
    def res2 = sql "select k1, k2, k3 from baseall order by k1"
    check2_doris(res1, res2)

    sql "alter table ${tbName3} add column v2 int sum NULL"


    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName3}' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    def res3 = sql "select * from ${tbName3} order by k1"
    def res4 = sql "select k1, k2, k3, null from baseall order by k1"
    check2_doris(res3, res4)
    sql "DROP TABLE ${tbName3} FORCE;"

}
