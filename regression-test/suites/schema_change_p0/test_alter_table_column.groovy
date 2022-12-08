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

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                value1 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1", "light_schema_change" = "true");
        """
    sql """
            ALTER TABLE ${tbName1} 
            ADD COLUMN k2 INT KEY AFTER k1,
            ADD COLUMN value2 VARCHAR(255) AFTER value1,
            ADD COLUMN value3 VARCHAR(255) AFTER value2,
            MODIFY COLUMN value2 INT AFTER value3;
        """
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    Thread.sleep(200)
    sql """
            ALTER TABLE ${tbName1}   
            ORDER BY(k1,k2,value1,value2,value3),
            DROP COLUMN value3;
        """
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql "SHOW ALTER TABLE COLUMN;"
    sql "insert into ${tbName1} values(1,1,10,20);"
    sql "insert into ${tbName1} values(1,1,30,40);"
    qt_sql "desc ${tbName1};"
    qt_sql "select * from ${tbName1};"
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
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName2)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql "SHOW ALTER TABLE COLUMN"
    sql "insert into ${tbName2} values(1,1,10,20);"
    sql "insert into ${tbName2} values(1,1,30,40);"
    qt_sql "desc ${tbName2};"
    qt_sql "select * from ${tbName2};"
    sql "DROP TABLE ${tbName2} FORCE;"

    // vector search
    def check_load_result = {checklabel, testTablex ->
        Integer max_try_milli_secs = 10000
        while(max_try_milli_secs) {
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
    def tbName3 = "p_test"
    sql "use test_query_db"
    sql "DROP TABLE IF EXISTS ${tbName3};"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName3} (
                `k1` int(11) NULL COMMENT "",
                `k2` int(11) NULL COMMENT "",
                `v1` int(11) SUM NULL COMMENT ""
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
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
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName3)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    def res3 = sql "select * from ${tbName3} order by k1"
    def res4 = sql "select k1, k2, k3, null from baseall order by k1"
    check2_doris(res3, res4)
    sql "DROP TABLE ${tbName3} FORCE;"
}
