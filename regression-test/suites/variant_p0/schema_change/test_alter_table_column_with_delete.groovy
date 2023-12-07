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

suite("test_alter_variant_table_column_with_delete") {
    def tbName1 = "alter_table_column_dup_with_delete"
    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 INT,
                v variant,
                vv double 
            )
            UNIQUE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
        """

    sql """insert into ${tbName1} values(1,'{"a":1, "b":2, "c":3}',1);"""
    sql """insert into ${tbName1} values(2,'{"d":4, "e":5, "f":6}',2);"""
    sql """delete from ${tbName1} where k1 = 2;"""
    sql """insert into ${tbName1} values(3,'{"g":7, "h":8, "i":9}',3);"""
    sql """insert into ${tbName1} values(4,'{"j":10, "k":11, "l":12}',4);"""
    qt_sql """select * from ${tbName1} order by k1;"""


    sql """
            ALTER TABLE ${tbName1} 
            MODIFY COLUMN vv text;
        """
    int max_try_secs = 120
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED") {
            sleep(3000)
            break
        } else {
            Thread.sleep(500)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql """insert into ${tbName1} values(2,'{"x":4, "y":5, "f":3}',2);"""
    qt_sql "select * from ${tbName1} order by k1;"
    //sql "DROP TABLE ${tbName1} FORCE;"
}
