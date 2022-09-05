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

suite("test_alter_table_column_with_delete") {
    def tbName1 = "alter_table_column_dup_with_delete"
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
            UNIQUE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1");
        """

    sql "insert into ${tbName1} values(1,1);"
    sql "insert into ${tbName1} values(2,2);"
    sql "delete from ${tbName1} where k1 = 2;"
    sql "insert into ${tbName1} values(3,3);"
    sql "insert into ${tbName1} values(4,4);"
    qt_sql "select * from ${tbName1} order by k1;"


    sql """
            ALTER TABLE ${tbName1} 
            MODIFY COLUMN value1 varchar(22);
        """
    int max_try_secs = 120
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(500)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql "insert into ${tbName1} values(5,'abc');"
    qt_sql "select * from ${tbName1} order by k1;"
    sql "DROP TABLE ${tbName1} FORCE;"
}
