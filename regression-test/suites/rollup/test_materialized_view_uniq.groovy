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
suite("test_materialized_view_uniq", "rollup") {
    def tbName1 = "test_materialized_view1"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE ${tbName1}(
                record_id int, 
                seller_id int, 
                store_id int, 
                sale_date date, 
                sale_amt bigint
            ) unique KEY (record_id,seller_id,store_id)
            DISTRIBUTED BY HASH(record_id) properties("replication_num" = "1");
        """
    try_sql "CREATE materialized VIEW amt_sum AS SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id;"

    sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName1}';"
    qt_sql "DESC ${tbName1} ALL;"
    sql "insert into ${tbName1} values(1, 1, 1, '2020-05-30',100);"
    sql "insert into ${tbName1} values(2, 1, 1, '2020-05-30',100);"
    Thread.sleep(1000)
    qt_sql "SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id;"
    qt_sql "SELECT * FROM ${tbName1} order by record_id;"
    qt_sql "SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id order by store_id;"

    sql "CREATE materialized VIEW amt_count AS SELECT record_id, seller_id, store_id, sale_amt FROM ${tbName1};"
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
    sql "SELECT store_id, count(sale_amt) FROM ${tbName1} GROUP BY store_id;"
    qt_sql "DESC ${tbName1} ALL;"
    sql "DROP TABLE ${tbName1} FORCE;"
}
