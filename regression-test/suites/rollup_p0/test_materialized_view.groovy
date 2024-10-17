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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_materialized_view") {

    def tbName1 = "test_materialized_view1"
    def tbName2 = "test_materialized_view2"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1}(
                record_id int, 
                seller_id int, 
                store_id int, 
                sale_date date, 
                sale_amt bigint
            ) 
            DISTRIBUTED BY HASH(record_id) properties("replication_num" = "1");
        """
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2}(
                record_id int, 
                seller_id int, 
                store_id int, 
                sale_date date, 
                sale_amt bigint
            ) 
            DISTRIBUTED BY HASH(record_id) properties("replication_num" = "1");
        """
    sql "CREATE materialized VIEW amt_sum AS SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id;"
    int max_try_secs = 60
    Awaitility.await().atMost(max_try_secs, SECONDS).pollInterval(2, SECONDS).until{
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            return true;
        } else {
            return false;
        }
    }
    sql "CREATE materialized VIEW seller_id_order AS SELECT store_id,seller_id, sale_amt FROM ${tbName2} ORDER BY store_id,seller_id;"
    max_try_secs = 60
    Awaitility.await().atMost(max_try_secs, SECONDS).pollInterval(2, SECONDS).until{
        String res = getJobState(tbName2)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            return true;
        } else {
            return false;
        }
    }
    sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName1}';"
    sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName2}';"
    qt_sql "DESC ${tbName1} ALL;"
    qt_sql "DESC ${tbName2} ALL;"
    sql "insert into ${tbName1} values(1, 1, 1, '2020-05-30',100);"
    sql "insert into ${tbName1} values(2, 1, 1, '2020-05-30',100);"
    sql "insert into ${tbName2} values(1, 1, 1, '2020-05-30',100);"
    sql "insert into ${tbName2} values(2, 1, 1, '2020-05-30',100);"
    Thread.sleep(1000)
    explain{
        sql("SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_sum)")
    }
    qt_sql "SELECT * FROM ${tbName1} order by record_id;"
    qt_sql "SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id order by store_id;"
    qt_sql "SELECT * FROM ${tbName2} order by record_id;"
    qt_sql "SELECT store_id, sum(sale_amt) FROM ${tbName2} GROUP BY store_id order by store_id;"

    sql "CREATE materialized VIEW amt_count AS SELECT store_id, count(sale_amt) FROM ${tbName1} GROUP BY store_id;"
    max_try_secs = 60
    Awaitility.await().atMost(max_try_secs, SECONDS).pollInterval(2, SECONDS).until{
        String res = getJobState(tbName1)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            return true;
        }
        return false;
    }
    sql "SELECT store_id, count(sale_amt) FROM ${tbName1} GROUP BY store_id;"
    order_qt_sql "DESC ${tbName1} ALL;"

    qt_sql "SELECT store_id, count(sale_amt) FROM ${tbName1} GROUP BY store_id;"

    explain {
        sql("SELECT store_id, count(sale_amt) FROM ${tbName1} GROUP BY store_id;")
        contains "(amt_count)"
    }

    sql "DROP TABLE ${tbName1} FORCE;"
    sql "DROP TABLE ${tbName2} FORCE;"
}
