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
suite("test_materialized_view", "rollup") {
    def tbName1 = "test_materialized_view1"
    def tbName2 = "test_materialized_view2"
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE ${tbName1}(
                record_id int, 
                seller_id int, 
                store_id int, 
                sale_date date, 
                sale_amt bigint
            ) 
            DISTRIBUTED BY HASH(record_id) properties("replication_num" = "1");
        """
    sql """
            CREATE TABLE ${tbName2}(
                record_id int, 
                seller_id int, 
                store_id int, 
                sale_date date, 
                sale_amt bigint
            ) 
            DISTRIBUTED BY HASH(record_id) properties("replication_num" = "1");
        """
    sql "CREATE materialized VIEW amt_sum AS SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id;"
    String res = "null"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName1}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "CREATE materialized VIEW seller_id_order AS SELECT store_id,seller_id, sale_amt FROM ${tbName2} ORDER BY store_id,seller_id;"
    res = "null"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName2}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName1}';"
    sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName2}';"
    qt_sql "DESC ${tbName1} ALL;"
    qt_sql "DESC ${tbName2} ALL;"
    sql "insert into ${tbName1} values(1, 1, 1, '2020-05-30',100);"
    sql "insert into ${tbName1} values(2, 1, 1, '2020-05-30',100);"
    sql "insert into ${tbName2} values(1, 1, 1, '2020-05-30',100);"
    sql "insert into ${tbName2} values(2, 1, 1, '2020-05-30',100);"
    Thread.sleep(5000)
    explain{
        sql("SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id")
        contains("rollup: amt_sum")
    }
    qt_sql "SELECT * FROM ${tbName1}"
    qt_sql "SELECT store_id, sum(sale_amt) FROM ${tbName1} GROUP BY store_id"
    qt_sql "SELECT * FROM ${tbName2}"
    qt_sql "SELECT store_id, sum(sale_amt) FROM ${tbName2} GROUP BY store_id"
    sql "DROP TABLE ${tbName1}"
    sql "DROP TABLE ${tbName2}"
}


