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
import java.util.concurrent.TimeUnit

suite("date", "rollup") {

    sql """set enable_nereids_planner=true"""
    sql "SET enable_fallback_to_original_planner=false"

    def tbName1 = "test_materialized_view_date1"

    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1}(
                record_id int, 
                seller_id int, 
                store_id int, 
                sale_date date,
                sale_date1 datev2,
                sale_datetime1 datetimev2,
                sale_datetime2 datetimev2(3),
                sale_datetime3 datetimev2(6),
                sale_amt bigint
            ) 
            DISTRIBUTED BY HASH(record_id) properties("replication_num" = "1");
        """

    createMV("CREATE materialized VIEW amt_max1 AS SELECT store_id, max(sale_date1) FROM ${tbName1} GROUP BY store_id;")
    createMV("CREATE materialized VIEW amt_max2 AS SELECT store_id, max(sale_datetime1) FROM ${tbName1} GROUP BY store_id;")
    createMV("CREATE materialized VIEW amt_max3 AS SELECT store_id, max(sale_datetime2) FROM ${tbName1} GROUP BY store_id;")
    createMV("CREATE materialized VIEW amt_max4 AS SELECT store_id, max(sale_datetime3) FROM ${tbName1} GROUP BY store_id;")


    sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName1}';"
    sql "insert into ${tbName1} values(1, 1, 1, '2020-05-30', '2020-05-30', '2020-05-30 11:11:11.111111', '2020-05-30 11:11:11.111111', '2020-05-30 11:11:11.111111',100);"
    sql "insert into ${tbName1} values(2, 1, 1, '2020-05-30', '2020-05-30', '2020-04-30 11:11:11.111111', '2020-04-30 11:11:11.111111', '2020-04-30 11:11:11.111111',100);"
    Thread.sleep(2000)

    sql "analyze table ${tbName1} with sync;"
    sql """set enable_stats=false;"""

    explain{
        sql("SELECT store_id, max(sale_date1) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_max1)")
    }
    explain{
        sql("SELECT store_id, max(sale_datetime1) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_max2)")
    }
    explain{
        sql("SELECT store_id, max(sale_datetime2) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_max3)")
    }
    explain{
        sql("SELECT store_id, max(sale_datetime3) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_max4)")
    }
    sql """set enable_stats=true;"""
    explain{
        sql("SELECT store_id, max(sale_date1) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_max1)")
    }
    explain{
        sql("SELECT store_id, max(sale_datetime1) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_max2)")
    }
    explain{
        sql("SELECT store_id, max(sale_datetime2) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_max3)")
    }
    explain{
        sql("SELECT store_id, max(sale_datetime3) FROM ${tbName1} GROUP BY store_id")
        contains("(amt_max4)")
    }

    qt_sql """ SELECT store_id, max(sale_date1) FROM ${tbName1} GROUP BY store_id """
    qt_sql """ SELECT store_id, max(sale_datetime1) FROM ${tbName1} GROUP BY store_id """
    qt_sql """ SELECT store_id, max(sale_datetime2) FROM ${tbName1} GROUP BY store_id """
    qt_sql """ SELECT store_id, max(sale_datetime3) FROM ${tbName1} GROUP BY store_id """

    sql "DROP TABLE ${tbName1} FORCE;"
}
