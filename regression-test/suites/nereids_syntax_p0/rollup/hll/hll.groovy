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
suite("hll", "rollup") {
    sql "DROP TABLE IF EXISTS test_materialized_view_hll1"
    sql """
            CREATE TABLE test_materialized_view_hll1(
                record_id int, 
                seller_id int, 
                store_id int, 
                sale_date date, 
                sale_amt bigint
            ) 
            DISTRIBUTED BY HASH(record_id) properties("replication_num" = "1");
        """

    createMV "CREATE materialized VIEW amt_count AS SELECT store_id, hll_union(hll_hash(sale_amt)) FROM test_materialized_view_hll1 GROUP BY store_id;"

    sql "insert into test_materialized_view_hll1 values(1, 1, 1, '2020-05-30',100);"
    sql "insert into test_materialized_view_hll1 values(2, 1, 1, '2020-05-30',100);"
    qt_sql "SELECT store_id, hll_union_agg(hll_hash(sale_amt)) FROM test_materialized_view_hll1 GROUP BY store_id;"

    qt_sql "desc test_materialized_view_hll1 all";

    sql "analyze table test_materialized_view_hll1 with sync;"
    sql """set enable_stats=false;"""

    explain {
        sql("SELECT store_id, hll_union_agg(hll_hash(sale_amt)) FROM test_materialized_view_hll1 GROUP BY store_id;")
        contains "(amt_count)"
    }

    sql """set enable_stats=true;"""
    explain {
        sql("SELECT store_id, hll_union_agg(hll_hash(sale_amt)) FROM test_materialized_view_hll1 GROUP BY store_id;")
        contains "(amt_count)"
    }
}
