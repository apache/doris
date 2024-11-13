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
suite("hll_with_light_sc", "rollup") {
    
    sql "DROP TABLE IF EXISTS test_materialized_view_hll_with_light_sc1"
    sql """
            CREATE TABLE test_materialized_view_hll_with_light_sc1(
                record_id int, 
                seller_id int, 
                store_id int, 
                sale_date date, 
                sale_amt bigint
            ) 
            DISTRIBUTED BY HASH(record_id) properties("replication_num" = "1", "light_schema_change" = "true");
        """

    createMV "CREATE materialized VIEW amt_count1 AS SELECT store_id, hll_union(hll_hash(sale_amt)) FROM test_materialized_view_hll_with_light_sc1 GROUP BY store_id;"

    qt_sql "desc test_materialized_view_hll_with_light_sc1 all";

    sql "insert into test_materialized_view_hll_with_light_sc1 values(1, 1, 1, '2020-05-30',100);"
    sql "insert into test_materialized_view_hll_with_light_sc1 values(2, 1, 1, '2020-05-30',100);"
    qt_sql "SELECT store_id, hll_union_agg(hll_hash(sale_amt)) FROM test_materialized_view_hll_with_light_sc1 GROUP BY store_id;"

    sql "analyze table test_materialized_view_hll_with_light_sc1 with sync;"
    sql """set enable_stats=false;"""

    mv_rewrite_success("SELECT store_id, hll_union_agg(hll_hash(sale_amt)) FROM test_materialized_view_hll_with_light_sc1 GROUP BY store_id;",
            "amt_count1")

    sql """set enable_stats=true;"""
    sql """alter table test_materialized_view_hll_with_light_sc1 modify column record_id set stats ('row_count'='2');"""
    mv_rewrite_success("SELECT store_id, hll_union_agg(hll_hash(sale_amt)) FROM test_materialized_view_hll_with_light_sc1 GROUP BY store_id;",
            "amt_count1")
}
