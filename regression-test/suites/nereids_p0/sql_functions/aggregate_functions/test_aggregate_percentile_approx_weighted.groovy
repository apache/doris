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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_aggregate_percentile_approx_weighted") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"


    sql "DROP TABLE IF EXISTS quantile_weighted_table"

    sql """
      create table quantile_weighted_table (
        k int,
        w int
      )
      DUPLICATE key (k)
      distributed by hash(k) buckets 1
      properties(
        "replication_num" = "1"
      );
    """
    sql """insert into quantile_weighted_table values(1,10),(2,6),(3,4),(4,2),(5,2),(6,1),(5,4);"""
    
    qt_select """
        select 
        percentile_approx_weighted(k,w,0.1),
        percentile_approx_weighted(k,w,0.35),
        percentile_approx_weighted(k,w,0.55),
        percentile_approx_weighted(k,w,0.78),
        percentile_approx_weighted(k,w,0.99)
        from quantile_weighted_table;
    """

    qt_select """
        select 
        percentile_approx_weighted(k,w,0.1,2048),
        percentile_approx_weighted(k,w,0.35,2048),
        percentile_approx_weighted(k,w,0.55,2048),
        percentile_approx_weighted(k,w,0.78,2048),
        percentile_approx_weighted(k,w,0.99,2048)
        from quantile_weighted_table;
    """
    sql """insert into quantile_weighted_table values(7,0),(8,-1);"""
    qt_select_3 """
        select 
        percentile_approx_weighted(k,w,0.1,2048),
        percentile_approx_weighted(k,w,0.35,2048),
        percentile_approx_weighted(k,w,0.55,2048),
        percentile_approx_weighted(k,w,0.78,2048),
        percentile_approx_weighted(k,w,0.99,2048)
        from quantile_weighted_table;
    """
}