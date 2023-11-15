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

suite("test_analyze_with_agg_complex_type") {
    sql """drop table if exists test_agg_complex_type;"""

    sql """create table test_agg_complex_type (
            datekey int,
            device_id bitmap BITMAP_UNION NULL,
                    hll_test hll hll_union,
                    qs QUANTILE_STATE QUANTILE_UNION
    )
    aggregate key (datekey)
    distributed by hash(datekey) buckets 1
    properties(
            "replication_num" = "1"
    );"""

    sql """insert into test_agg_complex_type values (1,to_bitmap(1), hll_hash("11"), TO_QUANTILE_STATE("11", 1.0));"""
    
    sql """insert into test_agg_complex_type values (2, to_bitmap(1),  hll_hash("12"), TO_QUANTILE_STATE("11", 1.0));"""
    
    sql """ANALYZE TABLE test_agg_complex_type WITH SYNC"""

    def show_result = sql """SHOW COLUMN CACHED STATS test_agg_complex_type"""

    assert show_result.size() == 1

    def expected_col_stats = { r, expected_value, idx ->
        return (int) Double.parseDouble(r[0][idx]) == expected_value
    }

    assert expected_col_stats(show_result, 2, 1)
    assert expected_col_stats(show_result, 0, 3)
    assert expected_col_stats(show_result, 8, 4)
    assert expected_col_stats(show_result, 4, 5)
    assert expected_col_stats(show_result, 1, 6)
    assert expected_col_stats(show_result, 2, 7)
}