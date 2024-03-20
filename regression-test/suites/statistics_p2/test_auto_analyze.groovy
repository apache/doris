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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.

suite("test_auto_analyze", "p2,auto_analyze_foundational_test") {
    def init_property = {
        sql """SET enable_nereids_planner=true;"""
        sql """SET enable_fallback_to_original_planner=false;"""
        sql """SET forbid_unknown_col_stats=true;"""

        sql """set global enable_auto_analyze=true;"""
        sql """set global auto_analyze_start_time="00:00:00";"""
        sql """set global auto_analyze_end_time="23:59:59";"""

        sql """SET query_timeout=180000;"""
    }
    init_property()

    def test_auto_analyze = {
        def auto_mark = false
        def max_wait_time_in_minutes = 21 * 60
        def waiting_time = 0
        def internal_sleep = 1
        while (waiting_time < max_wait_time_in_minutes) {
            waiting_time += internal_sleep
            def auto_analyze_origin_List = sql """show auto analyze;"""
            if (auto_analyze_origin_List == [] || auto_analyze_origin_List.size == 0) {
                sleep(internal_sleep * 60 * 1000)
                continue
            }
            if (auto_analyze_origin_List.size > 0) {
                for (int i = auto_analyze_origin_List.size()-1; i >= 0; i--) {
                    def stats = auto_analyze_origin_List[i][9]
                    if (stats == "FINISHED") {
                        def catalog_name = auto_analyze_origin_List[i][1]
                        def db_name = auto_analyze_origin_List[i][2]
                        if (db_name.startsWith("default_cluster:")) {
                            db_name = db_name.replace("default_cluster:", "")
                        }
                        def tb_name = auto_analyze_origin_List[i][3]
                        sql """switch ${catalog_name}"""
                        sql """use ${db_name}"""
                        def count_rows = sql """select count(*) from ${tb_name}"""
                        def stats_rows = sql """show column stats ${tb_name}"""
                        if (stats_rows != [] && stats_rows.size() > 0) {
                            Long row1 = count_rows[0][0]
                            Long row2 = new BigDecimal(stats_rows[0][1]).toLong()
                            if (row1 == row2) {
                                auto_mark = true
                                break
                            }
                        }
                    }
                }
                if (auto_mark) {
                    break
                }
            }
            sleep(internal_sleep * 60 * 1000)
        }
        return auto_mark
    }
    assertTrue(test_auto_analyze())

}
