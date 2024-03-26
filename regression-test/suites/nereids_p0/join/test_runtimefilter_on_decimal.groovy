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

suite("test_runtimefilter_on_decimal", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // bug fix
    sql "set disable_join_reorder=true;"
    sql "set enable_runtime_filter_prune=false;"
    sql "set runtime_filter_type='MIN_MAX';"
    sql "set runtime_filter_wait_infinitely=true;"

    sql "drop table if exists decimal_rftest_l";
    sql "drop table if exists decimal_rftest_r";
    sql """
        CREATE TABLE `decimal_rftest_l` (
          `k1_dec_l` decimalv3(26, 6)
        )
        DISTRIBUTED BY HASH(`k1_dec_l`) buckets 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        CREATE TABLE `decimal_rftest_r` (
          `k1_dec_r` decimalv3(27, 6)
        )
        DISTRIBUTED BY HASH(`k1_dec_r`) buckets 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        insert into decimal_rftest_l values ("12345678901234567890.123456");
    """
    sql """
        insert into decimal_rftest_r values (null);
    """
    qt_dec_rftest_1 """
        select /*+SET_VAR(parallel_pipeline_task_num=2)*/ * from decimal_rftest_l join decimal_rftest_r on k1_dec_l = k1_dec_r order by 1, 2;
    """

    sql """
        insert into decimal_rftest_l values ("-99999999999999999999.999999"), ("99999999999999999999.999999");
    """
    sql """
        insert into decimal_rftest_r values ("-99999999999999999999.999999"), ("12345678901234567890.123456"), ("99999999999999999999.999999");
    """
    qt_dec_rftest_2 """
        select /*+SET_VAR(parallel_pipeline_task_num=8)*/ * from decimal_rftest_l join decimal_rftest_r on k1_dec_l = k1_dec_r order by 1, 2;
    """
}
