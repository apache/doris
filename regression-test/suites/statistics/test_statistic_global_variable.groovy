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

suite("test_statistic_global_variable", "p0, nonConcurrent") {

    def verifyVairable = { variable, value ->
        sql """set global ${variable}="${value}";"""
        def result = sql """show variables like "${variable}"; """
        logger.info("result " + result)
        assertEquals(value, result[0][1])
    }

    try {
        verifyVairable("enable_auto_analyze", "true")
        verifyVairable("enable_auto_analyze", "false")
        verifyVairable("enable_partition_analyze", "true")
        verifyVairable("enable_partition_analyze", "false")
        verifyVairable("analyze_timeout", "1")
        verifyVairable("analyze_timeout", "43200")
        verifyVairable("auto_analyze_end_time", "11:11:11")
        verifyVairable("auto_analyze_end_time", "23:59:59")
        verifyVairable("auto_analyze_start_time", "22:22:22")
        verifyVairable("auto_analyze_start_time", "00:00:00")
        verifyVairable("auto_analyze_table_width_threshold", "3")
        verifyVairable("auto_analyze_table_width_threshold", "100")
        verifyVairable("partition_analyze_batch_size", "3")
        verifyVairable("partition_analyze_batch_size", "10")
        verifyVairable("enable_auto_analyze_internal_catalog", "false")
        verifyVairable("enable_auto_analyze_internal_catalog", "true")
        verifyVairable("external_table_auto_analyze_interval_in_millis", "1234")
        verifyVairable("external_table_auto_analyze_interval_in_millis", "86400000")
        verifyVairable("huge_table_default_sample_rows", "400000")
        verifyVairable("huge_table_default_sample_rows", "4194304")
        verifyVairable("huge_table_lower_bound_size_in_bytes", "55")
        verifyVairable("huge_table_lower_bound_size_in_bytes", "0")
        verifyVairable("huge_table_auto_analyze_interval_in_millis", "2345")
        verifyVairable("huge_table_auto_analyze_interval_in_millis", "0")
        verifyVairable("huge_partition_lower_bound_rows", "7777")
        verifyVairable("huge_partition_lower_bound_rows", "100000000")
        verifyVairable("table_stats_health_threshold", "11")
        verifyVairable("table_stats_health_threshold", "60")

    } finally {
        sql """set global enable_auto_analyze=false"""
        sql """set global enable_partition_analyze=false"""
        sql """set global analyze_timeout=43200"""
        sql """set global auto_analyze_end_time="23:59:59";"""
        sql """set global auto_analyze_start_time="00:00:00";"""
        sql """set global auto_analyze_table_width_threshold=100"""
        sql """set global partition_analyze_batch_size=10"""
        sql """set global enable_auto_analyze_internal_catalog=true"""
        sql """set global external_table_auto_analyze_interval_in_millis=86400000"""
        sql """set global huge_table_default_sample_rows=4194304"""
        sql """set global huge_table_lower_bound_size_in_bytes=0"""
        sql """set global huge_table_auto_analyze_interval_in_millis=0"""
        sql """set global huge_partition_lower_bound_rows=100000000"""
        sql """set global table_stats_health_threshold=60"""
    }
}

