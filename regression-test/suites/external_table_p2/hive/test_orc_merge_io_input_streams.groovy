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

import org.apache.doris.regression.action.ProfileAction

suite("test_orc_merge_io_input_streams", "p2,external") {

    String enabled = context.config.otherConfigs.get("enableExternalEmrTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable test")
        return;
    }

    String props = context.config.otherConfigs.get("emrCatalogCommonProp")
    String hms_catalog_name = "test_orc_merge_io_input_streams"

    sql """drop catalog if exists ${hms_catalog_name};"""
    sql """
        CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
        PROPERTIES (
            "type" = "hms",
            'hive.version' = '3.1.3', 
            ${props}
        );
    """

    logger.info("catalog " + hms_catalog_name + " created")
    sql """switch ${hms_catalog_name};"""
    logger.info("switched to catalog " + hms_catalog_name)
    sql """ use regression;"""

    sql """ set dry_run_query=true; """
    sql """ set enable_profile=true; """
    sql """ set profile_level=2; """
    // Consecutive stripes must use the same row reader to expose a stale stream map.
    sql """ set file_split_size=268435456; """
    sql """ set enable_orc_filter_by_min_max=true; """
    // Cached empty row groups would bypass the ORC stream lifecycle on repeated runs.
    sql """ set enable_condition_cache=false; """

    String token = "orc_merge_filtered_" + UUID.randomUUID().toString()
    qt_1 """  SELECT /* ${token} */ trace_id as trace_id_sub,created_time FROM test_orc_merge_io_input_streams_table
              where dt = replace(date_sub('2025-04-16', 1), '-', '') and trace_id='1210647803'; """

    String profile = new ProfileAction(context).getProfileBySql(token,
            ["EvaluatedRowGroupCount:", "SelectedRowGroupCount:", "OrcRowReadersCreated:",
             "OrcRowReaderStripeVisits:", "OrcMergedIO:"])
    // Merged and per-instance sections can repeat a counter; do not add duplicates.
    def counterMax = { String name ->
        (profile =~ /\b${name}:\s*(?:sum\s+)?(\d+)/).collect { it[1] as long }.max()
    }
    // Zero rows alone would also pass if file/stripe statistics skipped the whole fixture.
    // Require row-group PPD in one reader so skipping a stripe must advance its stream map.
    assertTrue(counterMax("EvaluatedRowGroupCount") >= 4, "Expected multiple stripes of row-group PPD")
    assertTrue(counterMax("SelectedRowGroupCount") == 0, "Expected every evaluated row group to be rejected")
    assertTrue(counterMax("OrcRowReadersCreated") == 1, "Expected one ORC reader across consecutive stripes")
    assertTrue(counterMax("OrcRowReaderStripeVisits") >= 2, "Expected consecutive stripes in that reader")
    assertTrue(counterMax("OrcMergedIO") >= 2, "Expected merged IO for consecutive stripes")
    qt_2 """ select * from test_orc_merge_io_input_streams_table ; """

    sql """drop catalog ${hms_catalog_name};"""
}

// Reproduce the ZLIB fixture with GenerateOrcMergeFixture.java and register it with
// regression-test/script/external_table_p2/emr/orc_merge_table.sql. See that directory's README.
