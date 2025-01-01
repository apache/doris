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

suite("test_hudi_timetravel", "p2,external,hudi,external_remote,external_remote_hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
    }

    String catalog_name = "test_hudi_timetravel"
    String props = context.config.otherConfigs.get("hudiEmrCatalog")
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            ${props}
        );
    """

    sql """ switch ${catalog_name};"""
    sql """ use regression_hudi;""" 
    sql """ set enable_fallback_to_original_planner=false """

    def test_hudi_timetravel_querys = { table_name, timestamps ->
        timestamps.eachWithIndex { timestamp, index ->
            def query_name = "qt_timetravel${index + 1}"
            "${query_name}" """ select count(user_id) from ${table_name} for time as of "${timestamp}"; """
        }
    }

    // spark-sql "select distinct _hoodie_commit_time from user_activity_log_cow_non_partition order by _hoodie_commit_time;"
    def timestamps_cow_non_partition = [
        "20241114151946599",
        "20241114151952471",
        "20241114151956317",
        "20241114151958164",
        "20241114152000425",
        "20241114152004116",
        "20241114152005954",
        "20241114152007945",
        "20241114152009764",
        "20241114152011901",
    ]

    // spark-sql "select distinct _hoodie_commit_time from user_activity_log_cow_partition order by _hoodie_commit_time;"
    def timestamps_cow_partition = [
        "20241114152034850",
        "20241114152042944",
        "20241114152052682",
        "20241114152101650",
        "20241114152110650",
        "20241114152120030",
        "20241114152128871",
        "20241114152137714",
        "20241114152147114",
        "20241114152156417",
    ]

    // spark-sql "select distinct _hoodie_commit_time from user_activity_log_mor_non_partition order by _hoodie_commit_time;"
    def timestamps_mor_non_partition = [
        "20241114152014186",
        "20241114152015753",
        "20241114152017539",
        "20241114152019371",
        "20241114152020915",
        "20241114152022911",
        "20241114152024706",
        "20241114152026873",
        "20241114152028770",
        "20241114152030746",
    ]

    // spark-sql "select distinct _hoodie_commit_time from user_activity_log_mor_partition order by _hoodie_commit_time;"
    def timestamps_mor_partition = [
        "20241114152207700",
        "20241114152214609",
        "20241114152223933",
        "20241114152232579",
        "20241114152241610",
        "20241114152252244",
        "20241114152302763",
        "20241114152313010",
        "20241114152323587",
        "20241114152334111",
    ]

    test_hudi_timetravel_querys("user_activity_log_cow_non_partition", timestamps_cow_non_partition)
    test_hudi_timetravel_querys("user_activity_log_cow_partition", timestamps_cow_partition)
    test_hudi_timetravel_querys("user_activity_log_mor_non_partition", timestamps_mor_non_partition)
    test_hudi_timetravel_querys("user_activity_log_mor_partition", timestamps_mor_partition)

    // disable jni scanner because the old hudi jni reader based on spark can't read the emr hudi data
    // sql """set force_jni_scanner=true;"""
    // test_hudi_timetravel_querys("user_activity_log_cow_non_partition", timestamps_cow_non_partition)
    // test_hudi_timetravel_querys("user_activity_log_cow_partition", timestamps_cow_partition)
    // test_hudi_timetravel_querys("user_activity_log_mor_non_partition", timestamps_mor_non_partition)
    // test_hudi_timetravel_querys("user_activity_log_mor_partition", timestamps_mor_partition)
    // sql """set force_jni_scanner=false;"""

    sql """drop catalog if exists ${catalog_name};"""
}
