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

suite("test_hudi_snapshot", "p2,external,hudi,external_remote,external_remote_hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
    }

    String catalog_name = "test_hudi_snapshot"
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

    def test_hudi_snapshot_querys = { table_name ->
        // Query users by event_time in descending order and limit output
        qt_q01 """SELECT * FROM ${table_name} ORDER BY event_time DESC LIMIT 10;"""

        // Query all active user records and limit output
        qt_q02 """SELECT * FROM ${table_name} WHERE is_active = TRUE ORDER BY event_time LIMIT 10;"""

        // Query specific user's activity records and limit output
        qt_q03 """SELECT * FROM ${table_name} WHERE user_id = '62785e0e-ad44-4321-8b20-9ee4c4daca4a' ORDER BY event_time LIMIT 5;"""

        // Query events within a specific time range and limit output
        qt_q04 """SELECT * FROM ${table_name} WHERE event_time BETWEEN '2024-01-01 00:00:00' AND '2024-12-31 23:59:59' ORDER BY event_time LIMIT 10;"""

        // Count users by age group and limit output
        qt_q05 """SELECT age, COUNT(*) AS user_count FROM ${table_name} GROUP BY age ORDER BY user_count, age DESC LIMIT 5;"""

        // Query users with purchase records and limit output
        qt_q06 """SELECT user_id, purchases FROM ${table_name} WHERE array_size(purchases) > 0 ORDER BY user_id LIMIT 5;"""

        // Query users with a specific tag and limit output
        qt_q07 """SELECT * FROM ${table_name} WHERE array_contains(tags, 'others') ORDER BY event_time LIMIT 5;"""

        // Query users living in a specific city and limit output
        qt_q08 """SELECT * FROM ${table_name} WHERE struct_element(address, 'city') = 'North Rachelview' ORDER BY event_time LIMIT 5;"""

        // Query users within a specific coordinate range and limit output
        qt_q09 """SELECT * FROM ${table_name} WHERE struct_element(struct_element(address, 'coordinates'), 'latitude') BETWEEN 0 AND 100 AND struct_element(struct_element(address, 'coordinates'), 'longitude') BETWEEN 0 AND 100 ORDER BY event_time LIMIT 5;"""

        // Query records with ratings above a specific value and limit output
        qt_q10 """SELECT * FROM ${table_name} WHERE rating > 4.5 ORDER BY event_time DESC LIMIT 5;"""

        // Query all users' signup dates and limit output
        qt_q11 """SELECT user_id, signup_date FROM ${table_name} ORDER BY signup_date DESC LIMIT 10;"""

        // Query users with a specific postal code and limit output
        qt_q12 """SELECT * FROM ${table_name} WHERE struct_element(address, 'postal_code') = '80312' ORDER BY event_time LIMIT 5;"""

        // Query users with profile pictures and limit output
        qt_q13 """SELECT user_id, profile_picture FROM ${table_name} WHERE profile_picture IS NOT NULL ORDER BY user_id LIMIT 5;"""

        // Query users by signup date and limit output
        qt_q14 """SELECT * FROM ${table_name} WHERE signup_date = '2024-01-15' ORDER BY user_id LIMIT 5;"""

        // Query the total count of purchases for each user and limit output
        qt_q15 """SELECT user_id, array_size(purchases) AS purchase_count FROM ${table_name} ORDER BY user_id LIMIT 5;"""
    }

    test_hudi_snapshot_querys("user_activity_log_mor_non_partition")
    test_hudi_snapshot_querys("user_activity_log_mor_partition")
    test_hudi_snapshot_querys("user_activity_log_cow_non_partition")
    test_hudi_snapshot_querys("user_activity_log_cow_partition")

    // disable jni scanner because the old hudi jni reader based on spark can't read the emr hudi data
    // sql """set force_jni_scanner=true;"""
    // test_hudi_snapshot_querys("user_activity_log_mor_non_partition")
    // test_hudi_snapshot_querys("user_activity_log_mor_partition")
    // test_hudi_snapshot_querys("user_activity_log_cow_non_partition")
    // test_hudi_snapshot_querys("user_activity_log_cow_partition")
    // sql """set force_jni_scanner=false;"""

    sql """drop catalog if exists ${catalog_name};"""
}
