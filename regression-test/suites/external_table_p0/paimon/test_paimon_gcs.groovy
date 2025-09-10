package paimon
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

suite("test_paimon_gcs", "p2,external,doris,external_docker,external_docker_doris,new_catalog_property") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String gcs_warehouse = "s3://selectdb-qa-datalake-test/paimon_warehouse"
        String gcs_ak = context.config.otherConfigs.get("GCSAk")
        String gcs_sk = context.config.otherConfigs.get("CCSSk")
        String gcs_endpoint = context.config.otherConfigs.get("GCSEndpoint")
        String gcs_region = context.config.otherConfigs.get("GCSRegion")

        String table_name = "hive_test_table"
        for (String propertyPrefix : ["gs", "s3"]) {
            def catalog_name = "test_paimon_gcs_${propertyPrefix}"
            sql """drop catalog if exists ${catalog_name}"""
            sql """
                    CREATE CATALOG ${catalog_name} PROPERTIES (
                            'type' = 'paimon',
                            'warehouse' = '${gcs_warehouse}',
                            '${propertyPrefix}.endpoint' = '${gcs_endpoint}',
                            '${propertyPrefix}.access_key' = '${gcs_ak}',
                            '${propertyPrefix}.secret_key' = '${gcs_sk}'
                    );
                """
            sql """switch `${catalog_name}`"""
            sql """show databases; """
            sql """use `${catalog_name}`.`gcs_db`"""
            order_qt_no_region """SELECT * FROM ${table_name} """
            // 3.1 new features
            // batch incremental
            sql """SELECT * FROM ${table_name}  @incr('startTimestamp'='876488912')"""
            // time travel
            sql """SELECT * FROM ${table_name}  FOR VERSION AS OF 1;"""
            // branch/tag
            // TODO(zgx): add branch/tag
            // system table
            sql """SELECT * FROM ${table_name}\$snapshots;"""
            sql """drop catalog if exists ${catalog_name}"""

            sql """drop catalog if exists ${catalog_name}_with_region"""
            sql """
                    CREATE CATALOG ${catalog_name}_with_region PROPERTIES (
                            'type' = 'paimon',
                            'warehouse' = '${gcs_warehouse}',
                            '${propertyPrefix}.endpoint' = '${gcs_endpoint}',
                            '${propertyPrefix}.access_key' = '${gcs_ak}',
                            '${propertyPrefix}.secret_key' = '${gcs_sk}',
                            '${propertyPrefix}.region' = '${gcs_region}'
                    );
                """
            sql """switch `${catalog_name}_with_region`"""
            sql """show databases; """
            sql """use `${catalog_name}_with_region`.`gcs_db`"""
            order_qt_region """SELECT * FROM ${table_name} """
            // 3.1 new features
            // batch incremental
            sql """SELECT * FROM ${table_name}  @incr('startTimestamp'='876488912')"""
            // time travel
            sql """SELECT * FROM ${table_name}  FOR VERSION AS OF 1;"""
            // branch/tag
            // TODO(zgx): add branch/tag
            // system table
            sql """SELECT * FROM ${table_name}\$snapshots;"""
            sql """drop catalog if exists ${catalog_name}_with_region"""
        }
    }
}


