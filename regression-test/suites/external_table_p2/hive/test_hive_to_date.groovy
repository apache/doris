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

suite("test_hive_to_date", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_to_date"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use multi_catalog;"""
        sql """set enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false"""
        qt_1 "select * from datev2_csv"
        qt_2 "select * from datev2_orc"
        qt_3 "select * from datev2_parquet"
        qt_4 "select * from datev2_csv where day>to_date(\"1999-01-01\")"
        qt_5 "select * from datev2_orc where day>to_date(\"1999-01-01\")"
        qt_6 "select * from datev2_parquet where day>to_date(\"1999-01-01\")"
        qt_7 "select * from datev2_csv where day<to_date(\"1999-01-01\")"
        qt_8 "select * from datev2_orc where day<to_date(\"1999-01-01\")"
        qt_9 "select * from datev2_parquet where day<to_date(\"1999-01-01\")"
    }
}

