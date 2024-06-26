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

suite("test_hive_to_date", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_hive_to_date"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """use `${catalog_name}`.`multi_catalog`"""

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

