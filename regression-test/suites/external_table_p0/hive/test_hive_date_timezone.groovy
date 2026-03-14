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

suite("test_hive_date_timezone", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return
    }

    for (String hivePrefix : ["hive3"]) {
        setHivePrefix(hivePrefix)
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String catalogName = "test_hive_date_timezone_${hivePrefix}"

        sql """drop catalog if exists ${catalogName}"""
        sql """
            create catalog if not exists ${catalogName} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
            );
        """

        try {
            sql """set enable_fallback_to_original_planner=false"""
            sql """switch ${catalogName}"""
            sql """use `schema_change`"""

            sql """set time_zone = 'UTC'"""
            qt_orc_date_utc """select date_col from orc_primitive_types_to_date order by id"""
            qt_parquet_date_utc """select date_col from parquet_primitive_types_to_date order by id"""

            sql """set time_zone = 'America/Mexico_City'"""
            qt_orc_date_west_tz """select date_col from orc_primitive_types_to_date order by id"""
            qt_parquet_date_west_tz """select date_col from parquet_primitive_types_to_date order by id"""
        } finally {
            sql """set time_zone = default"""
            sql """switch internal"""
            sql """drop catalog if exists ${catalogName}"""
        }
    }
}
