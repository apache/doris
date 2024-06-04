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

suite("test_hive_statistics_all_type_p0", "all_types,p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "test_${hivePrefix}_statistics_all_type_p0"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""
            sql """analyze table orc_all_types with sync"""
            def result = sql """show column stats orc_all_types;"""
            assertEquals(16, result.size())

            result = sql """show column stats orc_all_types (int_col);"""
            assertEquals("int_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3256.0", result[0][3])
            assertEquals("361.0", result[0][4])
            assertEquals("14400.0", result[0][5])

            result = sql """show column stats orc_all_types (char_col);"""
            assertEquals("char_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("25273.0", result[0][5])

            result = sql """show column stats orc_all_types (binary_col);"""
            assertEquals("binary_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3240.0", result[0][3])
            assertEquals("362.0", result[0][4])
            assertEquals("85788.0", result[0][5])

            result = sql """show column stats orc_all_types (bigint_col);"""
            assertEquals("bigint_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3209.0", result[0][3])
            assertEquals("377.0", result[0][4])
            assertEquals("28800.0", result[0][5])

            result = sql """show column stats orc_all_types (boolean_col);"""
            assertEquals("boolean_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("2.0", result[0][3])
            assertEquals("369.0", result[0][4])
            assertEquals("3600.0", result[0][5])

            result = sql """show column stats orc_all_types (date_col);"""
            assertEquals("date_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("2148.0", result[0][3])
            assertEquals("378.0", result[0][4])
            assertEquals("14400.0", result[0][5])

            result = sql """show column stats orc_all_types (float_col);"""
            assertEquals("float_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3274.0", result[0][3])
            assertEquals("340.0", result[0][4])
            assertEquals("14400.0", result[0][5])

            result = sql """show column stats orc_all_types (string_col);"""
            assertEquals("string_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3261.0", result[0][3])
            assertEquals("347.0", result[0][4])
            assertEquals("453634.0", result[0][5])

            result = sql """show column stats orc_all_types (varchar_col);"""
            assertEquals("varchar_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("6.0", result[0][3])
            assertEquals("0.0", result[0][4])
            assertEquals("35950.0", result[0][5])

            result = sql """show column stats orc_all_types (smallint_col);"""
            assertEquals("smallint_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("90.0", result[0][3])
            assertEquals("359.0", result[0][4])
            assertEquals("7200.0", result[0][5])

            result = sql """show column stats orc_all_types (timestamp_col);"""
            assertEquals("timestamp_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3243.0", result[0][3])
            assertEquals("339.0", result[0][4])
            assertEquals("28800.0", result[0][5])

            result = sql """show column stats orc_all_types (tinyint_col);"""
            assertEquals("tinyint_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("9.0", result[0][3])
            assertEquals("366.0", result[0][4])
            assertEquals("3600.0", result[0][5])

            result = sql """show column stats orc_all_types (double_col);"""
            assertEquals("double_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3200.0", result[0][3])
            assertEquals("395.0", result[0][4])
            assertEquals("28800.0", result[0][5])

            result = sql """show column stats orc_all_types (decimal_col);"""
            assertEquals("decimal_col", result[0][0])
            assertEquals("3600.0", result[0][2])
            assertEquals("3218.0", result[0][3])
            assertEquals("367.0", result[0][4])
            assertEquals("28800.0", result[0][5])

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

