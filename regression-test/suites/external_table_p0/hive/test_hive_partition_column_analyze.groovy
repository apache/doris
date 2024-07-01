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

suite("test_hive_partition_column_analyze", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_hive_partition_column_analyze"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        try {
            sql """set global enable_get_row_count_from_file_list=true"""

            sql """switch ${catalog_name};"""
            logger.info("switched to catalog " + catalog_name)

            sql """analyze table ${catalog_name}.partition_type.tinyint_partition (tinyint_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.smallint_partition (smallint_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.int_partition (int_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.bigint_partition (bigint_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.char_partition (char_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.varchar_partition (varchar_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.string_partition (string_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.date_partition (date_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.float_partition (float_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.double_partition (double_part) with sync"""
            sql """analyze table ${catalog_name}.partition_type.decimal_partition (decimal_part) with sync"""

            sql """use partition_type;"""

            result = sql """show column stats tinyint_partition (tinyint_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "tinyint_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "141474.0")
            assertEquals(result[0][6], "1.0")
            assertEquals(result[0][7], "1")
            assertEquals(result[0][8], "100")

            result = sql """show column stats smallint_partition (smallint_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "smallint_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "282948.0")
            assertEquals(result[0][6], "2.0")
            assertEquals(result[0][7], "1")
            assertEquals(result[0][8], "100")

            result = sql """show column stats int_partition (int_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "int_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "565896.0")
            assertEquals(result[0][6], "4.0")
            assertEquals(result[0][7], "1")
            assertEquals(result[0][8], "100")

            result = sql """show column stats bigint_partition (bigint_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "bigint_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "1131792.0")
            assertEquals(result[0][6], "8.0")
            assertEquals(result[0][7], "1")
            assertEquals(result[0][8], "100")

            result = sql """show column stats char_partition (char_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "char_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "101.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "2829480.0")
            assertEquals(result[0][6], "20.0")
            assertEquals(result[0][7], "\'1                   \'")
            assertEquals(result[0][8], "\'a                   \'")

            result = sql """show column stats varchar_partition (varchar_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "varchar_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "271630.0")
            assertEquals(result[0][6], "1.9199994345250717")
            assertEquals(result[0][7], "\'1\'")
            assertEquals(result[0][8], "\'99\'")

            result = sql """show column stats string_partition (string_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "string_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "271630.0")
            assertEquals(result[0][6], "1.9199994345250717")
            assertEquals(result[0][7], "\'1\'")
            assertEquals(result[0][8], "\'99\'")

            result = sql """show column stats date_partition (date_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "date_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "565896.0")
            assertEquals(result[0][6], "4.0")
            assertEquals(result[0][7], "\'2001-10-12\'")
            assertEquals(result[0][8], "\'2100-10-12\'")

            result = sql """show column stats float_partition (float_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "float_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "565896.0")
            assertEquals(result[0][6], "4.0")
            assertEquals(result[0][7], "296.3103")
            assertEquals(result[0][8], "32585.627")

            result = sql """show column stats double_partition (double_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "double_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "1131792.0")
            assertEquals(result[0][6], "8.0")
            assertEquals(result[0][7], "115.14474")
            assertEquals(result[0][8], "32761.14458")

            result = sql """show column stats decimal_partition (decimal_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "decimal_part")
            assertEquals(result[0][2], "141474.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "1131792.0")
            assertEquals(result[0][6], "8.0")
            assertEquals(result[0][7], "243.2868")
            assertEquals(result[0][8], "32527.1543")

            sql """analyze table ${catalog_name}.partition_type.decimal_partition (decimal_part) with sync with sql"""
            result = sql """show column stats decimal_partition (decimal_part)"""
            assertEquals(result.size(), 1)
            assertEquals(result[0][0], "decimal_part")
            assertEquals(result[0][2], "100000.0")
            assertEquals(result[0][3], "100.0")
            assertEquals(result[0][4], "0.0")
            assertEquals(result[0][5], "800000.0")
            assertEquals(result[0][6], "8.0")
            assertEquals(result[0][7], "243.2868")
            assertEquals(result[0][8], "32527.1543")
        } finally {
            sql """set global enable_get_row_count_from_file_list=false"""
        }
        sql """drop catalog ${catalog_name}"""
    }
}

