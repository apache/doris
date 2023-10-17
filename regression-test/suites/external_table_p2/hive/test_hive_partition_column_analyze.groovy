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

suite("test_hive_partition_column_analyze", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_partition_column_analyze"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        // Test analyze table without init.
        sql """analyze table ${catalog_name}.multi_partition.multi_partition_parquet (event_day) with sync"""
        sql """analyze table ${catalog_name}.multi_partition.multi_partition_orc (event_day) with sync"""

        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use multi_partition;"""
        def result = sql """show column stats multi_partition_parquet (event_day)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "event_day")
        assertTrue(result[0][1] == "3.83714205E8")
        assertTrue(result[0][2] == "99949.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "3.83714205E9")
        assertTrue(result[0][5] == "10.0")
        assertTrue(result[0][6] == "\'1749-09-24\'")
        assertTrue(result[0][7] == "\'2023-05-26\'")

        result = sql """show column stats multi_partition_orc (event_day)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "event_day")
        assertTrue(result[0][1] == "1.9007155E8")
        assertTrue(result[0][2] == "99949.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "1.9007155E9")
        assertTrue(result[0][5] == "10.0")
        assertTrue(result[0][6] == "\'1749-09-24\'")
        assertTrue(result[0][7] == "\'2023-05-26\'")

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
        sql """analyze table ${catalog_name}.partition_type.two_partition (part1, part2) with sync"""

        sql """use partition_type;"""

        result = sql """show column stats tinyint_partition (tinyint_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "tinyint_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "141474.0")
        assertTrue(result[0][5] == "1.0")
        assertTrue(result[0][6] == "1")
        assertTrue(result[0][7] == "100")

        result = sql """show column stats smallint_partition (smallint_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "smallint_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "282948.0")
        assertTrue(result[0][5] == "2.0")
        assertTrue(result[0][6] == "1")
        assertTrue(result[0][7] == "100")

        result = sql """show column stats int_partition (int_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "int_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "565896.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "1")
        assertTrue(result[0][7] == "100")

        result = sql """show column stats bigint_partition (bigint_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "bigint_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "1131792.0")
        assertTrue(result[0][5] == "8.0")
        assertTrue(result[0][6] == "1")
        assertTrue(result[0][7] == "100")

        result = sql """show column stats char_partition (char_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "char_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "101.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "2829480.0")
        assertTrue(result[0][5] == "20.0")
        assertTrue(result[0][6] == "\'1                   \'")
        assertTrue(result[0][7] == "\'a                   \'")

        result = sql """show column stats varchar_partition (varchar_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "varchar_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "271630.0")
        assertTrue(result[0][5] == "1.9199994345250717")
        assertTrue(result[0][6] == "\'1\'")
        assertTrue(result[0][7] == "\'99\'")

        result = sql """show column stats string_partition (string_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "string_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "271630.0")
        assertTrue(result[0][5] == "1.9199994345250717")
        assertTrue(result[0][6] == "\'1\'")
        assertTrue(result[0][7] == "\'99\'")

        result = sql """show column stats date_partition (date_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "date_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "565896.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "\'2001-10-12\'")
        assertTrue(result[0][7] == "\'2100-10-12\'")

        result = sql """show column stats float_partition (float_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "float_part")
        assertTrue(result[0][1] == "117416.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "469664.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "296.31")
        assertTrue(result[0][7] == "32585.627")

        result = sql """show column stats double_partition (double_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "double_part")
        assertTrue(result[0][1] == "16987.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "135896.0")
        assertTrue(result[0][5] == "8.0")
        assertTrue(result[0][6] == "115.145")
        assertTrue(result[0][7] == "32761.145")

        result = sql """show column stats decimal_partition (decimal_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "decimal_part")
        assertTrue(result[0][1] == "141474.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "1131792.0")
        assertTrue(result[0][5] == "8.0")
        assertTrue(result[0][6] == "243.2868")
        assertTrue(result[0][7] == "32527.1543")

        sql """analyze table ${catalog_name}.partition_type.decimal_partition (decimal_part) with sync with sql"""
        result = sql """show column stats decimal_partition (decimal_part)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "decimal_part")
        assertTrue(result[0][1] == "100000.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "800000.0")
        assertTrue(result[0][5] == "8.0")
        assertTrue(result[0][6] == "243.2868")
        assertTrue(result[0][7] == "32527.1543")

        result = sql """show column stats two_partition (part1)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "part1")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][6] == "1")
        assertTrue(result[0][7] == "100")

        result = sql """show column stats two_partition (part2)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "part2")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][6] == "\'1\'")
        assertTrue(result[0][7] == "\'99\'")

        sql """drop catalog ${catalog_name}"""
    }
}

