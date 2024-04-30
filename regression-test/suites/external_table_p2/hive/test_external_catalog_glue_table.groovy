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

suite("test_external_catalog_glue_table", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")

        sql """drop catalog if exists test_external_catalog_glue;"""
        sql """
            create catalog if not exists test_external_catalog_glue properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """switch test_external_catalog_glue;"""
        def q01 = {
            qt_q01 """ select glue_int from iceberg_glue_types order by glue_int limit 5 """
            qt_q02 """ select glue_bigint from iceberg_glue_types order by glue_bigint limit 5 """
            qt_q03 """ select glue_smallint from iceberg_glue_types order by glue_int limit 5 """
            qt_q04 """ select glue_decimal from iceberg_glue_types order by glue_decimal limit 5 """
            qt_q05 """ select glue_double from iceberg_glue_types order by glue_int limit 5 """
            qt_q06 """ select glue_timstamp from iceberg_glue_types order by glue_timstamp limit 20 """
            qt_q07 """ select glue_char from iceberg_glue_types order by glue_int limit 5 """
            qt_q08 """ select glue_varchar from iceberg_glue_types order by glue_varchar limit 5 """
            qt_q09 """ select glue_string from iceberg_glue_types order by glue_string limit 5 """
            qt_q10 """ select glue_decimal, glue_bool from iceberg_glue_types order by glue_decimal limit 5 """
            qt_q11 """ select glue_int,glue_smallint from iceberg_glue_types where glue_int > 2000 and glue_smallint < 10000 order by glue_int limit 10 """
            qt_q12 """ select glue_smallint from iceberg_glue_types where glue_smallint is null order by glue_smallint limit 3 """
            qt_q13 """ select glue_smallint from iceberg_glue_types where glue_smallint is not null order by glue_smallint limit 10 """
            qt_q14 """ select glue_string from iceberg_glue_types where glue_string>'040abff1da4748e4b' order by glue_int limit 5 """
            qt_q15 """ select count(1) from iceberg_glue_types """
            qt_q16 """ select glue_timstamp from iceberg_glue_types where glue_timstamp > '2023-03-07 20:35:59' order by glue_timstamp limit 5 """
            qt_q17 """ select * from iceberg_glue_types order by glue_decimal limit 5 """
            qt_q18 """ select glue_int, glue_varchar from iceberg_glue_types where glue_varchar > date '2023-03-07' """
        }
        sql """ use `iceberg_catalog`; """
        q01()
    }
}
