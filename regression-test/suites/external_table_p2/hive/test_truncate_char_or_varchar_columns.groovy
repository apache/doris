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

suite("test_truncate_char_or_varchar_columns", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_truncate_char_or_varchar_columns"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """switch ${catalog_name};"""
        sql """ set truncate_char_or_varchar_columns=true; """
        // test parquet format
        def q01_parquet = {
            qt_q01 """ select * from multi_catalog.test_truncate_char_or_varchar_columns_parquet order by id """
            qt_q02 """ select city, concat("at ", city, " in ", country) from ${catalog_name}.multi_catalog.test_truncate_char_or_varchar_columns_parquet order by id """
        }
        // test orc format
        def q01_orc = {
            qt_q01 """ select * from multi_catalog.test_truncate_char_or_varchar_columns_orc order by id """
            qt_q02 """ select city, concat("at ", city, " in ", country) from ${catalog_name}.multi_catalog.test_truncate_char_or_varchar_columns_orc order by id """
        }
        // test text format
        def q01_text = {
            qt_q01 """ select * from multi_catalog.test_truncate_char_or_varchar_columns_text order by id """
            qt_q02 """ select city, concat("at ", city, " in ", country) from ${catalog_name}.multi_catalog.test_truncate_char_or_varchar_columns_text order by id """
        }
        sql """ use `multi_catalog`; """
        q01_parquet()
        q01_orc()
        q01_text()

        sql """switch ${catalog_name};"""
        sql """ set truncate_char_or_varchar_columns=false; """
        // test parquet format
        def q02_parquet = {
            qt_q01 """ select * from multi_catalog.test_truncate_char_or_varchar_columns_parquet order by id """
            qt_q02 """ select city, concat("at ", city, " in ", country) from ${catalog_name}.multi_catalog.test_truncate_char_or_varchar_columns_parquet order by id """
        }
        // test orc format
        def q02_orc = {
            qt_q01 """ select * from multi_catalog.test_truncate_char_or_varchar_columns_orc order by id """
            qt_q02 """ select city, concat("at ", city, " in ", country) from ${catalog_name}.multi_catalog.test_truncate_char_or_varchar_columns_orc order by id """
        }
        // test text format
        def q02_text = {
            qt_q01 """ select * from multi_catalog.test_truncate_char_or_varchar_columns_text order by id """
            qt_q02 """ select city, concat("at ", city, " in ", country) from ${catalog_name}.multi_catalog.test_truncate_char_or_varchar_columns_text order by id """
        }
        sql """ use `multi_catalog`; """
        q02_parquet()
        q02_orc()
        q02_text()
    }
}

