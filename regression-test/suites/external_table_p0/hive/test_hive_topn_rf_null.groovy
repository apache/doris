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

suite("test_hive_topn_rf_null", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    def runTopnRfNullTest =  {
        for (String table_name in ["test_topn_rf_null_orc", "test_topn_rf_null_parquet"])  {
            order_qt_sql_test_1  """SELECT id, value, name FROM ${table_name} ORDER BY id ASC LIMIT 5;"""
            order_qt_sql_test_2  """SELECT id, value FROM ${table_name} ORDER BY id DESC LIMIT 3;"""
            order_qt_sql_test_3  """SELECT value, name FROM ${table_name} ORDER BY value ASC LIMIT 5;"""
            order_qt_sql_test_4  """SELECT id FROM ${table_name} ORDER BY value DESC LIMIT 5;"""
            order_qt_sql_test_5  """SELECT name FROM ${table_name} ORDER BY name ASC LIMIT 5;"""
            order_qt_sql_test_6  """SELECT id, name FROM ${table_name} ORDER BY name DESC LIMIT 5;"""
            order_qt_sql_test_7  """SELECT value FROM ${table_name} ORDER BY value DESC LIMIT 3;"""
            order_qt_sql_test_8  """SELECT id, name FROM ${table_name} ORDER BY id ASC LIMIT 7;"""
            order_qt_sql_test_9  """SELECT * FROM ${table_name} ORDER BY value ASC LIMIT 10;"""
            order_qt_sql_test_10 """SELECT name FROM ${table_name} ORDER BY name ASC LIMIT 2;"""
            order_qt_sql_test_11 """SELECT id, value, name FROM ${table_name} ORDER BY value ASC NULLS FIRST LIMIT 5;"""
            order_qt_sql_test_12 """SELECT id, value FROM ${table_name} ORDER BY value ASC NULLS LAST LIMIT 5;"""
            order_qt_sql_test_13 """SELECT value, name FROM ${table_name} ORDER BY value DESC NULLS FIRST LIMIT 5;"""
            order_qt_sql_test_14 """SELECT id FROM ${table_name} ORDER BY value DESC NULLS LAST LIMIT 5;"""
            order_qt_sql_test_15 """SELECT name FROM ${table_name} ORDER BY name ASC NULLS FIRST LIMIT 5;"""
            order_qt_sql_test_16 """SELECT id, name FROM ${table_name} ORDER BY name ASC NULLS LAST LIMIT 5;"""
            order_qt_sql_test_17 """SELECT value FROM ${table_name} ORDER BY name DESC NULLS FIRST LIMIT 3;"""
            order_qt_sql_test_18 """SELECT * FROM ${table_name} ORDER BY name DESC NULLS LAST LIMIT 3;"""
        }
    }




    for (String hivePrefix : ["hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog = "test_hive_topn_rf_null_${hivePrefix}"

        sql """drop catalog if exists ${catalog}"""
        sql """create catalog if not exists ${catalog} properties (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        logger.info("catalog " + catalog + " created")
        sql """switch ${catalog};"""

        sql """ use `default`; """
        sql """ set num_scanner_threads = 1 """

        sql """ set topn_filter_ratio=1"""
        runTopnRfNullTest();



        sql """ set topn_filter_ratio=0 """
        runTopnRfNullTest();



    }
}
