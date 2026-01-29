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

    def runTestTopRfPredicate = {

        for (String table_name in ["test_topn_rf_null_orc", "test_topn_rf_null_parquet"])  {
            order_qt_sql_test_19 """
                SELECT * FROM ${table_name} ORDER BY abs(id) ASC LIMIT 5;
            """
        
            order_qt_sql_test_20 """
                SELECT * FROM ${table_name} ORDER BY abs(id) desc LIMIT 5;
            """

            order_qt_sql_test_21 """
                SELECT * FROM ${table_name} ORDER BY abs(value) ASC LIMIT 5;
            """

            order_qt_sql_test_22 """
                SELECT * FROM ${table_name} ORDER BY abs(value) desc LIMIT 5;
            """
            order_qt_sql_test_23 """
                SELECT subq1.id AS pk1 FROM (
                ( SELECT t1.id  FROM ${table_name} AS t1 )
                UNION ALL
                ( SELECT t1.id  FROM ${table_name} AS t1  ORDER BY t1.id )) subq1
                where 
                    subq1.id <=> (SELECT t1.id FROM ${table_name} AS t1  ORDER BY t1.id LIMIT 1) ORDER BY 1 LIMIT 1 ;
            """ 



            order_qt_sql_test_24 """
                SELECT subq1.id AS pk1 FROM (
                ( SELECT t1.id  FROM ${table_name} AS t1 where abs(t1.id) < 10)
                UNION ALL
                ( SELECT t1.id  FROM ${table_name} AS t1  ORDER BY t1.id limit 10)) subq1
                where 
                    subq1.id <=> (SELECT t1.id FROM ${table_name} AS t1  ORDER BY t1.id LIMIT 1) ORDER BY 1 LIMIT 1 ;
            """ 

            order_qt_sql_test_24 """
                SELECT subq1.id AS pk1 FROM (
                ( SELECT t1.id  FROM ${table_name} AS t1 where t1.id < 1000)
                UNION ALL
                ( SELECT t1.id  FROM ${table_name} AS t1  ORDER BY t1.id desc )) subq1
                where 
                    subq1.id <=> (SELECT t1.id FROM ${table_name} AS t1  ORDER BY t1.id LIMIT 1) ORDER BY 1 LIMIT 1 ;
            """ 
        }
    }




    for (String hivePrefix : ["hive2"]) {
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
        sql """ set max_file_scanners_concurrency = 1 """

        sql """ set topn_filter_ratio=1"""
        runTopnRfNullTest();
        runTestTopRfPredicate();


        sql """ set topn_filter_ratio=0 """
        runTopnRfNullTest();
        runTestTopRfPredicate();

    }
}
