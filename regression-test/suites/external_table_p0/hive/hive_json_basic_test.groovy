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

suite("hive_json_basic_test",  "p0,external,hive,external_docker,external_docker_hive") {


    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_hive_json_basic_test"
            String broker_name = "hdfs"

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris'='thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`default`"""

            String tb1 = """json_all_complex_types"""
            String tb2 = """json_nested_complex_table"""
            String tb3 = """json_load_data_table"""

            def tables = sql """ show tables """
            logger.info("tables = ${tables}")

            qt_q1 """ select * from ${tb1} order by id """
            qt_q2 """ select * from ${tb1} where tinyint_col < 0  order by id """
            qt_q3 """ select * from ${tb1} where bigint_col > 0 order by id """
            qt_q4 """ select float_col from ${tb1} where float_col is not null  order by id """
            qt_q5 """ select * from ${tb1} where id = 2 order by id """



            qt_q6 """ select * from  ${tb2} order by user_id"""
            qt_q7 """ select user_id,activity_log from  ${tb2} order by user_id"""


            order_qt_q8 """ select * from ${tb3} order by id """
            
            order_qt_q9 """ select col1,id from ${tb3} order by id """ 
            



            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
