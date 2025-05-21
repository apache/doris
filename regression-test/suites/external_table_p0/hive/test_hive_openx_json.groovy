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

suite("test_hive_openx_json",  "p0,external,hive,external_docker,external_docker_hive") {


    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive3"]) {
        try {
            sql """set enable_fallback_to_original_planner=false"""
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_hive_openx_json"
            String broker_name = "hdfs"

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris'='thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`openx_json`"""

            try {
                sql  """ select * from json_table """;
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("DATA_QUALITY_ERROR"))
            }

            order_qt_q1  """  select * from json_table_ignore_malformed """


            try{
                sql  """ select * from json_data_arrays_tb """;
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("DATA_QUALITY_ERROR"))
            }
    
    
            try{
                sql  """ select * from scalar_to_array_tb """;
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("DATA_QUALITY_ERROR"))
            }

            sql """ set read_hive_json_in_one_column = true; """

            order_qt_2 """ select * from json_data_arrays_tb """
            order_qt_3 """ select * from json_one_column_table """

            try{
                sql  """ select * from scalar_to_array_tb """;
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("is not a string column."))
            }


            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
