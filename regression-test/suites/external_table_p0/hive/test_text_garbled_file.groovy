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

suite("test_text_garbled_file", "p0,external,hive,external_docker,external_docker_hive") {
    //test hive garbled files  , prevent be hanged

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {

        for (String hivePrefix : ["hive2", "hive3"]) {
            String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
            String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = hivePrefix + "_test_text_garbled_file"
            sql """drop catalog if exists ${catalog_name};"""
            sql """
                create catalog if not exists ${catalog_name} properties (
                    'type'='hms',
                    'hadoop.username' = 'hadoop',
                    'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
                );
            """
            logger.info("catalog " + catalog_name + " created")
            sql """switch ${catalog_name};"""
            logger.info("switched to catalog " + catalog_name)

                
            order_qt_garbled_file """
            select * from ${catalog_name}.multi_catalog.test_csv_format_error;        
            """ 
        }
    }
}

