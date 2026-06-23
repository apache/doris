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

suite("test_prepare_hive_data_in_case", "p0,external") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        String tableName = getHiveTempName("test_prepare_hive_data_in_case")
        String catalogName = getHiveTempName("test_prepare_hive_data_in_case", "catalog")
        try {
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")

            hive_docker """show databases;"""
            hive_docker """drop table if exists default.${tableName};  """
            hive_docker """
                            create table default.${tableName} (k1 String, k2 String);
                        """
            hive_docker """insert into default.${tableName} values ('aaa','bbb'),('ccc','ddd'),('eee','fff')"""
            def values = hive_docker """select count(*) from `default`.${tableName};"""

            log.info(values.toString())

            sql """drop catalog if exists ${catalogName};"""
            sql """CREATE CATALOG ${catalogName} PROPERTIES (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            def values2 = sql """select count(*) from ${catalogName}.`default`.${tableName};"""
            log.info(values2.toString())
            assertEquals(values[0][0],values2[0][0])

            // Execute in Hive in unstable, remove it
            // qt_hive_docker_01 """select * from default.test_prepare_hive_data_in_case order by k1 desc  ;"""

            qt_sql_02 """ select * from ${catalogName}.`default`.${tableName} order by k1 desc;"""

        } finally {
            try_sql """drop catalog if exists ${catalogName};"""
            try_hive_docker """drop table if exists default.${tableName};"""
        }
    }
}
