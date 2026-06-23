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

suite("test_file_meta_cache", "p0,external") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }




    for (String fileFormat : ["PARQUET",  "ORC"] ) {
        for (String hivePrefix : ["hive2", "hive3"]) {
            setHivePrefix(hivePrefix)
            String catalogName = getHiveTempName("test_file_meta_cache", fileFormat)
            String tableName = getHiveTempName("test_file_meta_cache", fileFormat)
            try {
                String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
                String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")

                sql """ drop catalog if exists ${catalogName} """
                sql """CREATE CATALOG ${catalogName} PROPERTIES (
                    'type'='hms',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
                );"""

                hive_docker """show databases;"""
                hive_docker """drop table if exists default.${tableName};  """
                hive_docker """
                                create table default.${tableName} (col1  int, col2 string) STORED AS ${fileFormat};
                            """
                hive_docker """insert into default.${tableName} values (1, "a"),(2, "b"); """

                sql """ refresh  catalog ${catalogName} """
                qt_1 """ select * from ${catalogName}.`default`.${tableName} order by col1 ; """

                hive_docker """ TRUNCATE TABLE ${tableName} """
                hive_docker """insert into default.${tableName} values (3, "c"), (4, "d"); """

                sql """ refresh  catalog ${catalogName} """
                qt_2 """ select * from ${catalogName}.`default`.${tableName} order by col1 ; """



                hive_docker """ drop TABLE ${tableName} """
                hive_docker """
                                create table default.${tableName} (col1  int, col2 string) STORED AS PARQUET;
                            """
                hive_docker """insert into default.${tableName} values (5, "e"), (6, "f"); """

                sql """ refresh  catalog ${catalogName} """
                qt_3 """ select * from ${catalogName}.`default`.${tableName} order by col1 ; """

                hive_docker """ INSERT OVERWRITE TABLE ${tableName} values (7,'g'), (8, 'h'); """

                sql """ refresh  catalog ${catalogName} """
                qt_4 """ select * from ${catalogName}.`default`.${tableName} order by col1 ; """


            } finally {
                try_sql """drop catalog if exists ${catalogName}"""
                try_hive_docker """drop table if exists default.${tableName}"""
            }
        }
    }

}
