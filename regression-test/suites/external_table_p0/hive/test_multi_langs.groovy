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

suite("test_multi_langs", "p0,external,hive,external_docker,external_docker_hive") {

    def formats = ["_parquet", "_orc", "_text"]
    def q1 = """select * from test_chineseSUFFIX where col1='是' order by id"""
    def q2 = """select * from test_chineseSUFFIX order by id"""
    def q3 = """select id, count(col1) from test_chineseSUFFIX where col1='是' group by id order by id"""
    def q4 = """select * from test_multi_langsSUFFIX where col1='ありがとう' order by id"""
    def q5 = """select * from test_multi_langsSUFFIX order by id"""
    def q6 = """select id, count(col1) from test_multi_langsSUFFIX where col1='ありがとう' group by id order by id"""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        for (String hivePrefix : ["hive2", "hive3"]) {
            setHivePrefix(hivePrefix)
            try {
                String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
                String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
                String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
                String catalog_name = "test_multi_langs"

                sql """drop catalog if exists ${catalog_name};"""
                sql """
                create catalog if not exists ${catalog_name} properties (
                    'type'='hms',
                    'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                    'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
                );
                """
                logger.info("catalog " + catalog_name + " created")
                sql """switch ${catalog_name};"""
                logger.info("switched to catalog " + catalog_name)
                sql """use multi_catalog;"""
                logger.info("use multi_catalog")

                for (String format in formats) {
                    logger.info("Process format " + format)
                    qt_01 q1.replace("SUFFIX", format)
                    qt_02 q2.replace("SUFFIX", format)
                    qt_03 q3.replace("SUFFIX", format)
                    qt_04 q4.replace("SUFFIX", format)
                    qt_05 q5.replace("SUFFIX", format)
                    qt_06 q6.replace("SUFFIX", format)
                }
                sql """drop catalog if exists ${catalog_name}"""
            } finally {
            }
        }
    }
}
