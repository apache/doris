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

suite("test_hive_star_qualifier", "p0,external,hive,external_docker,external_docker_hive") {
    String catalog_name = "test_hive_star_qualifier"

    def test1 = """select * from ${catalog_name}.multi_catalog.one_partition order by id;"""
    def test2 = """select * from multi_catalog.one_partition order by id;"""
    def test3 = """select * from one_partition order by id;"""
    def test4 = """select one_partition.* from ${catalog_name}.multi_catalog.one_partition order by id;"""
    def test5 = """select one_partition.* from multi_catalog.one_partition order by id;"""
    def test6 = """select one_partition.* from one_partition order by id;"""
    def test7 = """select multi_catalog.one_partition.* from ${catalog_name}.multi_catalog.one_partition order by id;"""
    def test8 = """select multi_catalog.one_partition.* from multi_catalog.one_partition order by id;"""
    def test9 = """select multi_catalog.one_partition.* from one_partition order by id;"""
    def test10 = """select ${catalog_name}.multi_catalog.one_partition.* from ${catalog_name}.multi_catalog.one_partition order by id;"""
    def test11 = """select ${catalog_name}.multi_catalog.one_partition.* from multi_catalog.one_partition order by id;"""
    def test12 = """select ${catalog_name}.multi_catalog.one_partition.* from one_partition order by id;"""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        for (String hivePrefix : ["hive2", "hive3"]) {
            setHivePrefix(hivePrefix)
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            sql """drop catalog if exists ${catalog_name};"""
            sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );
            """
            sql """use ${catalog_name}.multi_catalog"""
            qt_test1 test1
            qt_test2 test2
            qt_test3 test3
            qt_test4 test4
            qt_test5 test5
            qt_test6 test6
            qt_test7 test7
            qt_test8 test8
            qt_test9 test9
            qt_test10 test10
            qt_test11 test11
            qt_test12 test12
            sql """drop catalog if exists ${catalog_name};"""
        }
    }
}

