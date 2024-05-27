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

suite("paimon_base_types", "p2,external,paimon,external_remote,external_remote_paimon") {
    def all = """select * from all_table;"""
    def c1 = """select * from all_table where c1=1;"""
    def c2 = """select * from all_table where c2=2;"""
    def c3 = """select * from all_table where c3=3;"""
    def c4 = """select * from all_table where c4=4;"""
    def c5 = """select * from all_table where c5=5;"""
    def c6 = """select * from all_table where c6=6;"""
    def c7 = """select * from all_table where c7=7;"""
    def c8 = """select * from all_table where c8=8;"""
    def c9 = """select * from all_table where c9<10;"""
    def c10 = """select * from all_table where c10=10.1;"""
    def c11 = """select * from all_table where c11=11.1;"""
    def c12 = """select * from all_table where c12='2020-02-02';"""
    def c13 = """select * from all_table where c13='13str';"""
    def c14 = """select * from all_table where c14='14varchar';"""
    def c15 = """select * from all_table where c15='a';"""
    def c16 = """select * from all_table where c16=true;"""
    def c18 = """select * from all_table where c18='2023-08-13 09:32:38.53';"""

    String enabled = context.config.otherConfigs.get("enableExternalPaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "paimon"
        String user_name = context.config.otherConfigs.get("extHiveHmsUser")
        String hiveHost = context.config.otherConfigs.get("extHiveHmsHost")
        String hivePort = context.config.otherConfigs.get("extHdfsPort")

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                "type" = "paimon",
                "paimon.catalog.type" = "filesystem",
                "warehouse" = "hdfs://${hiveHost}:${hivePort}/paimon/paimon1",
                "hadoop.username" = "${user_name}"
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use db1;"""
        logger.info("use db1")

        qt_all all
        qt_c1 c1
        qt_c2 c2
        qt_c3 c3
        qt_c4 c4
        qt_c5 c5
        qt_c6 c6
        qt_c7 c7
        qt_c8 c8
        qt_c9 c9
        qt_c10 c10
        qt_c11 c11
        qt_c12 c12
        qt_c13 c13
        qt_c14 c14
        qt_c15 c15
        qt_c16 c16
        qt_c18 c18

    }
}

