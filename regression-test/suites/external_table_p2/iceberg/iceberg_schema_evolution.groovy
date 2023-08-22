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

suite("iceberg_schema_evolution", "p2,external,iceberg,external_remote,external_remote_iceberg") {
    def rename1 = """select * from rename_test order by rename_1;"""
    def rename2 = """select * from rename_test where rename_1 in (3, 4) order by rename_1;"""
    def drop1 = """select * from drop_test order by orig1;"""
    def drop2 = """select * from drop_test where orig1<=3 order by orig1;"""
    def drop3 = """select * from drop_test where orig1>3 order by orig1;"""
    def add1 = """select * from add_test order by orig1;"""
    def add2 = """select * from add_test where orig1 = 2;"""
    def add3 = """select * from add_test where orig1 = 5;"""
    def reorder1 = """select * from reorder_test order by orig1;"""
    def reorder2 = """select * from reorder_test where orig1 = 2;"""
    def reorder3 = """select * from reorder_test where orig1 = 5;"""
    def readd1 = """select * from readd_test order by orig1;"""
    def readd2 = """select * from readd_test where orig1<5 order by orig1;"""
    def readd3 = """select * from readd_test where orig1>2 order by orig1;"""


    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "iceberg_schema_evolution"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use iceberg_schema_evolution;"""
        qt_rename1 rename1
        qt_rename2 rename2
        qt_drop1 drop1
        qt_drop2 drop2
        qt_drop3 drop3
        qt_add1 add1
        qt_add2 add2
        qt_add3 add3
        qt_reorder1 reorder1
        qt_reorder2 reorder2
        qt_reorder3 reorder3
        qt_readd1 readd1
        qt_readd2 readd2
        qt_readd3 readd3
    }
}

