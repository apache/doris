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

suite("test_upper_case_column_name", "p0,external,hive,external_docker,external_docker_hive") {
    def hiveParquet1 = """select * from hive_upper_case_parquet;"""
    def hiveParquet2 = """select * from hive_upper_case_parquet where id=1;"""
    def hiveParquet3 = """select * from hive_upper_case_parquet where id>1;"""
    def hiveParquet4 = """select * from hive_upper_case_parquet where name='name';"""
    def hiveParquet5 = """select * from hive_upper_case_parquet where name!='name';"""
    def hiveParquet6 = """select id from hive_upper_case_parquet where id=1;"""
    def hiveParquet7 = """select name from hive_upper_case_parquet where id=1;"""
    def hiveParquet8 = """select id, name from hive_upper_case_parquet where id=1;"""
    def hiveOrc1 = """select * from hive_upper_case_orc;"""
    def hiveOrc2 = """select * from hive_upper_case_orc where id=1;"""
    def hiveOrc3 = """select * from hive_upper_case_orc where id>1;"""
    def hiveOrc4 = """select * from hive_upper_case_orc where name='name';"""
    def hiveOrc5 = """select * from hive_upper_case_orc where name!='name';"""
    def hiveOrc6 = """select id from hive_upper_case_orc where id=1;"""
    def hiveOrc7 = """select name from hive_upper_case_orc where id=1;"""
    def hiveOrc8 = """select id, name from hive_upper_case_orc where id=1;"""
    def icebergParquet1 = """select * from iceberg_upper_case_parquet;"""
    def icebergParquet2 = """select * from iceberg_upper_case_parquet where id=1;"""
    def icebergParquet3 = """select * from iceberg_upper_case_parquet where id>1;"""
    def icebergParquet4 = """select * from iceberg_upper_case_parquet where name='name';"""
    def icebergParquet5 = """select * from iceberg_upper_case_parquet where name!='name';"""
    def icebergParquet6 = """select id from iceberg_upper_case_parquet where id=1;"""
    def icebergParquet7 = """select name from iceberg_upper_case_parquet where id=1;"""
    def icebergParquet8 = """select id, name from iceberg_upper_case_parquet where id=1;"""
    def icebergOrc1 = """select * from iceberg_upper_case_orc;"""
    def icebergOrc2 = """select * from iceberg_upper_case_orc where id=1;"""
    def icebergOrc3 = """select * from iceberg_upper_case_orc where id>1;"""
    def icebergOrc4 = """select * from iceberg_upper_case_orc where name='name';"""
    def icebergOrc5 = """select * from iceberg_upper_case_orc where name!='name';"""
    def icebergOrc6 = """select id from iceberg_upper_case_orc where id=1;"""
    def icebergOrc7 = """select name from iceberg_upper_case_orc where id=1;"""
    def icebergOrc8 = """select id, name from iceberg_upper_case_orc where id=1;"""

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_upper_case_column_name"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """use `${catalog_name}`.`multi_catalog`"""

        qt_hiveParquet1 hiveParquet1
        qt_hiveParquet2 hiveParquet2
        qt_hiveParquet3 hiveParquet3
        qt_hiveParquet4 hiveParquet4
        qt_hiveParquet5 hiveParquet5
        qt_hiveParquet6 hiveParquet6
        qt_hiveParquet7 hiveParquet7
        qt_hiveParquet8 hiveParquet8
        qt_hiveOrc1 hiveOrc1
        qt_hiveOrc2 hiveOrc2
        qt_hiveOrc3 hiveOrc3
        qt_hiveOrc4 hiveOrc4
        qt_hiveOrc5 hiveOrc5
        qt_hiveOrc6 hiveOrc6
        qt_hiveOrc7 hiveOrc7
        qt_hiveOrc8 hiveOrc8
        // qt_icebergParquet1 icebergParquet1
        // qt_icebergParquet2 icebergParquet2
        // qt_icebergParquet3 icebergParquet3
        // qt_icebergParquet4 icebergParquet4
        // qt_icebergParquet5 icebergParquet5
        // qt_icebergParquet6 icebergParquet6
        // qt_icebergParquet7 icebergParquet7
        // qt_icebergParquet8 icebergParquet8
        // qt_icebergOrc1 icebergOrc1
        // qt_icebergOrc2 icebergOrc2
        // qt_icebergOrc3 icebergOrc3
        // qt_icebergOrc4 icebergOrc4
        // qt_icebergOrc5 icebergOrc5
        // qt_icebergOrc6 icebergOrc6
        // qt_icebergOrc7 icebergOrc7
        // qt_icebergOrc8 icebergOrc8
    }
}

