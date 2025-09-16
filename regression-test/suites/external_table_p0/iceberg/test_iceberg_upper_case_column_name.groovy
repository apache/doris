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

suite("test_iceberg_upper_case_column_name", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

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

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_upper_case_column_name"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

        sql """use `${catalog_name}`.`test_db`"""

        qt_icebergParquet1 icebergParquet1
        qt_icebergParquet2 icebergParquet2
        qt_icebergParquet3 icebergParquet3
        qt_icebergParquet4 icebergParquet4
        qt_icebergParquet5 icebergParquet5
        qt_icebergParquet6 icebergParquet6
        qt_icebergParquet7 icebergParquet7
        qt_icebergParquet8 icebergParquet8
        qt_icebergOrc1 icebergOrc1
        qt_icebergOrc2 icebergOrc2
        qt_icebergOrc3 icebergOrc3
        qt_icebergOrc4 icebergOrc4
        qt_icebergOrc5 icebergOrc5
        qt_icebergOrc6 icebergOrc6
        qt_icebergOrc7 icebergOrc7
        qt_icebergOrc8 icebergOrc8
}

