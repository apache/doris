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

suite("iceberg_partition_upper_case_nereids", "p0,external,doris,external_docker,external_docker_doris") {
    def orc_upper1 = """select * from iceberg_partition_upper_case_orc order by k1;"""
    def orc_upper2 = """select k1, city from iceberg_partition_upper_case_orc order by k1;"""
    def orc_upper3 = """select k1, k2 from iceberg_partition_upper_case_orc order by k1;"""
    def orc_upper4 = """select city from iceberg_partition_upper_case_orc order by city;"""
    def orc_upper5 = """select * from iceberg_partition_upper_case_orc where k1>1 and city='Beijing' order by k1;"""

    def orc_lower1 = """select * from iceberg_partition_lower_case_orc order by k1;"""
    def orc_lower2 = """select k1, city from iceberg_partition_lower_case_orc order by k1;"""
    def orc_lower3 = """select k1, k2 from iceberg_partition_lower_case_orc order by k1;"""
    def orc_lower4 = """select city from iceberg_partition_lower_case_orc order by city;"""
    def orc_lower5 = """select * from iceberg_partition_lower_case_orc where k1>1 and city='Beijing' order by k1;"""

    def parquet_upper1 = """select * from iceberg_partition_upper_case_parquet order by k1;"""
    def parquet_upper2 = """select k1, city from iceberg_partition_upper_case_parquet order by k1;"""
    def parquet_upper3 = """select k1, k2 from iceberg_partition_upper_case_parquet order by k1;"""
    def parquet_upper4 = """select city from iceberg_partition_upper_case_parquet order by city;"""
    def parquet_upper5 = """select * from iceberg_partition_upper_case_parquet where k1>1 and city='Beijing' order by k1;"""

    def parquet_lower1 = """select * from iceberg_partition_lower_case_parquet order by k1;"""
    def parquet_lower2 = """select k1, city from iceberg_partition_lower_case_parquet order by k1;"""
    def parquet_lower3 = """select k1, k2 from iceberg_partition_lower_case_parquet order by k1;"""
    def parquet_lower4 = """select city from iceberg_partition_lower_case_parquet order by city;"""
    def parquet_lower5 = """select * from iceberg_partition_lower_case_parquet where k1>1 and city='Beijing' order by k1;"""

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_partition_upper_case_nereids"

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


        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use multi_catalog;"""
        sql """set enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""
        qt_orcupper1 orc_upper1
        qt_orcupper2 orc_upper2
        qt_orcupper3 orc_upper3
        qt_orcupper4 orc_upper4
        qt_orcupper5 orc_upper5
        qt_orclower1 orc_lower1
        qt_orclower1 orc_lower2
        qt_orclower1 orc_lower3
        qt_orclower1 orc_lower4
        qt_orclower1 orc_lower5
        qt_parquetupper1 parquet_upper1
        qt_parquetupper2 parquet_upper2
        qt_parquetupper3 parquet_upper3
        qt_parquetupper4 parquet_upper4
        qt_parquetupper5 parquet_upper5
        qt_parquetlower1 parquet_lower1
        qt_parquetlower2 parquet_lower2
        qt_parquetlower3 parquet_lower3
        qt_parquetlower4 parquet_lower4
        qt_parquetlower5 parquet_lower5

}

