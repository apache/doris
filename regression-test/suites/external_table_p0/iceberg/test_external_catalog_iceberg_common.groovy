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

suite("test_external_catalog_iceberg_common", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_external_catalog_iceberg_common"

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


        sql """switch ${catalog_name};"""
        // test parquet format
        def q01_parquet = {
            qt_q01 """ SELECT COUNT(*) FROM (
                        SELECT l_returnflag, l_quantity, l_partkey, l_suppkey, l_discount, l_tax,
                            case
                                when l_tax <= 0.15 then '低频'
                                when l_tax <= 0.85 then '中频'
                                else '高频'
                            end
                            gr, cast(l_discount / 5 as int) * 5 as score_bins, l_comment from lineitem
                        ) as dc_1;
                    """
        }
        sql """ use `multi_catalog`; """
        // TODO support table:lineitem later
        // q01_parquet()  // 599715

        // test the special characters in table fields
        qt_sanitize_mara """select MaTnR, NtgEW, `/dsd/Sv_cnt_grP` from sanitize_mara order by mAtNr"""
}
