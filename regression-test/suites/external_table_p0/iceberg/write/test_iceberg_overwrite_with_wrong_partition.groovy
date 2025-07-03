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

suite("test_iceberg_overwrite_with_wrong_partition", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String tb1 = "tb_dst";
    String tb2 = "tb_src";

    try {
        String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog_name = "test_iceberg_overwrite_with_wrong_partition"

        sql """drop catalog if exists ${catalog_name}"""
        sql """CREATE CATALOG ${catalog_name} PROPERTIES (
                'type'='iceberg',
                'iceberg.catalog.type'='rest',
                'uri' = 'http://${externalEnvIp}:${rest_port}',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1"
            );"""

        sql """ switch ${catalog_name} """
        sql """ use multi_catalog """

        sql """ drop table if exists ${tb1} """
        sql """ drop table if exists ${tb2} """

        sql """ 
        create table ${tb1} (
            id bigint,
            id2 bigint
        ) PARTITION BY LIST(id2)() ;
        """
        sql """ 
        create table ${tb2} (
            id bigint,
            id2 bigint
        );
        """

        sql """ insert into ${tb2} values (2450841,2450841), (2450842,2450842); """
        sql """ insert into ${tb2} values (2450843,2450843), (2450844,2450844); """
        sql """ insert into ${tb2} values (2450845,2450845), (2450846,2450846); """
        sql """ insert into ${tb2} values (2450847,2450847), (2450848,2450848); """
        sql """ insert into ${tb2} values (2450849,2450849), (2450850,2450850); """
        sql """ insert into ${tb2} values (2450841,2450841), (2450842,2450842); """
        sql """ insert into ${tb2} values (2450843,2450843), (2450844,2450844); """
        sql """ insert into ${tb2} values (2450845,2450845), (2450846,2450846); """
        sql """ insert into ${tb2} values (2450847,2450847), (2450848,2450848); """
        sql """ insert into ${tb2} values (2450849,2450849), (2450850,2450850); """

        sql """ insert overwrite table ${tb1} (id, id2) select id, id2 from ${tb2} where id2 >= 2450841 AND id2 < 2450851; """

        order_qt_qt01 """ select * from ${tb1} """

    } finally {
        sql """ drop table if exists ${tb1} """
        sql """ drop table if exists ${tb2} """
    }
}

