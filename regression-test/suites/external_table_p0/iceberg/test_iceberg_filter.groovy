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

suite("test_iceberg_filter", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
            String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String catalog_name = "test_iceberg_filter"

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
            String tb_ts_filter = "tb_ts_filter";

            qt_qt01 """ select * from ${tb_ts_filter} order by id """
            qt_qt02 """ select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56' order by id """
            qt_qt03 """ select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56.1' order by id """
            qt_qt04 """ select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56.10' order by id """
            qt_qt05 """ select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56.100' order by id """
            qt_qt06 """ select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56.123' order by id """
            qt_qt07 """ select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56.1230' order by id """
            qt_qt08 """ select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56.123400' order by id """
            qt_qt09 """ select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56.123456' order by id """

            qt_qt10 """ select * from ${tb_ts_filter} where ts < '2024-05-30 20:34:56.12' order by id """
            qt_qt11 """ select * from ${tb_ts_filter} where ts > '2024-05-30 20:34:56.12' order by id """
            qt_qt12 """ select * from ${tb_ts_filter} where ts < '2024-05-30 20:34:56.1200' order by id """
            qt_qt13 """ select * from ${tb_ts_filter} where ts > '2024-05-30 20:34:56.1200' order by id """

            String tb_ts_ntz_filter = "${catalog_name}.test_db.tb_ts_ntz_filter";
            qt_qt14 """ select * from ${tb_ts_ntz_filter} where ts = '2024-06-11 12:34:56.123456' """
            qt_qt15 """ select * from ${tb_ts_ntz_filter} where ts > '2024-06-11 12:34:56.123456' """
            qt_qt16 """ select * from ${tb_ts_ntz_filter} where ts < '2024-06-11 12:34:56.123456' """
            qt_qt17 """ select * from ${tb_ts_ntz_filter} where ts > '2024-06-11 12:34:56.12345' """
            qt_qt18 """ select * from ${tb_ts_ntz_filter} where ts < '2024-06-11 12:34:56.123466' """

            explain {
                sql("select * from ${tb_ts_filter} where ts < '2024-05-30 20:34:56'")
                contains "inputSplitNum=0"
            }
            explain {
                sql("select * from ${tb_ts_filter} where ts < '2024-05-30 20:34:56.12'")
                contains "inputSplitNum=2"
            }
            explain {
                sql("select * from ${tb_ts_filter} where ts > '2024-05-30 20:34:56.1234'")
                contains "inputSplitNum=2"
            }
            explain {
                sql("select * from ${tb_ts_filter} where ts > '2024-05-30 20:34:56.0'")
                contains "inputSplitNum=6"
            }
            explain {
                sql("select * from ${tb_ts_filter} where ts = '2024-05-30 20:34:56.123456'")
                contains "inputSplitNum=1"
            }
            explain {
                sql("select * from ${tb_ts_filter} where ts < '2024-05-30 20:34:56.123456'")
                contains "inputSplitNum=6"
            }
            explain {
                sql("select * from ${tb_ts_filter} where ts > '2024-05-30 20:34:56.123456'")
                contains "inputSplitNum=0"
            }

        } finally {
        }
    }
}

/*

CREATE TABLE tb_ts_filter (
  id INT COMMENT '',
  ts TIMESTAMP_NTZ COMMENT '')
USING iceberg
TBLPROPERTIES (
  'format' = 'iceberg/parquet',
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd');

insert into tb_ts_filter values (1, timestamp '2024-05-30 20:34:56');
insert into tb_ts_filter values (2, timestamp '2024-05-30 20:34:56.1');
insert into tb_ts_filter values (3, timestamp '2024-05-30 20:34:56.12');
insert into tb_ts_filter values (4, timestamp '2024-05-30 20:34:56.123');
insert into tb_ts_filter values (5, timestamp '2024-05-30 20:34:56.1234');
insert into tb_ts_filter values (6, timestamp '2024-05-30 20:34:56.12345');
insert into tb_ts_filter values (7, timestamp '2024-05-30 20:34:56.123456');

*/

