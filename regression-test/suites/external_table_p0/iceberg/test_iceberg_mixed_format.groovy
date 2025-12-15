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

suite("test_iceberg_mixed_format", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && "false".equalsIgnoreCase(enabled)) {
        logger.info("disable Iceberg test")
        return
    }

    String hivePrefix = "hive2";
    String catalog_name = "test_iceberg_mixed_format"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog if not exists ${catalog_name} properties (
        "type"="iceberg",
        "iceberg.catalog.type"="hive",
        "hive.metastore.uris"="thrift://${externalEnvIp}:${hmsPort}"
    );"""

    sql """switch ${catalog_name}"""
    sql """use iceberg_catalog"""

    sql """drop table if exists mixed_format_table"""
    sql """
        CREATE TABLE mixed_format_table (
            id INT,
            data STRING
        )
        PARTITIONED BY (id)
        PROPERTIES (
            'write-format'='parquet'
        );
    """

    sql """INSERT INTO mixed_format_table VALUES (1, 'parquet_data')"""

    sql """ALTER TABLE mixed_format_table SET TBLPROPERTIES ('write.format.default'='orc')"""

    sql """INSERT INTO mixed_format_table VALUES (2, 'orc_data')"""

    qt_select_mixed """SELECT * FROM mixed_format_table ORDER BY id"""

    sql """drop table mixed_format_table"""
    sql """drop catalog ${catalog_name}"""
}
