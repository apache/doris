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

suite("test_iceberg_orc_decimal_bounds", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_orc_decimal_bounds"
    String db_name = "test_iceberg_orc_decimal_bounds_db"

    sql """drop catalog if exists ${catalog_name}"""
    sql """create catalog ${catalog_name} properties (
        'type' = 'iceberg',
        'iceberg.catalog.type' = 'rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        's3.access_key' = 'admin',
        's3.secret_key' = 'password',
        's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
        's3.region' = 'us-east-1'
    )"""
    sql """create database if not exists ${catalog_name}.${db_name}"""
    sql """use ${catalog_name}.${db_name}"""
    // EXPLAIN must finish planning splits so it exposes manifest pruning.
    sql "set enable_external_table_batch_mode = false"

    sql "drop table if exists orc_decimal_bounds_negative"
    sql """create table orc_decimal_bounds_negative (
        id int,
        d32 decimal(9, 0),
        d64 decimal(18, 2),
        d128 decimal(38, 2)
    ) properties ('write-format' = 'orc')"""
    sql """insert into orc_decimal_bounds_negative values
        (1, -100, -100.00, -999999999999999999999999999999999999.99),
        (2, -1, -1.00, -1.00)"""

    ["d32", "d64", "d128"].each { column ->
        // Negating the footer's two's-complement value makes both bounds positive
        // and incorrectly excludes this file from every negative range.
        explain {
            sql("select id from orc_decimal_bounds_negative where ${column} < 0")
            contains "inputSplitNum=1"
        }
        explain {
            sql("select id from orc_decimal_bounds_negative where ${column} > 0")
            contains "inputSplitNum=0"
        }
    }
    // ORC serializes -100.00 as value=-100, scale=0. Fixing only its sign
    // still leaves -1.00 in the manifest unless the column scale is restored.
    explain {
        sql("select id from orc_decimal_bounds_negative where d64 = -100.00")
        contains "inputSplitNum=1"
    }

    sql "drop table if exists orc_decimal_bounds_mixed"
    sql """create table orc_decimal_bounds_mixed (
        id int,
        d32 decimal(9, 0),
        d64 decimal(18, 2),
        d128 decimal(38, 2)
    ) properties ('write-format' = 'orc')"""
    sql """insert into orc_decimal_bounds_mixed values
        (1, -100, -100.00, -100.00),
        (2, 128, 128.00, 128.00),
        (3, null, null, null)"""

    ["d32", "d64", "d128"].each { column ->
        explain {
            sql("select id from orc_decimal_bounds_mixed where ${column} = -100")
            contains "inputSplitNum=1"
        }
        // Positive values with trailing zeros must retain their scale too.
        explain {
            sql("select id from orc_decimal_bounds_mixed where ${column} = 128")
            contains "inputSplitNum=1"
        }
        explain {
            sql("select id from orc_decimal_bounds_mixed where ${column} > 128")
            contains "inputSplitNum=0"
        }
    }
}
