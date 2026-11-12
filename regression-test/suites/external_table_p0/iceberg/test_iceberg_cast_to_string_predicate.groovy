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

suite("test_iceberg_cast_to_string_predicate", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_cast_to_string_predicate"
    String db_name = "test_iceberg_cast_to_string_predicate_db"

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
    // EXPLAIN must finish planning splits so it exposes data file pruning.
    sql "set enable_external_table_batch_mode = false"

    // One data file per INSERT: one holds only 15, the other only 150.
    sql "drop table if exists cast_to_string_predicate"
    sql "create table cast_to_string_predicate (id int)"
    sql "insert into cast_to_string_predicate values (15)"
    sql "insert into cast_to_string_predicate values (150)"

    // As strings '15' >= '100' (since '5' > '0'), so both rows match. Pushed down with the CAST stripped,
    // the predicate became id >= 100 and pruned the file holding 15.
    def castToString = """select id from cast_to_string_predicate
        where cast(id as varchar) >= '100' and cast(id as varchar) < '200'"""
    order_qt_cast_to_string """${castToString}"""
    explain {
        sql("""${castToString}""")
        contains "inputSplitNum=2"
        notContains """ref(name="id") >= 100"""
    }

    // A plain numeric predicate still prunes the file it cannot match.
    def numeric = """select id from cast_to_string_predicate where id >= 100"""
    order_qt_numeric """${numeric}"""
    explain {
        sql("""${numeric}""")
        contains "inputSplitNum=1"
    }
}
