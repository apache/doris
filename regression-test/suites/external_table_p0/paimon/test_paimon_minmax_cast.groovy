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

suite("test_paimon_minmax_cast", "p0,external,paimon,external_docker,external_docker_paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Paimon test is disabled")
        return
    }

    String catalogName = "test_paimon_minmax_cast"
    String dbName = "paimon_minmax_cast_db"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def originalSettings = ["enable_file_scanner_v2", "force_jni_scanner",
            "enable_strict_cast", "enable_push_down_no_group_agg"].collectEntries { name ->
        [(name): sql("show variables like '${name}'")[0][1]]
    }

    try {
        // A single writer and bucket keep both overflowing endpoints and the valid interior value
        // in one file. Separate files could let metadata aggregation retain the interior value.
        spark_paimon_multi """
            create database if not exists paimon.${dbName};
            drop table if exists paimon.${dbName}.minmax_cast;
            create table paimon.${dbName}.minmax_cast (value bigint)
                using paimon tblproperties (
                    'bucket'='1',
                    'bucket-key'='value',
                    'file.format'='parquet'
                );
            insert into paimon.${dbName}.minmax_cast
                select /*+ coalesce(1) */ value from values
                    (cast(-2147483649 as bigint)),
                    (cast(0 as bigint)),
                    (cast(2147483648 as bigint)) as data(value);
        """

        sql """drop catalog if exists ${catalogName}"""
        sql """create catalog ${catalogName} properties (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )"""
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""
        sql "set enable_file_scanner_v2=true"
        sql "set force_jni_scanner=false"
        sql "set enable_strict_cast=false"

        def queries = [
            "select min(cast(value as int)) from minmax_cast",
            "select max(cast(value as int)) from minmax_cast",
            "select min(cast(value as int)), max(cast(value as int)) from minmax_cast"
        ]
        queries.each { query ->
            sql "set enable_push_down_no_group_agg=false"
            def fullScanResult = sql(query)
            sql "set enable_push_down_no_group_agg=true"
            // Compare with row-by-row evaluation so the reference cannot share the metadata bug.
            assertEquals(fullScanResult, sql(query))
            explain {
                sql(query)
                contains "pushdown agg=NONE"
            }
        }

        // Keep a positive control: disabling all file MIN/MAX pushdown must not satisfy this test.
        explain {
            sql "select min(value), max(value) from minmax_cast"
            contains "pushdown agg=MINMAX"
            contains "inputSplitNum=1"
        }
    } finally {
        originalSettings.each { name, value -> sql "set ${name}=${value}" }
        sql "switch internal"
        sql """drop catalog if exists ${catalogName}"""
    }
}
