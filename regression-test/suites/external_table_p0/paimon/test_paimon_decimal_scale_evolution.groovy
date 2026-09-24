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

suite("test_paimon_decimal_scale_evolution", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minioPort = context.config.otherConfigs.get("spark_paimon_minio_port")
            ?: context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql "drop catalog if exists test_paimon_decimal_scale_evolution"
    sql """
        create catalog test_paimon_decimal_scale_evolution properties (
            'type'='paimon',
            'warehouse'='s3://warehouse/wh',
            's3.endpoint'='http://${externalEnvIp}:${minioPort}',
            's3.access_key'='admin',
            's3.secret_key'='password',
            's3.path.style.access'='true',
            'meta.cache.paimon.table.ttl-second'='0'
        )
    """

    try {
        spark_paimon_multi """
            create database if not exists paimon.paimon_decimal_scale_evolution_db;
            drop table if exists paimon.paimon_decimal_scale_evolution_db.decimal_scale_evolution;
            create table paimon.paimon_decimal_scale_evolution_db.decimal_scale_evolution (
                id int, amount decimal(5,2)
            ) using paimon tblproperties ('file.format'='parquet');
            insert into paimon.paimon_decimal_scale_evolution_db.decimal_scale_evolution
                values (1, cast(1.20 as decimal(5,2)));
            alter table paimon.paimon_decimal_scale_evolution_db.decimal_scale_evolution
                alter column amount type decimal(6,3);
        """

        sql "set force_jni_scanner=false"
        sql "set enable_file_scanner_v2=false"
        order_qt_decimal_scale_evolution_v1 """
            select id, amount
            from test_paimon_decimal_scale_evolution.paimon_decimal_scale_evolution_db.decimal_scale_evolution
            order by id
        """

        sql "set enable_file_scanner_v2=true"
        order_qt_decimal_scale_evolution_v2 """
            select id, amount
            from test_paimon_decimal_scale_evolution.paimon_decimal_scale_evolution_db.decimal_scale_evolution
            order by id
        """

        sql "set force_jni_scanner=true"
        order_qt_decimal_scale_evolution_jni """
            select id, amount
            from test_paimon_decimal_scale_evolution.paimon_decimal_scale_evolution_db.decimal_scale_evolution
            order by id
        """
    } finally {
        sql "set force_jni_scanner=false"
        sql "set enable_file_scanner_v2=false"
        sql "drop catalog if exists test_paimon_decimal_scale_evolution"
    }
}
