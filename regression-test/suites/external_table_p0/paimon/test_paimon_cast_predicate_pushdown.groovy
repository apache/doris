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

// WHY: Paimon cannot evaluate a Doris CAST, and the connector expression converter drops CASTs. A pushed
// CAST(code AS INT) = 5 would reach Paimon as code = 5 and let it prune the data files that hold '05' or ' 5',
// so the query would silently return fewer rows. Paimon declares supportsCastPredicatePushdown() = false: a
// conjunct with a CAST anywhere in it stays in Doris, and only the plain conjuncts reach Paimon. branch-4.x
// fixed the same row loss in its Paimon predicate converter (#67933).
//
// Each row is written in its own INSERT so the rows land in separate data files and file-level pruning is
// reachable; with a single file the scan would read every row anyway and a wrong pushdown would hide.
suite("test_paimon_cast_predicate_pushdown", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_paimon_cast_predicate_pushdown"
    String dbName = "test_paimon_cast_predicate_pushdown_db"

    spark_paimon_multi """
        create database if not exists paimon.${dbName};
        drop table if exists paimon.${dbName}.cast_pushdown_tbl;
        create table paimon.${dbName}.cast_pushdown_tbl (id int, code string, amount decimal(10, 2)) using paimon;
        insert into paimon.${dbName}.cast_pushdown_tbl values (1, '05', 1.24);
        insert into paimon.${dbName}.cast_pushdown_tbl values (2, ' 5', 1.20);
        insert into paimon.${dbName}.cast_pushdown_tbl values (3, '5', 1.30);
        insert into paimon.${dbName}.cast_pushdown_tbl values (4, 'abc', null);
        insert into paimon.${dbName}.cast_pushdown_tbl values (5, null, 1.25);
    """

    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
                'type' = 'paimon',
                'warehouse' = 's3://warehouse/wh',
                's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.path.style.access' = 'true'
        );
    """
    sql """use `${catalog_name}`.`${dbName}`"""

    def castPredicates = [
            "cast(code as int) = 5",
            "5 = cast(code as int)",
            "cast(code as int) > 4",
            "cast(code as int) in (5, 6)",
            "cast(code as int) not in (5, 6)",
            "cast(code as int) is null",
            "cast(code as int) is not null",
            "cast(code as int) = 5 or id = 4",
            "cast(amount as decimal(10, 1)) = 1.2",
    ]

    // A conjunct that contains a CAST never reaches Paimon.
    castPredicates.each { predicate ->
        explain {
            sql "select id from cast_pushdown_tbl where ${predicate}"
            contains "predicatesFromPaimon: NONE"
        }
    }
    // Plain conjuncts are still pushed, also next to a CAST conjunct that stays in Doris.
    ["id = 3", "cast(code as int) = 5 and id = 3"].each { predicate ->
        explain {
            sql "select id from cast_pushdown_tbl where ${predicate}"
            contains "predicatesFromPaimon:"
            notContains "predicatesFromPaimon: NONE"
        }
    }

    try {
        ["jni": "true", "native": "false"].each { reader, forceJni ->
            sql """set force_jni_scanner = ${forceJni}"""
            (castPredicates + ["cast(code as int) = 5 and id = 3"]).eachWithIndex { predicate, index ->
                "order_qt_${reader}_${index}"("""
                    select id, code, amount from cast_pushdown_tbl where ${predicate} order by id
                """)
            }
            // The source LIMIT stays off while a CAST conjunct is evaluated in Doris, so one matching row is
            // returned instead of whatever row Paimon would have read first.
            "order_qt_${reader}_limit"("""
                select count(*) from (
                    select id from cast_pushdown_tbl where cast(code as int) = 5 limit 1
                ) t
            """)
        }
    } finally {
        sql """set force_jni_scanner = false"""
    }
}
