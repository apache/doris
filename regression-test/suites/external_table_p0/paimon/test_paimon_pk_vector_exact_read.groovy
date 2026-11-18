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

// Build compacted source files at runtime instead of storing SDK tables in be/test.
suite("test_paimon_pk_vector_exact_read", "p0,external,paimon") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))) {
        logger.info("disabled paimon test")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    spark_paimon_multi "CREATE DATABASE IF NOT EXISTS paimon.pk_vector_exact_db"
    [["vector_l2", "l2", false], ["vector_dot", "inner_product", false],
            ["vector_nullable", "l2", true], ["vector_empty", "l2", false]].each { fixture ->
        String table = fixture[0]
        String nullability = fixture[2] ? "" : "NOT NULL"
        spark_paimon_multi """
            DROP TABLE IF EXISTS paimon.pk_vector_exact_db.${table};
            CREATE TABLE paimon.pk_vector_exact_db.${table} (
                id INT, embedding ARRAY<FLOAT> ${nullability}
            ) USING paimon TBLPROPERTIES (
                'primary-key'='id', 'bucket'='1', 'file.format'='parquet',
                'merge-engine'='partial-update', 'changelog-producer'='none',
                'deletion-vectors.enabled'='true', 'deletion-vectors.merge-on-read'='false',
                'vector-field'='embedding', 'field.embedding.vector-dim'='2',
                'vector-index.search-mode'='fast'
            );
        """
        if (table != "vector_empty") {
            // Overlapping key ranges force physical compaction. Merely upgrading one APPEND
            // file's level does not turn it into a readable primary-key vector source.
            for (int parity : [0, 1]) {
                String embedding = fixture[2]
                        ? "IF(id = 5, CAST(NULL AS ARRAY<FLOAT>), array(CAST(id AS FLOAT), CAST(0 AS FLOAT)))"
                        : "array(CAST(id AS FLOAT), CAST(0 AS FLOAT))"
                spark_paimon_multi """
                    INSERT INTO paimon.pk_vector_exact_db.${table}
                    SELECT CAST(id AS INT), ${embedding}
                    FROM (SELECT explode(sequence(0, 2047)) AS id) WHERE id % 2 = ${parity};
                """
            }
            spark_paimon_multi """CALL paimon.sys.compact(
                    table => 'pk_vector_exact_db.${table}', compact_strategy => 'full')"""
        }
        // Enable indexing after compaction: no payload is present, so these cases exercise
        // exact fallback even though the persisted table mode is FAST.
        spark_paimon_multi """
            ALTER TABLE paimon.pk_vector_exact_db.${table} SET TBLPROPERTIES (
                'pk-vector.index.columns'='embedding',
                'fields.embedding.pk-vector.index.type'='ivf-rq',
                'fields.embedding.pk-vector.distance.metric'='${fixture[1]}'
            );
        """
    }
    sql "DROP CATALOG IF EXISTS test_paimon_vector_exact"
    sql """CREATE CATALOG test_paimon_vector_exact PROPERTIES (
        'type'='paimon', 'paimon.catalog.type'='filesystem', 'warehouse'='s3://warehouse/wh',
        's3.endpoint'='http://${externalEnvIp}:${minioPort}', 's3.access_key'='admin',
        's3.secret_key'='password', 's3.region'='us-east-1', 'use_path_style'='true')"""
    def originalRust = sql("select @@enable_paimon_rust_reader")[0][0]
    def originalV2 = sql("select @@enable_file_scanner_v2")[0][0]
    def originalBatch = sql("select @@batch_size")[0][0]
    def originalForceJni = sql("select @@force_jni_scanner")[0][0]
    try {
        sql "set enable_file_scanner_v2=true"
        sql "set enable_paimon_rust_reader=true"
        sql "set force_jni_scanner=false"
        def search = { String table, String metric, String query, int k, String extra ->
            """vector_search('table'='test_paimon_vector_exact.pk_vector_exact_db.${table}',
                'column'='embedding', 'metric'='${metric}', 'query_vector'='${query}',
                'top_k'='${k}', 'use_index'='false' ${extra})"""
        }
        [["vector_l2", "l2", "[0,0]"], ["vector_dot", "dot", "[1,0]"]].each { fixture ->
            String table = fixture[0]
            String metric = fixture[1]
            String query = fixture[2]
            // 2048 outputs cross the SDK's Arrow batch boundary as well as the scanner caps.
            for (int cap : [1, 17, 128, 4096]) {
                sql "set batch_size=${cap}"
                String expectedDistance = metric == "l2" ? "id * cast(id as double)" : "-id"
                "qt_${metric}_batch_${cap}" """select count(*), count(distinct id),
                        min(id), max(id), sum(id),
                        sum(if(size(embedding)=2 and embedding[1]=id and embedding[2]=0, 0, 1)),
                        max(abs(_distance - (${expectedDistance})) /
                            greatest(1.0, abs(${expectedDistance}))) < 0.00001
                    from ${search(table, metric, query, 2049, "")}"""
            }
            "qt_${metric}_top_one" """select id from ${search(table, metric, query, 1, "")}
                    order by _distance, id"""
            "qt_${metric}_score_only" """select count(*), min(_distance), max(_distance)
                    from ${search(table, metric, query, 4, "")}"""
        }
        qt_nullable """select count(*), count(distinct id), min(id), max(id), sum(id)
                from ${search("vector_nullable", "l2", "[0,0]", 2049, "")}"""
        qt_nullable_prefilter """select id from ${search("vector_nullable", "l2", "[0,0]", 20,
                ", 'filter'='id in (4,5,6,1001,1002,1025,1026,2025,2026)', 'prefilter'='true'")}
                order by _distance, id"""
        qt_empty_prefilter """select count(*) from ${search("vector_l2", "l2", "[0,0]", 10,
                ", 'filter'='id < 0', 'prefilter'='true'")}"""
        qt_empty_table """select count(*) from ${search("vector_empty", "l2", "[0,0]", 10, "")}"""
        qt_offset_after_all_rows """select count(*) from ${search("vector_l2", "l2", "[0,0]", 1,
                ", 'offset'='2048'")}"""
        test {
            sql "select * from ${search('vector_l2', 'l2', '[0,0]', 0, '')}"
            exception "top_k"
        }
        test {
            sql "select * from ${search('vector_l2', 'l2', '[0,null]', 1, '')}"
            exception "finite, non-null FLOAT"
        }
        test {
            sql "select * from ${search('vector_l2', 'l2', '[0]', 1, '')}"
            exception "not supported"
        }
        // Invalid requests must leave the session able to execute another real vector scan.
        qt_after_error """select id from ${search("vector_l2", "l2", "[0,0]", 4, "")}
                order by _distance, id"""
        // A table-backed TVF must not bypass a source row policy. Until policy-aware
        // candidate generation is supported, reject the TVF explicitly for governed users.
        sql """DROP ROW POLICY IF EXISTS vector_source_policy
                ON test_paimon_vector_exact.pk_vector_exact_db.vector_l2 FOR paimon_vector_policy_user"""
        sql "DROP USER IF EXISTS paimon_vector_policy_user"
        sql "CREATE USER paimon_vector_policy_user IDENTIFIED BY 'Vector_test_123'"
        sql "GRANT SELECT_PRIV ON test_paimon_vector_exact.pk_vector_exact_db.vector_l2 TO paimon_vector_policy_user"
        sql """CREATE ROW POLICY vector_source_policy
                ON test_paimon_vector_exact.pk_vector_exact_db.vector_l2
                AS RESTRICTIVE TO paimon_vector_policy_user USING (id > 0)"""
        String policyUrl = context.config.jdbcUrl.substring(0,
                context.config.jdbcUrl.indexOf('/', 'jdbc:mysql://'.length()) + 1)
        connectToDoris("paimon_vector_policy_user", "Vector_test_123", policyUrl) {
            sql "set enable_file_scanner_v2=true"
            sql "set enable_paimon_rust_reader=true"
            qt_source_policy """select count(*) from test_paimon_vector_exact.pk_vector_exact_db.vector_l2
                    where id = 0"""
            test {
                sql "select id from ${search('vector_l2', 'l2', '[0,0]', 1, '')}"
                exception "source row filters or column masks"
            }
        }
    } finally {
        sql """DROP ROW POLICY IF EXISTS vector_source_policy
                ON test_paimon_vector_exact.pk_vector_exact_db.vector_l2 FOR paimon_vector_policy_user"""
        sql "DROP USER IF EXISTS paimon_vector_policy_user"
        sql "set batch_size=${originalBatch}"
        sql "set force_jni_scanner=${originalForceJni}"
        sql "set enable_paimon_rust_reader=${originalRust}"
        sql "set enable_file_scanner_v2=${originalV2}"
    }
}
