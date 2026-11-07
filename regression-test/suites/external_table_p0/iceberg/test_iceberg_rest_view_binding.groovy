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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

suite("test_iceberg_rest_view_binding", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergViewTest"))) {
        return
    }
    def config = context.config.otherConfigs
    String host = config.get("externalEnvIp")
    String rest = "http://${host}:${config.get('iceberg_rest_uri_port')}"
    String sourceDb = config.get("iceberg_view_source_namespace") ?: "iceberg_rest_view_source"
    String storeDb = config.get("iceberg_view_store_namespace") ?: "iceberg_rest_view_store"
    String catalog = config.get("iceberg_view_catalog") ?: "iceberg_rest_view_binding"
    String accessKey = config.get("iceberg_view_access_key")
    String secretKey = config.get("iceberg_view_secret_key")

    if (!accessKey || !secretKey) {
        throw new IllegalArgumentException("Configure iceberg_view_access_key and iceberg_view_secret_key")
    }

    // Create real Iceberg view metadata through the REST API; no mock catalog is involved.
    def request = { String method, String path, Object body, List<Integer> expected ->
        def connection = new URL(rest + path).openConnection(Proxy.NO_PROXY)
        connection.requestMethod = method
        connection.connectTimeout = 20000
        connection.readTimeout = 20000
        try {
            if (body != null) {
                connection.doOutput = true
                connection.setRequestProperty("Content-Type", "application/json")
                connection.outputStream.withCloseable { it.write(JsonOutput.toJson(body).getBytes("UTF-8")) }
            }
            int status = connection.responseCode
            def stream = status < 400 ? connection.inputStream : connection.errorStream
            String response = stream == null ? "" : stream.withCloseable { it.getText("UTF-8") }
            if (!expected.contains(status)) {
                throw new IllegalStateException("${method} ${path}: ${status} ${response}")
            }
            return response ? new JsonSlurper().parseText(response) : [:]
        } finally {
            connection.disconnect()
        }
    }
    for (String namespace : [sourceDb, storeDb]) {
        request("POST", "/v1/namespaces", [namespace: [namespace], properties: [:]], [200, 409])
    }
    def version = { String text, Map summary, String defaultCatalog, String namespace, List representations ->
        def result = ["version-id": 1, "timestamp-ms": System.currentTimeMillis(), "schema-id": 0,
                      summary: summary, "default-namespace": namespace == null ? [] : [namespace],
                      representations: representations ?: [[type: "sql", dialect: "doris", sql: text]]]
        if (defaultCatalog != null) {
            result["default-catalog"] = defaultCatalog
        }
        return result
    }
    def createView = { String name, String text, List<String> names, Map summary = [:],
                       String defaultCatalog = catalog, String namespace = sourceDb, List representations = [] ->
        String path = "/v1/namespaces/${storeDb}/views"
        request("DELETE", "${path}/${name}", null, [200, 204, 404])
        def fields = names.withIndex().collect { String column, int index ->
            [id: index + 1, name: column, required: false, type: "int"]
        }
        def created = request("POST", path,
                [name: name, schema: [type: "struct", "schema-id": 0, fields: fields],
                 "view-version": version(text, summary, defaultCatalog, namespace, representations),
                 properties: [:]], [200]).metadata
        // CREATE may stamp a server-generated summary. Commit the required summary explicitly,
        // then load it back so engine-name cases exercise the metadata they claim to test.
        def current = created.versions.find { it["version-id"] == created["current-version-id"] }
        def next = new LinkedHashMap(current)
        next["version-id"] = current["version-id"] + 1
        next["timestamp-ms"] = System.currentTimeMillis()
        next.summary = summary
        request("POST", "${path}/${name}",
                [requirements: [[type: "assert-view-uuid", uuid: created["view-uuid"]]],
                 updates: [[action: "add-view-version", "view-version": next],
                           [action: "set-current-view-version", "view-version-id": next["version-id"]]]], [200])
        def loaded = request("GET", "${path}/${name}", null, [200]).metadata
        def persisted = loaded.versions.find { it["version-id"] == loaded["current-version-id"] }
        assertEquals(summary, persisted.summary)
        assertEquals(names, loaded.schemas.find { it["schema-id"] == persisted["schema-id"] }.fields*.name)
        assertEquals(defaultCatalog, persisted["default-catalog"])
        assertEquals(namespace == null ? [] : [namespace], persisted["default-namespace"])
        def expectedSql = representations ?: [[type: "sql", dialect: "doris", sql: text]]
        assertEquals(new JsonSlurper().parseText(JsonOutput.toJson(expectedSql)), persisted.representations)
    }
    def representation = { String dialect, int value ->
        [type: "sql", dialect: dialect, sql: "SELECT CAST(${value} AS INT) AS x"]
    }

    spark_iceberg_multi """
        CREATE NAMESPACE IF NOT EXISTS demo.${sourceDb};
        CREATE NAMESPACE IF NOT EXISTS demo.${storeDb};
        DROP TABLE IF EXISTS demo.${sourceDb}.t;
        CREATE TABLE demo.${sourceDb}.t (x INT, y INT) USING iceberg;
        INSERT INTO demo.${sourceDb}.t VALUES (10, 20), (11, 21);
        DROP TABLE IF EXISTS demo.${storeDb}.t;
        CREATE TABLE demo.${storeDb}.t (x INT, y INT) USING iceberg;
        INSERT INTO demo.${storeDb}.t VALUES (100, 200), (101, 201);
        DROP TABLE IF EXISTS demo.${sourceDb}.t_filter_unpartitioned;
        CREATE TABLE demo.${sourceDb}.t_filter_unpartitioned (id INT, p INT, v INT, n INT) USING iceberg;
        INSERT INTO demo.${sourceDb}.t_filter_unpartitioned VALUES
            (1, 10, 100, 1), (2, 10, 200, NULL), (3, 20, 300, 3),
            (4, 20, NULL, 4), (5, 30, 500, NULL), (6, NULL, 600, 6), (7, NULL, NULL, NULL);
        DROP TABLE IF EXISTS demo.${sourceDb}.t_filter_partitioned;
        CREATE TABLE demo.${sourceDb}.t_filter_partitioned (id INT, p INT, v INT, n INT)
            USING iceberg PARTITIONED BY (p);
        INSERT INTO demo.${sourceDb}.t_filter_partitioned SELECT * FROM demo.${sourceDb}.t_filter_unpartitioned;
        USE demo.${sourceDb};
        DROP VIEW IF EXISTS demo.${storeDb}.v_spark_created;
        CREATE VIEW demo.${storeDb}.v_spark_created (declared_name) AS SELECT x FROM t;
    """
    createView("v_engine_doris", "SELECT CAST(10 AS INT) AS x", ["x"], ["engine-name": "doris"])
    createView("v_engine_missing", "SELECT CAST(10 AS INT) AS x", ["x"])
    createView("v_engine_empty", "SELECT CAST(10 AS INT) AS x", ["x"], ["engine-name": ""])
    createView("v_engine_spark", "", ["x"], ["engine-name": "spark"], catalog, sourceDb,
            [representation("spark", 10), representation("doris", 20)])
    createView("v_engine_unknown", "", ["x"], ["engine-name": "unknown"], catalog, sourceDb,
            [representation("spark", 30), representation("doris", 40)])
    createView("v_prefer_doris", "", ["x"], [:], catalog, sourceDb,
            [representation("spark", 50), representation("doris", 60)])
    createView("v_fallback", "", ["x"], [:], catalog, sourceDb, [representation("spark", 70)])
    createView("v_rename", "SELECT x FROM t", ["declared_name"])
    createView("v_swap", "SELECT x, y FROM t", ["y", "x"])
    createView("v_repeat", "SELECT x, x FROM t", ["a", "b"])
    createView("v_computed", "SELECT x + 1, y + 2 FROM t", ["declared_a", "declared_b"])
    createView("v_nested", "SELECT a, b FROM v_repeat", ["nested_a", "nested_b"], [:], catalog, storeDb)
    createView("v_no_default_catalog", "SELECT x FROM t", ["x"], [:], null)
    createView("v_missing_default_constant", "SELECT CAST(10 AS INT) AS x", ["x"], [:], "missing_view_catalog")
    createView("v_missing_default_qualified", "SELECT x FROM ${catalog}.${sourceDb}.t", ["x"],
            [:], "missing_view_catalog")
    createView("v_missing_default_cte", "WITH c AS (SELECT CAST(80 AS INT) AS x) SELECT x FROM c", ["x"],
            [:], "missing_view_catalog")
    createView("v_missing_default_subquery", "SELECT (SELECT MAX(x) FROM ${catalog}.${sourceDb}.t) AS x", ["x"],
            [:], "missing_view_catalog")
    createView("v_missing_default_unqualified", "SELECT x FROM t", ["x"], [:], "missing_view_catalog")
    createView("v_missing_default_db_qualified", "SELECT x FROM ${sourceDb}.t", ["x"], [:], "missing_view_catalog")
    createView("v_missing_default_nested", "SELECT a, b FROM ${catalog}.${storeDb}.v_repeat", ["a", "b"],
            [:], "missing_view_catalog", null)
    createView("v_refresh", "SELECT CAST(10 AS INT) AS x", ["a"])
    for (String layout : ["partitioned", "unpartitioned"]) {
        def metadata = request("GET", "/v1/namespaces/${sourceDb}/tables/t_filter_${layout}", null, [200]).metadata
        def spec = metadata["partition-specs"].find { it["spec-id"] == metadata["default-spec-id"] }
        assertEquals(layout == "partitioned" ? ["identity"] : [], spec.fields*.transform)
        assertEquals(layout == "partitioned" ? [2] : [], spec.fields*.get("source-id"))
        createView("v_filter_${layout}", "SELECT id, p, v, n FROM t_filter_${layout}", ["id", "p", "v", "n"])
        createView("v_filter_${layout}_inner", """SELECT id, p, v, n FROM t_filter_${layout}
            WHERE (p IN (10, 20) OR p IS NULL) AND id >= 2""", ["id", "p", "v", "n"])
        createView("v_filter_${layout}_renamed", "SELECT id, p, v, n FROM t_filter_${layout}",
                ["row_id", "part_key", "metric", "nullable_metric"])
        createView("v_filter_${layout}_swapped", "SELECT id, p, v, n FROM t_filter_${layout}", ["id", "p", "n", "v"])
        createView("v_filter_${layout}_computed", "SELECT id, p, v + 1, COALESCE(n, -1) FROM t_filter_${layout}",
                ["id", "p", "v_plus_one", "n_filled"])
        createView("v_filter_${layout}_repeat", "SELECT id, p, v, v FROM t_filter_${layout}", ["id", "p", "a", "b"])
        createView("v_filter_${layout}_nested", """SELECT row_id, part_key, metric, nullable_metric
            FROM v_filter_${layout}_renamed WHERE metric >= 200 OR metric IS NULL""",
                ["id", "p", "v", "n"], [:], catalog, storeDb)
    }
    // An empty namespace is valid Iceberg metadata; omitting the namespace field is not.
    for (String mode : ["empty_namespace", "no_defaults"]) {
        String defaultCatalog = mode == "empty_namespace" ? catalog : null
        createView("v_${mode}_constant", "SELECT CAST(10 AS INT) AS x", ["x"], [:], defaultCatalog, null)
        createView("v_${mode}_qualified", "SELECT x FROM ${catalog}.${sourceDb}.t", ["x"], [:], defaultCatalog, null)
        createView("v_${mode}_db_qualified", "SELECT x FROM ${sourceDb}.t", ["x"], [:], defaultCatalog, null)
        createView("v_${mode}_unqualified", "SELECT x FROM t", ["x"], [:], defaultCatalog, null)
    }
    createView("v_no_defaults_partitioned", "SELECT id, p, v, n FROM ${catalog}.${sourceDb}.t_filter_partitioned",
            ["id", "p", "v", "n"], [:], null, null)
    createView("v_no_defaults_partitioned_db_qualified", "SELECT id, p, v, n FROM ${sourceDb}.t_filter_partitioned",
            ["id", "p", "v", "n"], [:], null, null)
    createView("v_no_defaults_nested", "SELECT x FROM ${catalog}.${storeDb}.v_empty_namespace_qualified",
            ["x"], [:], null, null)

    sql """DROP CATALOG IF EXISTS ${catalog}"""
    sql """CREATE CATALOG ${catalog} PROPERTIES (
        'type' = 'iceberg', 'iceberg.catalog.type' = 'rest', 'uri' = '${rest}',
        's3.access_key' = '${accessKey}', 's3.secret_key' = '${secretKey}',
        's3.endpoint' = 'http://${host}:${config.get('iceberg_minio_port')}',
        's3.region' = 'us-east-1', 'use_path_style' = 'true')"""
    sql """USE ${catalog}.${storeDb}"""

    order_qt_table_control "SELECT x, y FROM ${catalog}.${sourceDb}.t"
    order_qt_spark_created "SELECT declared_name FROM v_spark_created"
    order_qt_engine_doris "SELECT * FROM v_engine_doris"
    order_qt_engine_missing "SELECT * FROM v_engine_missing"
    order_qt_engine_empty "SELECT * FROM v_engine_empty"
    order_qt_preserve_engine "SELECT * FROM v_engine_spark"
    order_qt_unknown_engine_fallback "SELECT * FROM v_engine_unknown"
    order_qt_prefer_doris "SELECT * FROM v_prefer_doris"
    order_qt_fallback "SELECT * FROM v_fallback"
    order_qt_declared_schema "DESC v_swap"
    order_qt_renamed "SELECT declared_name FROM v_rename"
    order_qt_swapped "SELECT x, y FROM v_swap"
    order_qt_repeated "SELECT a, b FROM v_repeat"
    order_qt_replace_one "SELECT * REPLACE (100 AS a) FROM v_repeat"
    order_qt_replace_both "SELECT * REPLACE (100 AS a, 200 AS b) FROM v_repeat"
    order_qt_except_one "SELECT * EXCEPT (a) FROM v_repeat"
    order_qt_computed "SELECT declared_a, declared_b FROM v_computed"
    order_qt_nested "SELECT nested_a, nested_b FROM v_nested"
    order_qt_restore_namespace "SELECT v.nested_a, caller.x FROM v_nested v JOIN t caller ON caller.x = v.nested_a + 90"
    order_qt_no_default_catalog "SELECT x FROM v_no_default_catalog"
    order_qt_missing_unused_default "SELECT x FROM v_missing_default_constant"
    order_qt_missing_default_qualified "SELECT x FROM v_missing_default_qualified"
    order_qt_missing_default_cte "SELECT x FROM v_missing_default_cte"
    order_qt_missing_default_subquery "SELECT x FROM v_missing_default_subquery"
    order_qt_missing_default_nested "SELECT a, b FROM v_missing_default_nested"
    for (String mode : ["empty_namespace", "no_defaults"]) {
        "order_qt_${mode}_constant" "SELECT x FROM v_${mode}_constant"
        "order_qt_${mode}_qualified" "SELECT x FROM v_${mode}_qualified"
        "order_qt_${mode}_db_qualified" "SELECT x FROM v_${mode}_db_qualified"
        test {
            sql "SELECT x FROM v_${mode}_unqualified"
            exception "Current database is not set."
        }
    }
    order_qt_no_defaults_partitioned """SELECT id, p, v, n FROM v_no_defaults_partitioned
        WHERE (p = 20 OR p IS NULL) AND (v >= 300 OR v IS NULL)"""
    order_qt_no_defaults_partitioned_db_qualified """SELECT id, p, v, n FROM v_no_defaults_partitioned_db_qualified
        WHERE p = 10"""
    order_qt_no_defaults_nested "SELECT x FROM v_no_defaults_nested"
    order_qt_namespace_restored_after_empty "SELECT x FROM t"

    for (String name : ["v_missing_default_unqualified", "v_missing_default_db_qualified"]) {
        test {
            sql "SELECT * FROM ${name}"
            exception "Unknown catalog 'missing_view_catalog'"
        }
    }
    order_qt_restore_after_error "SELECT x, y FROM t"
    for (String selector : ["@branch(main)", "@tag(t1)"]) {
        test {
            sql "SELECT * FROM v_repeat${selector}"
            exception "view not supported with scan params"
        }
    }
    for (String selector : ["FOR VERSION AS OF 1", "FOR TIME AS OF '2026-01-01 00:00:00'"]) {
        test {
            sql "SELECT * FROM v_repeat ${selector}"
            exception "view not supported with snapshot time/version travel"
        }
    }
    test {
        sql "SELECT * EXCEPT (a, b) FROM v_repeat"
        exception "All slots in * EXCEPT clause are excepted"
    }
    test {
        sql "SELECT x FROM v_rename"
        exception "Unknown column"
    }

    // Both reads and CTAS writes execute on BE, including two independent aliases of one source slot.
    sql "CREATE DATABASE IF NOT EXISTS internal.iceberg_rest_view_binding_internal"
    sql "DROP TABLE IF EXISTS internal.iceberg_rest_view_binding_internal.ctas_computed"
    sql """CREATE TABLE internal.iceberg_rest_view_binding_internal.ctas_computed
        DISTRIBUTED BY HASH(declared_a) BUCKETS 1 PROPERTIES ('replication_num' = '1')
        AS SELECT * FROM v_computed"""
    order_qt_ctas_computed "SELECT declared_a, declared_b FROM internal.iceberg_rest_view_binding_internal.ctas_computed"
    sql "DROP TABLE IF EXISTS internal.iceberg_rest_view_binding_internal.ctas_repeat"
    sql """CREATE TABLE internal.iceberg_rest_view_binding_internal.ctas_repeat
        DISTRIBUTED BY HASH(a) BUCKETS 1 PROPERTIES ('replication_num' = '1')
        AS SELECT * REPLACE (100 AS a) FROM v_repeat"""
    order_qt_ctas_repeat "SELECT a, b FROM internal.iceberg_rest_view_binding_internal.ctas_repeat"

    order_qt_refresh_before "SELECT a FROM v_refresh"
    String refreshPath = "/v1/namespaces/${storeDb}/views/v_refresh"
    def previous = request("GET", refreshPath, null, [200]).metadata
    int schemaId = (previous.schemas*.get("schema-id").max() as int) + 1
    int versionId = (previous.versions*.get("version-id").max() as int) + 1
    def updatedVersion = version("SELECT CAST(20 AS INT) AS x", [:], catalog, sourceDb, [])
    updatedVersion["version-id"] = versionId
    updatedVersion["schema-id"] = schemaId
    def updated = request("POST", refreshPath,
            [requirements: [[type: "assert-view-uuid", uuid: previous["view-uuid"]]],
             updates: [[action: "add-schema", schema: [type: "struct", "schema-id": schemaId,
                        fields: [[id: 1, name: "b", required: false, type: "int"]]]],
                       [action: "add-view-version", "view-version": updatedVersion],
                       [action: "set-current-view-version", "view-version-id": versionId]]], [200]).metadata
    assertEquals(previous["view-uuid"], updated["view-uuid"])
    assertEquals(versionId, updated["current-version-id"])
    sql "REFRESH CATALOG ${catalog}"
    order_qt_refresh_after "SELECT b FROM v_refresh"
    test {
        sql "SELECT a FROM v_refresh"
        exception "Unknown column"
    }

    // The two physical layouts hold identical values, including NULL partition keys and payloads.
    // Every predicate is checked through both a view and a direct base-table scan.
    def predicates = [
        all_rows: "TRUE",
        partition_equal: "p = 10",
        partition_range: "p >= 10 AND p < 30",
        partition_in: "p IN (10, 30)",
        partition_null: "p IS NULL",
        partition_not_null: "p IS NOT NULL",
        partition_not_in: "p NOT IN (10, 30)",
        value_equal: "v = 300",
        value_range: "v BETWEEN 200 AND 500",
        value_null: "v IS NULL",
        value_not_null: "v IS NOT NULL",
        compound: "p = 20 AND (v >= 300 OR v IS NULL)",
        or_null: "p = 10 OR p IS NULL",
        null_safe: "p <=> NULL",
        not_value: "NOT (v >= 300)",
        not_in_null: "v NOT IN (100, NULL)",
        arithmetic: "v + COALESCE(n, 0) >= 303",
        empty_partition: "p = 999",
        empty_value: "v > 9999",
        null_combination: "n IS NULL AND (v IS NOT NULL OR p IS NULL)"
    ]
    for (String layout : ["partitioned", "unpartitioned"]) {
        predicates.each { String name, String predicate ->
            "order_qt_${layout}_${name}_view" "SELECT id, p, v, n FROM v_filter_${layout} WHERE ${predicate}"
            "order_qt_${layout}_${name}_table" """SELECT id, p, v, n
                FROM ${catalog}.${sourceDb}.t_filter_${layout} WHERE ${predicate}"""
        }
        "order_qt_${layout}_inner_outer_where" """SELECT id, p, v, n FROM v_filter_${layout}_inner
            WHERE p = 20 OR p IS NULL"""
        "order_qt_${layout}_renamed_where" """SELECT row_id, part_key, metric, nullable_metric
            FROM v_filter_${layout}_renamed
            WHERE part_key IN (10, 20) AND (metric >= 200 OR metric IS NULL)"""
        "order_qt_${layout}_swapped_where" "SELECT id, p, v, n FROM v_filter_${layout}_swapped WHERE v >= 3"
        "order_qt_${layout}_computed_where" """SELECT id, p, v_plus_one, n_filled FROM v_filter_${layout}_computed
            WHERE v_plus_one >= 301 AND n_filled >= 3"""
        "order_qt_${layout}_nested_where" """SELECT id, p, v, n FROM v_filter_${layout}_nested
            WHERE p = 20 OR p IS NULL"""
        "order_qt_${layout}_repeat_where" """SELECT id, a, b
            FROM (SELECT * REPLACE (999 AS a) FROM v_filter_${layout}_repeat) r
            WHERE a = 999 AND b >= 300"""
        "order_qt_${layout}_where_group_having_view" """SELECT p, COUNT(*), COUNT(v), SUM(v), MIN(n), MAX(n)
            FROM v_filter_${layout} WHERE id >= 2 GROUP BY p HAVING COUNT(*) >= 1"""
        "order_qt_${layout}_where_group_having_table" """SELECT p, COUNT(*), COUNT(v), SUM(v), MIN(n), MAX(n)
            FROM ${catalog}.${sourceDb}.t_filter_${layout} WHERE id >= 2 GROUP BY p HAVING COUNT(*) >= 1"""
        "qt_${layout}_where_topn" """SELECT id, p, v, n FROM v_filter_${layout}
            WHERE p >= 10 OR p IS NULL ORDER BY v DESC NULLS LAST, id LIMIT 4"""
    }
    order_qt_where_mixed_view_join """SELECT l.id, l.p, l.v, r.n FROM v_filter_partitioned l
        JOIN v_filter_unpartitioned r ON l.id = r.id
        WHERE (l.p = 20 OR l.p IS NULL) AND (r.v >= 300 OR r.v IS NULL)"""
    order_qt_where_view_table_join """SELECT v.id, v.p, v.v, t.n FROM v_filter_partitioned v
        JOIN ${catalog}.${sourceDb}.t_filter_unpartitioned t ON v.id = t.id
        WHERE t.p IN (10, 30) AND v.v >= 200"""
    order_qt_where_left_join """SELECT l.id, r.id, r.v FROM v_filter_partitioned l
        LEFT JOIN v_filter_unpartitioned r ON l.id = r.id AND r.v >= 300
        WHERE l.p IS NULL OR r.id IS NULL"""
    order_qt_where_union_all """SELECT id, COUNT(*), SUM(v) FROM (
        SELECT id, v FROM v_filter_partitioned WHERE p = 20
        UNION ALL
        SELECT id, v FROM v_filter_unpartitioned WHERE p = 20) u GROUP BY id"""

    sql "DROP TABLE IF EXISTS internal.iceberg_rest_view_binding_internal.ctas_where_partitioned"
    sql """CREATE TABLE internal.iceberg_rest_view_binding_internal.ctas_where_partitioned
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')
        AS SELECT id, p, v, n FROM v_filter_partitioned
        WHERE (p = 20 OR p IS NULL) AND (v >= 300 OR v IS NULL)"""
    order_qt_where_ctas_partitioned "SELECT * FROM internal.iceberg_rest_view_binding_internal.ctas_where_partitioned"
    order_qt_where_ctas_partitioned_count "SELECT COUNT(*) FROM internal.iceberg_rest_view_binding_internal.ctas_where_partitioned"
    sql "DROP TABLE IF EXISTS internal.iceberg_rest_view_binding_internal.ctas_where_unpartitioned"
    sql """CREATE TABLE internal.iceberg_rest_view_binding_internal.ctas_where_unpartitioned
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')
        AS SELECT id, p, v, n FROM v_filter_unpartitioned
        WHERE (p = 20 OR p IS NULL) AND (v >= 300 OR v IS NULL)"""
    order_qt_where_ctas_unpartitioned "SELECT * FROM internal.iceberg_rest_view_binding_internal.ctas_where_unpartitioned"
    order_qt_where_ctas_unpartitioned_count "SELECT COUNT(*) FROM internal.iceberg_rest_view_binding_internal.ctas_where_unpartitioned"
    // Keep the catalog, source files, views and CTAS tables for debugging.
}
