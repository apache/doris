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
import java.security.MessageDigest

suite("variant_relational_performance", "p2,nonConcurrent") {
    def env = System.getenv()
    // This benchmark is driven by run_relational_benchmark.py. A plain variant_p2 run has no
    // prepared dimension tables, so the suite only runs when a phase is set explicitly.
    def phase = env.get("VARIANT_BENCH_PHASE")
    if (phase == null) {
        log.info("Skip variant_relational_performance: VARIANT_BENCH_PHASE is not set")
        return
    }
    long expectedRows = env.getOrDefault("VARIANT_BENCH_ROWS", "44273863").toLong()
    int repeats = env.getOrDefault("VARIANT_BENCH_REPEATS", "7").toInteger()
    int warmups = env.getOrDefault("VARIANT_BENCH_WARMUPS", "2").toInteger()
    if (!(phase in ["prepare", "query"]) || expectedRows < 1 || repeats < 1 || warmups < 0) {
        throw new IllegalArgumentException("Invalid VARIANT_BENCH configuration")
    }
    // These paths come directly from variant_p2/sql and the GitHub Events schema.
    def keys = [
        actor_login: [column: "actor", path: "login", type: "STRING"],
        repo_name: [column: "repo", path: "name", type: "STRING"],
        payload_action: [column: "payload", path: "action", type: "STRING"],
        actor_id: [column: "actor", path: "id", type: "BIGINT"]
    ]
    def requestedKeys = env.getOrDefault("VARIANT_BENCH_KEYS", "actor_login,actor_id").split(",") as Set
    if (!keys.keySet().containsAll(requestedKeys)) {
        throw new IllegalArgumentException("Unknown VARIANT_BENCH_KEYS: ${requestedKeys - keys.keySet()}")
    }
    keys = keys.findAll { key, ignored -> requestedKeys.contains(key) }

        def actualRows = (sql("SELECT count(*) FROM github_events"))[0][0].toString().toLong()
        assertEquals(expectedRows, actualRows)
        if (phase == "prepare") {
            sql "SET default_variant_max_subcolumns_count = 0"
            keys.each { key, spec ->
                def nativeKey = "${spec.column}['${spec.path}']"
                sql "DROP TABLE IF EXISTS variant_relational_dim_${key}"
                sql """CREATE TABLE variant_relational_dim_${key} (id BIGINT, k VARIANT)
                    DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 16
                    PROPERTIES("replication_num"="1")"""
                sql """INSERT INTO variant_relational_dim_${key}
                    SELECT min(id), ${nativeKey} FROM github_events
                    WHERE ${nativeKey} IS NOT NULL GROUP BY ${nativeKey}"""
            }
            return
        }

        sql "SET parallel_pipeline_task_num = 8"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        if (env.getOrDefault("VARIANT_BENCH_SPILL", "false").toBoolean()) {
            sql "SET enable_spill = true"
            sql "SET enable_force_spill = true"
            sql "SET spill_min_revocable_mem = 1"
        }
        def output = new File(env.getOrDefault("VARIANT_BENCH_RESULTS",
                "tmp/variant-relational-results.jsonl"))
        output.parentFile.mkdirs()
        def record = { event -> output.append(JsonOutput.toJson(event) + "\n") }
        def fingerprint = { result ->
            MessageDigest.getInstance("SHA-256").digest(JsonOutput.toJson(
                result.collect { row -> row.collect { value -> value == null ? null : value.toString() } }
            ).getBytes("UTF-8")).encodeHex().toString()
        }
        def query = { statement ->
            long start = System.nanoTime()
            def result = sql(statement)
            [ms: (System.nanoTime() - start) / 1e6, hash: fingerprint(result), resultRows: result.size()]
        }
        record([event: "start", rows: actualRows, repeats: repeats, warmups: warmups,
                parallelPipelineTasks: 8, keys: keys.keySet(), cpus: env.get("VARIANT_BENCH_CPUS"),
                spill: env.get("VARIANT_BENCH_SPILL"), time: new Date().toString()])
        keys.each { key, spec ->
            def nativeKey = "${spec.column}['${spec.path}']"
            def castKey = "CAST(${nativeKey} AS ${spec.type})"
            def nativeGroups = """SELECT ${castKey} k, min(id) first_id, count(*) n
                FROM github_events GROUP BY ${nativeKey}"""
            def castGroups = """SELECT ${castKey} k, min(id) first_id, count(*) n
                FROM github_events GROUP BY ${castKey}"""
            assertEquals(0, (sql("""SELECT count(*) FROM (
                (${nativeGroups}) EXCEPT (${castGroups})) difference"""))[0][0].toString().toInteger())
            assertEquals(0, (sql("""SELECT count(*) FROM (
                (${castGroups}) EXCEPT (${nativeGroups})) difference"""))[0][0].toString().toInteger())
            record([event: "full_group_correctness", key: key])

            // ORDER BY ... LIMIT is a TopN and never sorts the whole input. The window forces a
            // full sort of every row, and its order-sensitive checksum compares the complete
            // native order with the CAST order instead of the first rows only.
            def fullSort = { orderKey -> """SELECT count(*), sum(CAST(id AS LARGEINT) * rn) FROM (
                SELECT id, row_number() OVER (ORDER BY ${orderKey} NULLS FIRST, id) rn
                FROM github_events) ranked""" }
            def queries = [
                group: [
                    native: "SELECT count(*), sum(n*n), sum(first_id) FROM (${nativeGroups}) g",
                    cast: "SELECT count(*), sum(n*n), sum(first_id) FROM (${castGroups}) g"
                ],
                order_topn: [
                    native: "SELECT id FROM github_events ORDER BY ${nativeKey} NULLS FIRST, id LIMIT 1000",
                    cast: "SELECT id FROM github_events ORDER BY ${castKey} NULLS FIRST, id LIMIT 1000"
                ],
                order_full: [native: fullSort(nativeKey), cast: fullSort(castKey)]
            ]

            // A join against an empty or stale dimension still makes native and CAST agree, so
            // prove the dimension is the current grouping of the source before timing a join.
            def dimension = "SELECT id, CAST(k AS ${spec.type}) k FROM variant_relational_dim_${key}"
            def expectedDimension = """SELECT min(id) id, ${castKey} k FROM github_events
                WHERE ${nativeKey} IS NOT NULL GROUP BY ${castKey}"""
            def dimensionRows = (sql("SELECT count(*) FROM variant_relational_dim_${key}"))[0][0]
                    .toString().toLong()
            assertTrue(dimensionRows > 0)
            assertEquals(0, (sql("""SELECT count(*) FROM (
                (${dimension}) EXCEPT (${expectedDimension})) difference"""))[0][0].toString().toInteger())
            assertEquals(0, (sql("""SELECT count(*) FROM (
                (${expectedDimension}) EXCEPT (${dimension})) difference"""))[0][0].toString().toInteger())
            record([event: "dimension_verified", key: key, rows: dimensionRows])

            // Variant keys get no runtime filter while the CAST keys do, so cast_no_rf isolates the
            // key comparison cost from the runtime filter benefit.
            def leftKey = "l.${spec.column}['${spec.path}']"
            def castPredicate = "CAST(${leftKey} AS ${spec.type}) = CAST(r.k AS ${spec.type})"
            ["broadcast", "shuffle"].each { distribution ->
                def join = { hint, predicate -> """SELECT ${hint} count(*), sum(l.id) FROM github_events l
                    JOIN [${distribution}] variant_relational_dim_${key} r ON ${predicate}""" }
                queries["join_${distribution}".toString()] = [
                    native: join("", "${leftKey} = r.k"),
                    cast: join("", castPredicate),
                    cast_no_rf: join("/*+ SET_VAR(runtime_filter_mode='OFF') */", castPredicate)
                ]
            }

            queries.each { operation, statements ->
                def oracle = query(statements.cast).hash
                statements.each { mode, statement ->
                    record([event: "plan", key: key, operation: operation, mode: mode,
                            plan: sql("EXPLAIN ${statement}")])
                }
                def modes = statements.keySet() as List
                for (int round = -warmups; round < repeats; ++round) {
                    // Rotate the starting mode so no mode always runs first or last.
                    Collections.rotate(modes, 1)
                    modes.each { mode ->
                        def sample = query(statements[mode])
                        assertEquals(oracle, sample.hash)
                        record(sample + [event: "sample", key: key, operation: operation,
                                mode: mode, round: round, sql: statements[mode]])
                    }
                }
            }
        }
        record([event: "complete", time: new Date().toString()])
}
