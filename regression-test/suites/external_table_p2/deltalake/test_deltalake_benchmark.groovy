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
import org.apache.doris.regression.util.ResultUtils
import java.sql.SQLException

// The reference is computed from Hive on the same physical Parquet files, not a constant result.
suite("test_deltalake_benchmark", "p2,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))
            || !"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeBenchmarkTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    for (String reader : ["delta", "hive"]) {
        sql "drop catalog if exists test_deltalake_benchmark_${reader}"
        String metadata = reader == "delta" ? "'type'='deltalake', 'warehouse'='${warehouse}'"
                : "'type'='hms', 'hive.metastore.uris'='thrift://${host}:${hmsPort}'"
        sql """create catalog test_deltalake_benchmark_${reader} properties (
            ${metadata}, 's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
    }
    sql "set enable_sql_cache=false"
    sql "set enable_file_cache=false"
    sql "set enable_file_scanner_v2=true"
    sql "set parallel_pipeline_task_num=4"
    sql "set exec_mem_limit=8589934592"
    sql "set query_timeout=120"
    sql "set time_zone='UTC'"
    String queryPattern = context.config.otherConfigs.getOrDefault("deltalakeBenchmarkQueryPattern", ".*")
    List<String> failedQueries = []
    int executedQueries = 0
    File repository = new File(context.config.suitePath).canonicalFile.parentFile.parentFile
    for (String workload : ["tpch", "tpcds"]) {
        File directory = new File(repository, "tools/${workload}-tools/queries"
                + (workload == "tpcds" ? "/sf100" : ""))
        List<File> queries = directory.listFiles().findAll { it.name.endsWith('.sql') }.sort { it.name }
        assertEquals(workload == "tpch" ? 22 : 103, queries.size(), "${workload} query inventory")
        for (File file : queries) {
            String queryName = "${workload}/${file.name}"
            if (!(queryName ==~ queryPattern)) {
                continue
            }
            executedQueries++
            // Match the existing Hive TPC-H regression's Q7 control on both readers.
            // Its default Hive estimate joins supplier/customer before orders and exceeds memory.
            boolean fixedJoinOrder = workload == "tpch" && file.name == "q7.sql"
            sql "set disable_join_reorder=${fixedJoinOrder}"
            String query = file.getText("UTF-8")
            if (workload == "tpcds" && file.name == "query30.sql") {
                // TPC-DS 3.2's generator DDL omits the old query template's _sk suffix.
                query = query.replace("c_last_review_date_sk", "c_last_review_date")
            }
            if (workload == "tpch" && file.name == "q15.sql") {
                // The existing tool query refers to a view; make it a CTE for read-only catalogs.
                query = """with revenue0 as (
                    select l_suppkey supplier_no, sum(l_extendedprice * (1-l_discount)) total_revenue
                    from lineitem where l_shipdate >= date '1996-01-01'
                        and l_shipdate < date '1996-01-01' + interval '3' month
                    group by l_suppkey
                ) ${query}"""
            }
            List reference = null
            def runQuery = { String reader ->
                sql "switch test_deltalake_benchmark_${reader}"
                sql "use delta_${workload}"
                long start = System.nanoTime()
                List rows = sql(query)
                double milliseconds = (System.nanoTime() - start) / 1_000_000.0
                List normalized = rows.collect { row ->
                    row.collect { value -> value instanceof BigDecimal ? value.stripTrailingZeros() : value }
                }.sort { it.toString() }
                if (reference == null) {
                    reference = normalized
                } else {
                    // The shared typed comparator keeps exact decimals/integers and tolerates
                    // floating aggregate round-off (for example, TPC-DS Q39 stddev_samp).
                    ResultUtils.assertSparkDorisResultEquals(reference, normalized)
                }
                return milliseconds
            }
            // Try both readers even if one cannot execute a query. Keep every failure visible,
            // continue collecting coverage, and fail the suite after the remaining queries run.
            List failures = []
            def attempt = { String reader, String phase ->
                try {
                    return runQuery(reader)
                } catch (SQLException failure) {
                    Map detail = [workload: workload, query: file.name, reader: reader,
                                  phase: phase, error: failure.message]
                    failures.add(detail)
                    logger.warn("DELTA_BENCHMARK_FAILURE " + JsonOutput.toJson(detail))
                    return null
                }
            }
            Map first = [hive: attempt("hive", "first"), delta: attempt("delta", "first")]
            Map samples = [hive: [], delta: []]
            for (int round = 0; round < 3 && failures.isEmpty(); round++) {
                for (String reader : (round % 2 == 0 ? ["delta", "hive"] : ["hive", "delta"])) {
                    samples[reader].add(attempt(reader, "repeat-${round + 1}"))
                }
            }
            if (!failures.isEmpty()) {
                failedQueries.add(queryName)
            }
            logger.info("DELTA_BENCHMARK " + JsonOutput.toJson([
                    workload: workload, query: file.name, rows: reference?.size(),
                    status: failures.isEmpty() ? "complete" : "failed",
                    fixed_join_order: fixedJoinOrder,
                    first_ms: first, samples_ms: samples]))
        }
    }
    assertTrue(executedQueries > 0, "No benchmark queries matched ${queryPattern}")
    assertTrue(failedQueries.isEmpty(), "Benchmark queries failed: ${failedQueries}")
}
