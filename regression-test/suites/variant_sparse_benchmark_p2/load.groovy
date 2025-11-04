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

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.Random

suite("variant_stream_load_batch", "variant_type,p2") {
    // read parameters from env or default values
    // N: number of keys in variant JSON object: {"id": <id>, "var": {"key0":0, ..., "keyN":N}}
    // M: rows per batch for stream load
    // BATCHES (optional): number of batches to send; if not set, compute minimal batches to cover TOTAL_ROWS
    int N = 1000 
    int M = 10240
    Integer BATCHES = System.getenv("VARIANT_BATCHES")?.toInteger()
    // TOTAL_ROWS: optional, total rows to generate; default M * (BATCHES or 1)
    // Integer TOTAL_ROWS = 10000000
    Integer TOTAL_ROWS =5700000
    int THREADS = (System.getenv("VARIANT_THREADS") ?: "10").toInteger()
    int MAXV = (System.getenv("VARIANT_MAXV") ?: "10000").toInteger()
    long SEED = (System.getenv("VARIANT_SEED") ?: "${System.nanoTime()}").toLong()

    def tbl = "variant_batch_t5"

    def create_table = { ->
        // allow unlimited variant subcolumns for this test
        sql "set default_variant_max_subcolumns_count = 1"
        sql """
            DROP TABLE IF EXISTS ${tbl}
        """
        sql """
            CREATE TABLE IF NOT EXISTS ${tbl} (
                id BIGINT NOT NULL,
                v_map Map<String, String> NULL,
                v_json JSON NULL,
                v_variant_1 VARIANT<properties("variant_sparse_hash_shard_count" = "1")> NULL,
                v_variant_32 VARIANT<properties("variant_sparse_hash_shard_count" = "32")> NULL,
                v_variant_64 VARIANT<properties("variant_sparse_hash_shard_count" = "64")> NULL
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    }

    def load_json_lines = { String jsonPath ->
        streamLoad {
            table "${tbl}"
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            set 'max_filter_ratio', '0.1'
            file jsonPath
            time 120000 // 120s
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                logger.info("Stream load ${jsonPath} result: ${result}".toString())
                assertEquals("success", json.Status.toLowerCase())
                assertTrue(json.NumberLoadedRows > 0)
            }
        }
    }

    // generate one batch json file and stream load it
    def generate_and_load_batch = { int batchIndex, int startId, int rowsInBatch ->
        def tmpDir = new File("/tmp/doris_regression_variant")
        if (!tmpDir.exists()) tmpDir.mkdirs()
        def f = new File(tmpDir, "variant_batch_${batchIndex}.json")
        def rnd = new Random(SEED + batchIndex)

        // Write JSON lines
        f.withWriter("UTF-8") { w ->
            for (int i = 0; i < rowsInBatch; i++) {
                long id = startId + i
                // build json line to match table schema: id, v_map, v_json, v_variant_1/32/64
                // v_map uses string values; others use numeric values
                // generate one value per key so map/json/variant share the same value
                int[] vals = new int[N + 1]
                for (int k = 0; k <= N; k++) {
                    vals[k] = rnd.nextInt(MAXV)
                }
                StringBuilder sb = new StringBuilder(64 + (N + 1) * 50)
                sb.append('{')
                sb.append("\"id\":").append(id).append(',')

                // v_map Map<String, String>
                sb.append("\"v_map\":{")
                for (int k = 0; k <= N; k++) {
                    if (k > 0) sb.append(',')
                    int v = vals[k]
                    sb.append("\"key").append(k).append("\":\"").append(v).append("\"")
                }
                sb.append('}').append(',')

                // v_json JSON (numeric values)
                sb.append("\"v_json\":{")
                for (int k = 0; k <= N; k++) {
                    if (k > 0) sb.append(',')
                    int v = vals[k]
                    sb.append("\"key").append(k).append("\":").append(v)
                }
                sb.append('}').append(',')

                // v_variant_1
                sb.append("\"v_variant_1\":{")
                for (int k = 0; k <= N; k++) {
                    if (k > 0) sb.append(',')
                    int v = vals[k]
                    sb.append("\"key").append(k).append("\":").append(v)
                }
                sb.append('}').append(',')

                // v_variant_32
                sb.append("\"v_variant_32\":{")
                for (int k = 0; k <= N; k++) {
                    if (k > 0) sb.append(',')
                    int v = vals[k]
                    sb.append("\"key").append(k).append("\":").append(v)
                }
                sb.append('}').append(',')

                // v_variant_64
                sb.append("\"v_variant_64\":{")
                for (int k = 0; k <= N; k++) {
                    if (k > 0) sb.append(',')
                    int v = vals[k]
                    sb.append("\"key").append(k).append("\":").append(v)
                }
                sb.append('}')

                // end of row object
                sb.append('}')
                w.write(sb.toString())
                w.write("\n")
            }
        }

        // stream load
        load_json_lines.call(f.getAbsolutePath())
        // remove file after load to save space
        try { f.delete() } catch (Throwable ignore) {}
    }

    try {
        create_table.call()

        int batches
        if (TOTAL_ROWS != null) {
            batches = (int) Math.ceil(TOTAL_ROWS / (double) M)
        } else if (BATCHES != null) {
            batches = BATCHES
        } else {
            batches = 1
            TOTAL_ROWS = M
        }
        if (TOTAL_ROWS == null) {
            TOTAL_ROWS = batches * M
        }

        def executorService = Executors.newFixedThreadPool(THREADS)
        def futures = []
        try {
            for (int b = 0; b < batches; b++) {
                int startIdForBatch = b * M + 1
                int remainingForBatch = TOTAL_ROWS - b * M
                int rowsThisBatch = Math.min(M, Math.max(remainingForBatch, 0))
                if (rowsThisBatch <= 0) break
                def batchIndex = b
                futures << executorService.submit({
                    generate_and_load_batch.call(batchIndex, startIdForBatch, rowsThisBatch)
                } as Runnable)
            }
            // wait for all tasks
            try {
                futures.each { f -> f.get() }
            } catch (Exception e) {
                throw e.cause ?: e
            }
        } finally {
            executorService.shutdown()
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)
        }

        // verify row count
        def res = sql "select count() from ${tbl}"
        assertTrue(res[0][0] as long == (long) TOTAL_ROWS)
    } finally {
        // nothing
    }
}


