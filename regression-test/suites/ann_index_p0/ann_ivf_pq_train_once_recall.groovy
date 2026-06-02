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

// Regression for #63913: the ANN index writer must train the FAISS quantizer
// EXACTLY ONCE per index build, not once per buffered chunk.
//
// Background: AnnIndexColumnWriter buffers vectors and flushes them one chunk at
// a time (ann_index_build_chunk_size). The buggy code called train() on every
// chunk. For a PQ (product-quantization) index, train() re-fits the codebook on
// the latest chunk, but vectors from earlier chunks were already add()ed and
// encoded under the previous codebook. After the final chunk re-trains, those
// earlier codes no longer match the stored codebook, so they decode to garbage
// distances at query time -> recall collapses on any segment that spans more
// than one chunk.
//
// This test shrinks ann_index_build_chunk_size so a single 20k-row segment spans
// 10 chunks, builds an IVF+PQ index, and asserts recall@10 (vs exact brute-force
// l2_distance) stays high. On a buggy BE this recall drops to ~0.1 and the test
// fails; on the fixed BE it stays high. An IVF+FLAT table loaded from the same
// data is used as a positive control (FLAT has no codebook, so it is unaffected
// and must reach near-exact recall) -- this proves the harness can achieve high
// recall, so a low PQ recall is specifically the train-reentry bug.
//
// nonConcurrent: it temporarily changes a global BE config.
suite("ann_ivf_pq_train_once_recall", "nonConcurrent") {
    def dim       = 32
    def nRows     = 20000
    def chunkSize = 2000      // 20000 / 2000 = 10 chunks per segment -> bug triggers hard
    def nlist     = 64
    def topk      = 10
    def nQueries  = 30
    def rnd       = new Random(42)   // fixed seed -> reproducible

    // -- generate i.i.d. gaussian base vectors as a stream-load CSV (id|[v0,...]) --
    // The vector itself contains commas, so use '|' as the column separator.
    def sb = new StringBuilder()
    for (int i = 0; i < nRows; i++) {
        sb.append(i).append('|').append('[')
        for (int d = 0; d < dim; d++) {
            float v = (float) rnd.nextGaussian()
            if (d > 0) sb.append(',')
            sb.append(String.format(Locale.US, '%.6f', v))   // Locale.US: never a comma decimal
        }
        sb.append(']').append('\n')
    }
    def csv = sb.toString()

    // -- query vectors (independent random) --
    def queries = new float[nQueries][dim]
    for (int q = 0; q < nQueries; q++) {
        for (int d = 0; d < dim; d++) {
            queries[q][d] = (float) rnd.nextGaussian()
        }
    }

    def vecLiteral = { float[] v ->
        def s = new StringBuilder('[')
        for (int d = 0; d < v.length; d++) {
            if (d > 0) s.append(',')
            s.append(String.format(Locale.US, '%.6f', v[d]))
        }
        s.append(']')
        return s.toString()
    }

    def idsOf = { String q ->
        def rows = sql q
        return rows.collect { (it[0] as long) } as Set
    }

    // recall@topk averaged over all queries: approx (uses index) vs exact (brute force)
    def measureRecall = { String table ->
        double total = 0.0d
        for (int q = 0; q < nQueries; q++) {
            def lit    = vecLiteral(queries[q])
            def approx = idsOf("select id from ${table} order by l2_distance_approximate(vec, ${lit}) limit ${topk}".toString())
            def exact  = idsOf("select id from ${table} order by l2_distance(vec, ${lit}) limit ${topk}".toString())
            total += (approx.intersect(exact).size() / (double) topk)
        }
        return total / nQueries
    }

    def loadCsv = { String table ->
        streamLoad {
            table "${table}"
            set 'column_separator', '|'
            set 'columns', 'id, vec'
            inputStream new ByteArrayInputStream(csv.getBytes("UTF-8"))
            time 120000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(nRows, json.NumberLoadedRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    sql "set enable_common_expr_pushdown = true"
    sql "set enable_ann_index_result_cache = false"   // avoid cache masking real index behavior
    // Scan all lists so IVF coarse-quantization adds no approximation: this isolates
    // PQ-codebook correctness, which is what the bug breaks.
    sql "set ivf_nprobe = ${nlist}"

    setBeConfigTemporary([ann_index_build_chunk_size: chunkSize]) {
        // ================= IVF + PQ : the path the bug corrupts =================
        sql "drop table if exists ann_pq_train_once"
        sql """
        create table ann_pq_train_once (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec) using ann properties (
                'index_type' = 'ivf',
                'metric_type'= 'l2_distance',
                'dim'        = '${dim}',
                'nlist'      = '${nlist}',
                'quantizer'  = 'pq',
                'pq_m'       = '16',
                'pq_nbits'   = '8')
        ) engine=olap
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties ('replication_num' = '1');
        """
        loadCsv("ann_pq_train_once")

        // Guard: the approximate query MUST be pushed into the ANN index. Otherwise
        // it degenerates to exact distances, recall would be a trivial 1.0, and the
        // test would silently stop guarding the bug.
        explain {
            sql "select id from ann_pq_train_once order by l2_distance_approximate(vec, ${vecLiteral(queries[0])}) limit ${topk}".toString()
            contains "ANN SORT INFO"
        }

        double pqRecall = measureRecall("ann_pq_train_once")
        logger.info("[#63913] IVF+PQ multi-chunk recall@${topk} = ${pqRecall} (chunks per segment = ${nRows / chunkSize})")
        // Fixed build: typically ~0.8-0.95. Buggy build (per-chunk retrain): ~0.1.
        // Threshold 0.5 sits in the wide gap between them.
        assertTrue(pqRecall >= 0.5d,
            ("IVF+PQ recall@${topk} = ${pqRecall} is too low. The PQ codebook was likely " +
             "re-trained on every chunk so earlier chunks decode against the wrong codebook " +
             "(regression of #63913 'train ANN index once'). Expected >= 0.5.").toString())

        // ============ IVF + FLAT : positive control, must be unaffected ============
        sql "drop table if exists ann_flat_control"
        sql """
        create table ann_flat_control (
            id int not null,
            vec array<float> not null,
            index ann_idx (vec) using ann properties (
                'index_type' = 'ivf',
                'metric_type'= 'l2_distance',
                'dim'        = '${dim}',
                'nlist'      = '${nlist}',
                'quantizer'  = 'flat')
        ) engine=olap
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties ('replication_num' = '1');
        """
        loadCsv("ann_flat_control")

        double flatRecall = measureRecall("ann_flat_control")
        logger.info("[#63913] IVF+FLAT control recall@${topk} = ${flatRecall}")
        // FLAT has no codebook; with nprobe == nlist it is exact. If this is low, the
        // problem is the environment/harness, not the bug under test.
        assertTrue(flatRecall >= 0.95d,
            ("IVF+FLAT control recall@${topk} = ${flatRecall} is unexpectedly low; this points " +
             "to an environment/harness issue rather than the train-once bug.").toString())
    }

    sql "drop table if exists ann_pq_train_once"
    sql "drop table if exists ann_flat_control"
}
