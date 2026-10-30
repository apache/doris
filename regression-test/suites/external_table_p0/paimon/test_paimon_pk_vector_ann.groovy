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

// End-to-end regression for the Paimon PK-vector (ANN) search path: the
// PushDownVectorTopNIntoPaimonScan rewrite, the shared rust eligibility gate
// and the BE paimon-rust vector reader, compared against the exact brute-force
// answer the same SQL computes with the rust reader disabled.
//
// The fixture (database pk_vector_ann_db, tables ann_l2 / ann_ip) is created
// by the suite itself through the Spark ThriftServer's paimon catalog: 512
// deterministic 16-dim vectors derived from the row id (unique: the generator
// is mod 521, a prime above the row count), an ivf-rq PK-vector index with
// nlist = nprobe = 4 — exhaustive recall, so ANN must equal brute force by
// construction. Doris DDL cannot build this fixture: Doris has no VECTOR
// column type in either direction of the type mapping, and the ivf-rq index
// build needs the vindex factory jars that only the Spark server carries
// (paimon-vector + paimon-vector-index-java; see regression.md).
//
// Float precision note: the ANN distances are computed in f32 by paimon-rust
// while the brute-force leg computes in Doris doubles, so near-tied neighbors
// can swap positions between the legs. The assertions therefore compare the
// top-1 exactly (it is the probe row itself, distance 0), the first three in
// order, and the whole top-N as a set.
import org.apache.doris.regression.action.ProfileAction

suite("test_paimon_pk_vector_ann", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_paimon_pk_vector_ann"
    String database = "pk_vector_ann_db"
    int rows = 512
    String dimension = "16"

    // Session overrides are captured for the finally block the way the sibling
    // suites do.
    def originalRustReader = sql("select @@enable_paimon_rust_reader")[0][0]
    def originalV2 = sql("select @@enable_file_scanner_v2")[0][0]
    def originalEnableProfile = sql("select @@enable_profile")[0][0]

    try {
        sql "set enable_profile=true"
        sql """DROP CATALOG IF EXISTS ${catalogName}"""
        // The s3.region property is required: paimon-rust's S3 client rejects a
        // missing region, while the JNI reader falls back to the SDK default.
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'paimon',
                'paimon.catalog.type' = 'filesystem',
                'warehouse' = 's3://warehouse/wh',
                's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.region' = 'us-east-1',
                'use_path_style' = 'true'
            )"""

        // ------------------------------------------------------------------ fixture
        // Recreated on every run, so the suite is self-contained. The vector
        // column is declared ARRAY<FLOAT> + 'field.embedding.vector-dim': the 2.1
        // connector turns that into a paimon VECTOR<FLOAT, dim> column, which no
        // Doris DDL can express. The write commit builds the ivf-rq index inline
        // through the vindex factory jars loaded on the Spark server.
        String vectorOptions = '{"ivf-rq.nlist":"4","ivf-rq.dimension":"' + dimension + '",' +
                '"ivf-rq.rq.bits":"4","ivf-rq.train.sample-ratio":"1"}'
        String generatedRows = """
            SELECT id, CAST(id % 7 AS INT),
                   transform(sequence(0, 15),
                       i -> CAST(((id * 17 + i * 13) % 521 - 260) AS FLOAT) / 260.0)
            FROM (SELECT explode(sequence(1, ${rows})) AS id)"""
        [false, true].each { innerProduct ->
            def table = innerProduct ? "ann_ip" : "ann_l2"
            def metric = innerProduct ? "inner_product" : "l2"
            spark_paimon_multi """
                CREATE DATABASE IF NOT EXISTS paimon.${database};
                DROP TABLE IF EXISTS paimon.${database}.${table};
                CREATE TABLE paimon.${database}.${table} (
                    id BIGINT, tag INT, embedding ARRAY<FLOAT>
                ) USING paimon TBLPROPERTIES (
                    'primary-key' = 'id', 'bucket' = '2',
                    'merge-engine' = 'partial-update',
                    'deletion-vectors.enabled' = 'true',
                    'deletion-vectors.merge-on-read' = 'false',
                    'changelog-producer' = 'none',
                    'vector-field' = 'embedding',
                    'field.embedding.vector-dim' = '${dimension}',
                    'pk-vector.index.columns' = 'embedding',
                    'fields.embedding.pk-vector.index.type' = 'ivf-rq',
                    'fields.embedding.pk-vector.distance.metric' = '${metric}',
                    'fields.embedding.pk-vector.index.options' = '${vectorOptions}',
                    'ivf.nprobe' = '4'
                );
                INSERT INTO paimon.${database}.${table} ${generatedRows};
            """
        }
        sql """switch ${catalogName}"""
        sql "use ${database}"
        def fixtureCount = sql("select count(*) from ann_l2")
        assertEquals(rows.toLong(), fixtureCount[0][0], "fixture must have ${rows} rows")

        // The probe vector is the embedding of id=7 itself, taken from the data:
        // its top-1 neighbor must be id=7 with distance 0, and every leg below
        // uses the same in-data vector so ANN and brute force are comparable
        // without any golden values.
        def probeRow = sql("select embedding from ann_l2 where id = 7")
        String probe = probeRow[0][0].toString()

        def idsOf = { rowsResult -> rowsResult.collect { it[0] } }
        def compareTopN = { String table, boolean innerProduct, int n ->
            def fn = innerProduct ? "inner_product_approximate" : "l2_distance_approximate"
            def orderClause = innerProduct ? "desc" : ""
            sql "set enable_paimon_rust_reader=0"
            def brute = idsOf(sql("""select id from ${table}
                    order by ${fn}(embedding, ${probe}) ${orderClause} limit ${n}"""))
            sql "set enable_paimon_rust_reader=1"
            def ann = idsOf(sql("""select id from ${table}
                    order by ${fn}(embedding, ${probe}) ${orderClause} limit ${n}"""))
            assertEquals(brute.first(), ann.first(), "top-1 must match exactly")
            assertEquals(brute.take(3), ann.take(3),
                    "top-3 order must match: brute=${brute} ann=${ann}")
            assertEquals(brute.sort(), ann.sort(),
                    "top-${n} sets must match: brute=${brute} ann=${ann}")
        }

        // ------------------------------------------------------------------ l2
        def profileAction = new ProfileAction(context)
        compareTopN("ann_l2", false, 5)
        assertEquals(7L, idsOf(sql("""select id from ann_l2
                order by l2_distance_approximate(embedding, ${probe}) limit 1"""))[0],
                "the probe row itself must be top-1")

        // The ANN leg must actually have executed on the rust vector reader —
        // otherwise the equality above could pass on a silent brute-force scan.
        def annQuery = """select id from ann_l2
                order by l2_distance_approximate(embedding, ${probe}) limit 5"""
        sql annQuery
        def queryId = sql("select last_query_id()")[0][0]
        def profile = profileAction.getProfile(queryId.toString(), ["PaimonRustReader"])
        assertTrue(profile.contains("PaimonRustReader"),
                "ANN query must run through the paimon-rust vector reader")

        // Non-index-only projection: the raw vector column is still readable
        // alongside the distance; tag = (id - 1) % 7 by the fixture generator.
        def annTagged = sql("""select id, tag from ann_l2
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")
        annTagged.each { row ->
            assertEquals(row[0] % 7, row[1], "tag must be id % 7")
        }

        // Residual filter: the filter rides to BE's vector-search stage and
        // must agree with the filtered brute force (as a set; see the header).
        sql "set enable_paimon_rust_reader=0"
        def bruteFiltered = idsOf(sql("""select id from ann_l2 where tag < 3
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        sql "set enable_paimon_rust_reader=1"
        def annFiltered = idsOf(sql("""select id from ann_l2 where tag < 3
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        assertEquals(bruteFiltered, annFiltered, "filtered ANN must match filtered brute force")

        // Rewrite disabled by the scanner switch: still correct, just slower.
        sql "set enable_file_scanner_v2=0"
        def noRewrite = idsOf(sql("""select id from ann_l2
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        sql "set enable_file_scanner_v2=1"
        sql "set enable_paimon_rust_reader=0"
        def bruteTop5 = idsOf(sql("""select id from ann_l2
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        assertEquals(bruteTop5, noRewrite, "without FileScannerV2 the query must brute-force")

        // ------------------------------------------------------------------
        // Bounded-search soundness (review feedback on the rewrite):
        // A predicate that cannot ride into the BE vector search must not be
        // answered from a top-(LIMIT) fetch, so the rewrite declines for these
        // shapes and the query runs with the distance computed in Doris. Every
        // leg is a differential against the rust-disabled brute force.

        // A predicate on the distance itself: the synthetic distance column is
        // outside the paimon-rust predicate converter's field registry, and a
        // residual applied after the bounded fetch would return the wrong rows
        // (with candidate distances 0/2/3, `dis > 1 LIMIT 1` must return the
        // distance-2 row, not an empty result).
        def distanceLowerBound = {
            sql "set enable_paimon_rust_reader=0"
            def brute = idsOf(sql("""select id from ann_l2
                    where l2_distance_approximate(embedding, ${probe}) > 1
                    order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
            sql "set enable_paimon_rust_reader=1"
            def ann = idsOf(sql("""select id from ann_l2
                    where l2_distance_approximate(embedding, ${probe}) > 1
                    order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
            assertEquals(brute, ann, "a distance predicate must not be served by a bounded fetch")
        }
        distanceLowerBound()

        // An unconvertible data predicate (the ARRAY<FLOAT> vector column is not
        // a paimon-rust predicate type): same soundness rule.
        sql "set enable_paimon_rust_reader=0"
        def bruteArrayPred = idsOf(sql("""select id from ann_l2
                where embedding is not null
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        sql "set enable_paimon_rust_reader=1"
        def annArrayPred = idsOf(sql("""select id from ann_l2
                where embedding is not null
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        assertEquals(bruteArrayPred, annArrayPred,
                "an unconvertible predicate must not be served by a bounded fetch")

        // A pinned file-creation-time read state: the ordinary path drops manifest
        // entries older than the bound, but a PK-vector search plans its own splits
        // and never consults the bound -- so the rewrite declines and both legs
        // must agree on the file-visible rows. A bound of 1 keeps every file; a
        // far-future bound keeps none.
        sql "set enable_paimon_rust_reader=0"
        def bruteOld = idsOf(sql("""select id from ann_l2@options(
                'scan.file-creation-time-millis'='1')
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        def bruteNone = idsOf(sql("""select count(*) from ann_l2@options(
                'scan.file-creation-time-millis'='1791821967062000')"""))
        sql "set enable_paimon_rust_reader=1"
        def annOld = idsOf(sql("""select id from ann_l2@options(
                'scan.file-creation-time-millis'='1')
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        def annNone = idsOf(sql("""select count(*) from ann_l2@options(
                'scan.file-creation-time-millis'='1791821967062000')"""))
        assertEquals(bruteOld, annOld, "a file-creation-time bound must be honored on the vector path")
        assertEquals(bruteNone, annNone, "a far-future file bound must exclude all files on both paths")

        // -------------------------------------------------------- more fixtures
        // Two additional shapes the bounded rewrite must handle: a PK-vector table
        // whose merge engine permits the index WITHOUT deletion vectors (first-row,
        // allowed since paimon 2.1), and a partitioned PK-vector table (whose vector
        // splits must carry the derived partition values or every scan fails with
        // "Missing partition values").
        spark_paimon_multi """
            CREATE TABLE IF NOT EXISTS paimon.${database}.ann_l2_nodv (
                id BIGINT, tag INT, embedding ARRAY<FLOAT>
            ) USING paimon TBLPROPERTIES (
                'primary-key' = 'id', 'bucket' = '2',
                'merge-engine' = 'first-row',
                'changelog-producer' = 'none',
                'vector-field' = 'embedding',
                'field.embedding.vector-dim' = '${dimension}',
                'pk-vector.index.columns' = 'embedding',
                'fields.embedding.pk-vector.index.type' = 'ivf-rq',
                'fields.embedding.pk-vector.distance.metric' = 'l2',
                'fields.embedding.pk-vector.index.options' = '${vectorOptions}',
                'ivf.nprobe' = '4'
            );
            INSERT INTO paimon.${database}.ann_l2_nodv ${generatedRows};
        """
        spark_paimon_multi """
            CREATE DATABASE IF NOT EXISTS paimon.${database};
            DROP TABLE IF EXISTS paimon.${database}.ann_l2_part;
            CREATE TABLE paimon.${database}.ann_l2_part (
                id BIGINT, dt STRING, tag INT, embedding ARRAY<FLOAT>
            ) USING paimon PARTITIONED BY (dt) TBLPROPERTIES (
                'primary-key' = 'id', 'bucket' = '2',
                'merge-engine' = 'partial-update',
                'deletion-vectors.enabled' = 'true',
                'deletion-vectors.merge-on-read' = 'false',
                'changelog-producer' = 'none',
                'vector-field' = 'embedding',
                'field.embedding.vector-dim' = '${dimension}',
                'pk-vector.index.columns' = 'embedding',
                'fields.embedding.pk-vector.index.type' = 'ivf-rq',
                'fields.embedding.pk-vector.distance.metric' = 'l2',
                'fields.embedding.pk-vector.index.options' = '${vectorOptions}',
                'ivf.nprobe' = '4'
            );
            INSERT INTO paimon.${database}.ann_l2_part
                SELECT id, IF(id % 2 = 0, '2026-01-01', '2026-01-02'), CAST(id % 7 AS INT),
                       transform(sequence(0, 15),
                           i -> CAST(((id * 17 + i * 13) % 521 - 260) AS FLOAT) / 260.0)
                FROM (SELECT explode(sequence(1, ${rows})) AS id);
        """
        assertEquals(rows.toLong(), sql("select count(*) from ann_l2_nodv")[0][0])
        assertEquals(rows.toLong(), sql("select count(*) from ann_l2_part")[0][0])

        // ------------------------------------------------ no-deletion-vector shape
        // Unfiltered: paimon-rust's vector search has no data predicate, so the
        // rewrite stays sound on the no-DV table.
        sql "set enable_paimon_rust_reader=0"
        def bruteNoDv = idsOf(sql("""select id from ann_l2_nodv
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        // Filtered: the rust PkVectorSearchParams rejects every data predicate
        // unless deletion vectors are enabled without merge-on-read, so the rewrite
        // must decline here and both legs agree through the ordinary path.
        def bruteNoDvFilter = idsOf(sql("""select id from ann_l2_nodv where tag < 3
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        sql "set enable_paimon_rust_reader=1"
        def annNoDv = idsOf(sql("""select id from ann_l2_nodv
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        def annNoDvFilter = idsOf(sql("""select id from ann_l2_nodv where tag < 3
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        assertEquals(bruteNoDv, annNoDv, "unfiltered ANN must match brute force on the no-DV table")
        assertEquals(bruteNoDvFilter, annNoDvFilter,
                "a data predicate on a no-deletion-vector table must not run as a bounded ANN")

        // ------------------------------------------------------------ partitioned
        // The vector splits of a partitioned table must carry the derived partition
        // values (setPaimonParams throws "Missing partition values" otherwise), and
        // the ANN result must still match the ordinary brute-force answer.
        sql "set enable_paimon_rust_reader=0"
        def brutePart = idsOf(sql("""select id from ann_l2_part
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        sql "set enable_paimon_rust_reader=1"
        def annPart = idsOf(sql("""select id from ann_l2_part
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        assertEquals(brutePart, annPart, "partitioned ANN must match brute force")

        // ---------------------------------------------- NULL query-vector element
        // A NULL element inside the query vector must not become a zero coordinate:
        // the ordinary function rejects it, and the rewrite must decline so both
        // legs raise the same validation error.
        String nullVector = "array(cast(null as float)" +
                (1..(dimension.toInteger() - 1)).collect { ", 0.0" }.join() + ")"
        def queryWithNulls = """select id from ann_l2
                order by l2_distance_approximate(embedding, ${nullVector}) limit 5"""
        def expectValidationError = { String q ->
            try {
                sql q
                return null
            } catch (Throwable t) {
                return t.getMessage()
            }
        }
        sql "set enable_paimon_rust_reader=0"
        def bruteError = expectValidationError(queryWithNulls)
        sql "set enable_paimon_rust_reader=1"
        def annError = expectValidationError(queryWithNulls)
        assertNotNull("the ordinary path must reject a NULL query vector", bruteError)
        assertEquals(bruteError, annError,
                "the ANN path must reject a NULL query vector the same way, not search a zero vector")


        // ------------------------------------------- non-movable project expression
        // assert_true in the projection must keep evaluating below the TopN: the
        // ordinary path raises for the rejected far rows (the fixture has id%7=0
        // tags), while a bounded fetch that truncates before the Project would
        // silently succeed. The rewrite declines, so both legs raise the same error.
        def queryWithAssertTrue = """select id, assert_true(tag > 0, 'bad tag') from ann_l2
                order by l2_distance_approximate(embedding, ${probe}) limit 5"""
        sql "set enable_paimon_rust_reader=0"
        def bruteAssertError = expectValidationError(queryWithAssertTrue)
        sql "set enable_paimon_rust_reader=1"
        def annAssertError = expectValidationError(queryWithAssertTrue)
        assertNotNull("the ordinary path must reject a failing assert_true", bruteAssertError)
        assertEquals(bruteAssertError, annAssertError,
                "a non-movable projection must not be truncated by a bounded fetch")

        // ------------------------------------------- IN predicate with a NULL option
        // tag IN (0, 1, NULL) passes the shape whitelist (every option is a
        // literal) but the BE converter rejects the null option and with it the
        // whole predicate, so the rewrite must decline: the IN stays in the
        // residual and both legs must agree.
        sql "set enable_paimon_rust_reader=0"
        def bruteInNull = idsOf(sql("""select id from ann_l2 where tag in (0, 1, null)
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        sql "set enable_paimon_rust_reader=1"
        def annInNull = idsOf(sql("""select id from ann_l2 where tag in (0, 1, null)
                order by l2_distance_approximate(embedding, ${probe}) limit 5""")).sort()
        assertEquals(bruteInNull, annInNull, "an IN with a NULL option must not run as a bounded ANN")

        // -------------------------------------------------------- inner product
        compareTopN("ann_ip", true, 5)
    } finally {
        sql "set enable_paimon_rust_reader=${originalRustReader}"
        sql "set enable_file_scanner_v2=${originalV2}"
        sql "set enable_profile=${originalEnableProfile}"
        try {
            sql """DROP CATALOG IF EXISTS ${catalogName}"""
        } catch (Throwable t) {
            logger.info("drop catalog failed: " + t)
        }
    }
}
