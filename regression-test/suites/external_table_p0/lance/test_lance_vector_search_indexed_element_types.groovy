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

suite("test_lance_vector_search_indexed_element_types", "p0,external") {
    /*
     * vector_search() through a physical index over non-Float32 vector element types.
     *
     * test_lance_vector_search_element_types already covers how the FE encodes a query
     * vector of each element type, but it does so with flat search on purpose and stays
     * away from indexes. This suite is the other half: the same element types read through
     * an ANN index, which is a different BE path - the index stores its own copy of the
     * vectors, and the query has to survive encoding, the index format and the distance
     * kernel for that type rather than only the first of the three.
     *
     *   vs_ivf_flat_f64          Float64 + l2      collinear data, exact ladder
     *   vs_ivf_flat_f16_cosine   Float16 + cosine  directional data
     *   vs_ivf_flat_u8           UInt8 + hamming   thermometer data, 128 dimensions
     *
     * Every table is IVF_FLAT because that is the one algorithm whose full-partition probe
     * must reproduce the flat search exactly, so each table can carry that assertion rather
     * than only recording what it returned. For UInt8 it is a choice, not a constraint:
     * measured on the embedded Lance, the PQ and SQ builders reject UInt8, which leaves
     * IVF_FLAT and IVF_HNSW_FLAT - the graph variant is covered at plan level by
     * test_lance_vector_search_index_matrix.
     *
     * The element types choose their metric rather than the other way round. Float16 uses
     * cosine because a Float16 L2 index over the collinear ladder does not finish training -
     * that ladder's squared distances reach 16 * 1023^2, past Float16's 65504 ceiling - and
     * not because Float16 and L2 are incompatible; the same pairing builds fine on bounded
     * data. UInt8 uses hamming because Lance treats UInt8 vectors as binary vectors
     * and rejects every other distance for them. Int8 is absent for the reason recorded at
     * the end of test_lance_vector_search_element_types.
     *
     * Data shapes differ per table and each is documented where its queries are built. See
     * lance_build_preinstalled_catalog.py for how they are generated and verified.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance indexed element type test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_indexed_element_types"

    // Float64 keeps the collinear shape of the L2 suites: embedding[j] = (row_id - 1) + j,
    // so a query equal to row r's vector puts row n at exact squared L2 distance
    // 16 * (n - r)^2 - the same 0, 16, 64, 144, 256 ladder, now in double precision.
    String f64Head = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
    String f64Boundary = "[256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271]"

    // Float16 uses the directional data, whose rows sit on quasi-uniform directions with
    // varying norms. No closed-form ladder, and the values below are the generator's
    // rounded ones, which is what makes a Float16 round-trip of them meaningful.
    String f16Head = "[0.5212,-1.0089,1.0115,-0.8052,0.9277,-0.6251,0.7094,0.7868,-0.9736,0.6707,-0.4194,0.5045,0.5806,-0.3585,-0.7997,0.9973]"
    String f16Boundary = "[0.1835,0.4286,0.3099,0.7048,0.2626,0.1009,-0.072,-0.5285,-0.7581,-0.4567,0.6234,0.704,0.7249,-0.7313,0.2246,-0.7363]"

    // UInt8 uses a thermometer code: row r sets its first r bits, so Lance's hamming - which
    // counts differing BITS, not bytes - works out to exactly |a - b| between rows a and b.
    // That reproduces the symmetric ladder the collinear data gives under L2, which is why
    // top_k below is 9: it is the last cut that ends on a complete tie pair. One bit per row
    // means 1024 bits, hence 128 dimensions - the one table here whose width differs.
    def thermometer = { int row ->
        "[" + (0..<128).collect { j -> (1 << Math.max(0, Math.min(8, (row - 1) - j * 8))) - 1 }.join(",") + "]"
    }
    String u8Head = thermometer(1)
    // Row 505 rather than 256: the generator measured this index's partition edges at rows
    // 248-255, 502-509 and 760-767, and pins 505 - the middle of the run nearest the 512
    // split. A single-partition probe there misses part of row 505's true neighbourhood
    // while a flat scan returns all of it, which is exactly what the discriminator below is
    // looking for. The row moves whenever the fixture is rebuilt, because which side of a
    // partition edge discriminates is decided by kmeans; it is pinned on the binary profile
    // in lance_build_preinstalled_catalog.py and must be changed here at the same time.
    String u8Boundary = thermometer(505)

    // One complete record per table rather than several maps keyed by the same table names:
    // with parallel maps, a table added to one and forgotten in another interpolates a null
    // into the SQL below and fails as an opaque vector-parse error instead of as the missing
    // coverage it actually is. Every loop in this suite iterates this one map, so a table is
    // either fully described or not tested at all - and the guard below says which.
    //
    // compareByDistance picks the column the nprobes=1 discriminator may compare. The
    // collinear and thermometer profiles are symmetric - rows r-d and r+d tie - so only their
    // distance sequence is stable enough to compare. The directional profile was verified
    // tie-free, and there it is the other way round: the rows are exact while the distances
    // carry float noise between the indexed and flat paths. This restates
    // DataProfile.symmetric_ties in lance_build_preinstalled_catalog.py, which derives it from
    // whether the profile has a closed-form ladder; if a profile's ladder changes there, this
    // flag has to change with it.
    Map<String, Map<String, Object>> tables = [
            "vs_ivf_flat_f64"       : [metric: "l2", head: f64Head,
                                       boundary: f64Boundary, compareByDistance: true],
            "vs_ivf_flat_f16_cosine": [metric: "cosine", head: f16Head,
                                       boundary: f16Boundary, compareByDistance: false],
            "vs_ivf_flat_u8"        : [metric: "hamming", head: u8Head,
                                       boundary: u8Boundary, compareByDistance: true],
    ]
    tables.each { String table, Map<String, Object> spec ->
        for (String field : ["metric", "head", "boundary", "compareByDistance"]) {
            assertNotNull(spec[field],
                    "${table}: incomplete test record, ${field} is missing. Every table in "
                    + "this suite must carry all four, or the queries below silently "
                    + "interpolate a null.")
        }
    }

    def search = { String table, String query, String topK, String nprobes, String metric ->
        """vector_search("table"="${catalogName}.doris.${table}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "metric"="${metric}", "nprobes"="${nprobes}", "use_index"="true")"""
    }
    def flatSearch = { String table, String query, String topK, String metric ->
        """vector_search("table"="${catalogName}.doris.${table}", "column"="embedding", "query_vector"="${query}", "top_k"="${topK}", "metric"="${metric}", "use_index"="false")"""
    }

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
            "type" = "lance",
            "lance.catalog.type" = "filesystem",
            "warehouse" = "s3://warehouse/lance",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "use_path_style" = "true"
        )
    """
    sql """SET enable_file_scanner_v2 = true"""

    // The vector column's element type and width are part of what this suite is about, so
    // record the mapping Doris reports for each of them.
    order_qt_f64_desc """DESC `${catalogName}`.`doris`.`vs_ivf_flat_f64`"""
    order_qt_f16_desc """DESC `${catalogName}`.`doris`.`vs_ivf_flat_f16_cosine`"""
    order_qt_u8_desc """DESC `${catalogName}`.`doris`.`vs_ivf_flat_u8`"""

    // Each element type must reach an indexed plan, not quietly fall back to a flat scan.
    for (Map.Entry<String, Map<String, Object>> entry : tables.entrySet()) {
        String table = entry.getKey()
        Map<String, Object> spec = entry.getValue()
        String metric = spec.metric
        explain {
            sql("""SELECT row_id, _distance
                   FROM ${search(table, spec.head, "5", "4", metric)}
                   ORDER BY _distance, row_id""")
            contains "externalSearchType=VECTOR"
            contains "lanceMetric=${metric}"
            contains "lanceSearchFragments=2"
            contains "lanceSearchUnindexedFragments=0"
            contains "lanceSearchIndexSegments=1"
        }
    }

    // Silent-fallback discriminator: a real single-partition probe must miss part of the
    // boundary row's true neighbourhood, while a pipeline that ignored use_index or nprobes
    // would return exactly the flat rows.
    for (Map.Entry<String, Map<String, Object>> entry : tables.entrySet()) {
        String table = entry.getKey()
        Map<String, Object> spec = entry.getValue()
        String metric = spec.metric
        def singleProbe = sql """
            SELECT row_id, _distance
            FROM ${search(table, spec.boundary, "9", "1", metric)}
            ORDER BY _distance, row_id
        """
        def flat = sql """
            SELECT row_id, _distance
            FROM ${flatSearch(table, spec.boundary, "9", metric)}
            ORDER BY _distance, row_id
        """
        assertEquals(9, singleProbe.size())
        assertEquals(9, flat.size())
        int column = spec.compareByDistance ? 1 : 0
        assertFalse(singleProbe.collect { it[column] }.equals(flat.collect { it[column] }),
                "${table}: nprobes=1 produced the same result as the flat search, so the "
                + "single-partition restriction had no effect and the index was not used. "
                + "nprobes=1=" + singleProbe + " flat=" + flat)
    }

    // IVF_FLAT keeps the original vectors, so probing every partition is an exhaustive scan
    // by another name and must reproduce the flat search. Row ids only: for Float16 in
    // particular the two paths agree on the ordering but not on the last bits of a distance.
    for (Map.Entry<String, Map<String, Object>> entry : tables.entrySet()) {
        String table = entry.getKey()
        Map<String, Object> spec = entry.getValue()
        String metric = spec.metric
        def fullProbe = sql """
            SELECT row_id
            FROM ${search(table, spec.head, "10", "4", metric)}
            ORDER BY _distance, row_id
        """
        def flat = sql """
            SELECT row_id
            FROM ${flatSearch(table, spec.head, "10", metric)}
            ORDER BY _distance, row_id
        """
        assertEquals(10, fullProbe.size())
        assertEquals(flat.collect { it[0] }, fullProbe.collect { it[0] },
                "${table}: probing every partition did not reproduce the flat search, which "
                + "IVF_FLAT guarantees it must. indexed=" + fullProbe + " flat=" + flat)
    }

    // Float64 keeps the exact ladder, so these distances are hand-checkable: 0, 16, 64, 144,
    // 256 for rows 1 through 5.
    qt_f64_l2 """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_flat_f64", f64Head, "5", "4", "l2")}
        ORDER BY _distance, row_id
    """
    // The magnitude the golden above cannot record. A Float16 column queried with the
    // pre-rounding value does not return an exactly-zero self-distance, and that is worth
    // pinning: a backend that started widening Float16 to Float32 before comparing, or one
    // that lost the query vector's precision entirely, would move this off 1ulp. Asserted on
    // the absolute value and with a tolerance, because only the magnitude is reproducible.
    def f16Self = sql """
        SELECT _distance
        FROM ${search("vs_ivf_flat_f16_cosine", f16Head, "1", "4", "cosine")}
    """
    assertEquals(1, f16Self.size())
    double f16SelfDistance = Math.abs((f16Self[0][0] as Number).doubleValue())
    assertTrue(f16SelfDistance < 1e-5,
            "Float16 cosine self-distance is ${f16Self[0][0]}, expected something within one "
            + "float32 ulp of zero. A larger value means the query vector no longer reaches "
            + "the distance kernel at the precision this test assumes.")

    // Float16 records rows only, no `_distance`. Cosine divides by the vector norms, and the
    // query literal is the value before Float16 rounding while the column stores the value
    // after, so the two are not bit-identical and the cosine of a row against itself comes out
    // as 1 +/- 1ulp instead of exactly 1. The distance is then +/-2^-23, and which sign it
    // takes depends on the accumulation order of the build running the query: arm64 produced
    // +1.1920929E-7 here and x86_64 produced -1.1920929E-7 on CI, a relative difference of 2.
    // The magnitude is meaningful and is asserted below with a tolerance; the sign is not, and
    // a golden cannot record one without the other. Float64 and UInt8 keep their distances
    // because those are exact integers on any platform. Float32 cosine is unaffected - there
    // the query literal and the stored value are bit-identical, so its self-distance is a
    // clean 0.0 (see test_lance_vector_search_metrics).
    qt_f16_cosine """
        SELECT row_id, label
        FROM ${search("vs_ivf_flat_f16_cosine", f16Head, "5", "4", "cosine")}
        ORDER BY _distance, row_id
    """
    // Hamming over the thermometer code: rows 1 through 5 are at distances 0, 1, 2, 3, 4.
    qt_u8_hamming """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_flat_u8", u8Head, "5", "4", "hamming")}
        ORDER BY _distance, row_id
    """

    // Flat baselines for the same three queries. The loop above already asserts that a full
    // probe returns the same rows as a flat scan, but an assertion that two things are equal
    // says nothing when both move together: a change to the Float64 L2, Float16 cosine or
    // UInt8 hamming distance kernel in the BE shifts the indexed and flat paths identically,
    // keeps that assertion green, and shows up only as a bare diff in the goldens above with
    // no way to tell whether the index path or the kernel moved. Recording the flat answer
    // separates the two: both goldens moving means the kernel changed, only the indexed one
    // moving means the index path did. Float16 needs this most: it is the one table here
    // whose distances have no closed form to anchor against, which is also why its two
    // blocks record rows only.
    qt_f64_l2_flat """
        SELECT row_id, label, _distance
        FROM ${flatSearch("vs_ivf_flat_f64", f64Head, "5", "l2")}
        ORDER BY _distance, row_id
    """
    qt_f16_cosine_flat """
        SELECT row_id, label
        FROM ${flatSearch("vs_ivf_flat_f16_cosine", f16Head, "5", "cosine")}
        ORDER BY _distance, row_id
    """
    qt_u8_hamming_flat """
        SELECT row_id, label, _distance
        FROM ${flatSearch("vs_ivf_flat_u8", u8Head, "5", "hamming")}
        ORDER BY _distance, row_id
    """

    // Asking an index for a distance its element type does not support is a query error and
    // not a wrong answer. UInt8 is hamming-only in Lance, and the check happens on the
    // scanner, so it surfaces the same way with an index as it does on a flat search.
    test {
        sql """SELECT row_id FROM ${search("vs_ivf_flat_u8", u8Head, "5", "4", "l2")}"""
        exception "does not support UInt8"
    }
}
