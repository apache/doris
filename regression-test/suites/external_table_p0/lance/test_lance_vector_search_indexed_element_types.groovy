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
     * Every table is IVF_FLAT, which is the only index type Lance accepts for UInt8 and is
     * also the one whose full-partition probe must reproduce the flat search exactly - so
     * each table can carry that assertion rather than only recording what it returned.
     *
     * The element types choose their metric rather than the other way round. Float16 uses
     * cosine because building a Float16 L2 index does not complete in the embedded Lance
     * version, and UInt8 uses hamming because Lance treats UInt8 vectors as binary vectors
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
    String f64Boundary = "[255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270]"

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
    // Row 513 rather than 256: the generator measured this index's partition edges at rows
    // 255-262, 513-520 and 767-774, and pins 513. A single-partition probe there returns
    // 508 as its ninth row where the flat search returns 517 - one genuinely missed
    // neighbour, which is exactly what the discriminator below is looking for.
    String u8Boundary = thermometer(513)

    Map<String, String> metricOf = [
            "vs_ivf_flat_f64"       : "l2",
            "vs_ivf_flat_f16_cosine": "cosine",
            "vs_ivf_flat_u8"        : "hamming",
    ]
    Map<String, String> headOf = [
            "vs_ivf_flat_f64"       : f64Head,
            "vs_ivf_flat_f16_cosine": f16Head,
            "vs_ivf_flat_u8"        : u8Head,
    ]
    Map<String, String> boundaryOf = [
            "vs_ivf_flat_f64"       : f64Boundary,
            "vs_ivf_flat_f16_cosine": f16Boundary,
            "vs_ivf_flat_u8"        : u8Boundary,
    ]
    // The collinear and thermometer profiles are symmetric - rows r-d and r+d tie - so only
    // their distance sequence is stable enough to compare. The directional profile was
    // verified tie-free, and there it is the other way round: the rows are exact while the
    // distances carry float noise between the indexed and flat paths.
    Set<String> compareByDistance = ["vs_ivf_flat_f64", "vs_ivf_flat_u8"] as Set

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
    for (Map.Entry<String, String> entry : metricOf.entrySet()) {
        String table = entry.getKey()
        String metric = entry.getValue()
        explain {
            sql("""SELECT row_id, _distance
                   FROM ${search(table, headOf[table], "5", "4", metric)}
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
    for (Map.Entry<String, String> entry : metricOf.entrySet()) {
        String table = entry.getKey()
        String metric = entry.getValue()
        def singleProbe = sql """
            SELECT row_id, _distance
            FROM ${search(table, boundaryOf[table], "9", "1", metric)}
            ORDER BY _distance, row_id
        """
        def flat = sql """
            SELECT row_id, _distance
            FROM ${flatSearch(table, boundaryOf[table], "9", metric)}
            ORDER BY _distance, row_id
        """
        assertEquals(9, singleProbe.size())
        assertEquals(9, flat.size())
        int column = compareByDistance.contains(table) ? 1 : 0
        assertFalse(singleProbe.collect { it[column] }.equals(flat.collect { it[column] }),
                "${table}: nprobes=1 produced the same result as the flat search, so the "
                + "single-partition restriction had no effect and the index was not used. "
                + "nprobes=1=" + singleProbe + " flat=" + flat)
    }

    // IVF_FLAT keeps the original vectors, so probing every partition is an exhaustive scan
    // by another name and must reproduce the flat search. Row ids only: for Float16 in
    // particular the two paths agree on the ordering but not on the last bits of a distance.
    for (Map.Entry<String, String> entry : metricOf.entrySet()) {
        String table = entry.getKey()
        String metric = entry.getValue()
        def fullProbe = sql """
            SELECT row_id
            FROM ${search(table, headOf[table], "10", "4", metric)}
            ORDER BY _distance, row_id
        """
        def flat = sql """
            SELECT row_id
            FROM ${flatSearch(table, headOf[table], "10", metric)}
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
    qt_f16_cosine """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_flat_f16_cosine", f16Head, "5", "4", "cosine")}
        ORDER BY _distance, row_id
    """
    // Hamming over the thermometer code: rows 1 through 5 are at distances 0, 1, 2, 3, 4.
    qt_u8_hamming """
        SELECT row_id, label, _distance
        FROM ${search("vs_ivf_flat_u8", u8Head, "5", "4", "hamming")}
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
