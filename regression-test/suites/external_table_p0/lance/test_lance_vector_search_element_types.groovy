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

suite("test_lance_vector_search_element_types", "p0,external") {
    /*
     * vector_search() over every vector element type Doris encodes, on the pre-existing
     * all_types.lance fixture. The other vector suites are all Float32; this one covers what
     * varies per element type in Doris itself: the FE packs the query vector differently for
     * each of them (Float16 through Arrow's half-float conversion, int8/uint8 as single bytes)
     * and the BE decodes it symmetrically before handing it to lance-c. A mistake there returns
     * plausible but wrong rows rather than an error, and nothing covered it - all_types.lance
     * carries a fixed_size_list column of each element type, but no suite had ever searched one.
     *
     * No index is involved on purpose. Index behaviour is algorithm-level and already covered by
     * the Float32 suites, while encoding is per element type, so a flat search isolates exactly
     * what this suite is about - and keeps the fixture untouched.
     *
     * Fixture shape: 12 rows, 4-dimensional vectors, and only row 1 is non-null in every vector
     * column. So each search below returns exactly one row, which also pins that Lance skips null
     * rows rather than scoring them.
     */
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance vector element type test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_element_types"
    // `default` is the Lance root namespace and a reserved word, so it needs quoting here.
    String tableName = "${catalogName}.`default`.all_types"

    def search = { String column, String query, String metric ->
        """vector_search("table"="${tableName}", "column"="${column}", "query_vector"="${query}", "top_k"="3", "metric"="${metric}", "use_index"="false")"""
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

    // Row 1 holds [0.1, 0.2, 0.3, 0.4] in all three float columns. Querying with that exact
    // vector must return distance 0: for float32 and float64 the literals are the stored values,
    // and for float16 they are only equal after both sides round to half precision, which is
    // what makes this a real check of the FE's half-float encoding rather than of the literal.
    qt_float16_l2 """
        SELECT row_id, _distance
        FROM ${search("fixed_size_list_float16_col", "[0.1,0.2,0.3,0.4]", "l2")}
        ORDER BY _distance, row_id
    """
    qt_float32_l2 """
        SELECT row_id, _distance
        FROM ${search("fixed_size_list_float32_col", "[0.1,0.2,0.3,0.4]", "l2")}
        ORDER BY _distance, row_id
    """
    qt_float64_l2 """
        SELECT row_id, _distance
        FROM ${search("fixed_size_list_float64_col", "[0.1,0.2,0.3,0.4]", "l2")}
        ORDER BY _distance, row_id
    """

    // uint8 vectors are binary vectors to Lance: hamming is the only distance it accepts for
    // them, and row 1 holds exactly [0, 1, 2, 3].
    qt_uint8_hamming """
        SELECT row_id, _distance
        FROM ${search("fixed_size_list_uint8_col", "[0,1,2,3]", "hamming")}
        ORDER BY _distance, row_id
    """

    // Lance converts signed INT8 vectors to floats for L2. The conversion must preserve null
    // elements so the fixed-size-list batches with null rows still have the correct shape.
    qt_int8_l2 """
        SELECT row_id, _distance
        FROM ${search("fixed_size_list_int8_col", "[-1,0,1,2]", "l2")}
        ORDER BY _distance, row_id
    """

    // Every search above returns one row because the other 11 are null, so a regression that
    // scored null rows instead of skipping them would show up as extra rows in the goldens.
    def float32Rows = sql """
        SELECT row_id FROM ${search("fixed_size_list_float32_col", "[0.1,0.2,0.3,0.4]", "l2")}
    """
    assertEquals(1, float32Rows.size())

    // Asking Lance for a distance its element type does not support is a query error, not a
    // wrong answer and not a crash.
    test {
        sql """SELECT row_id FROM ${search("fixed_size_list_uint8_col", "[0,1,2,3]", "l2")}"""
        exception "does not support UInt8"
    }
}
