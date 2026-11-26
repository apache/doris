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

suite("test_lance_blob", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Lance Blob tests require the Iceberg MinIO fixture")
        return
    }
    String endpoint = "http://${context.config.otherConfigs.get('externalEnvIp')}:" +
            context.config.otherConfigs.get('iceberg_minio_port')
    sql "DROP CATALOG IF EXISTS test_lance_blob"
    sql """CREATE CATALOG test_lance_blob PROPERTIES (
        "type" = "lance", "lance.catalog.type" = "filesystem",
        "warehouse" = "s3://warehouse/lance/blob_v2",
        "s3.endpoint" = "${endpoint}", "s3.access_key" = "admin",
        "s3.secret_key" = "password", "s3.region" = "us-east-1",
        "use_path_style" = "true")"""

    // Check against ordinary binary columns before recording output, so -genOut cannot
    // bless an incorrect Blob payload. Each tag also produces a reviewable .out block.
    def checkBlob = { String tag, String actual, String expected ->
        check_sqls_result_equal(actual, expected)
        quickTest(tag, actual)
    }
    String managed = "test_lance_blob.`default`.managed"
    String external = "test_lance_blob.`default`.external"
    order_qt_schema "DESC ${managed}"
    // Ordinary binary reference columns contain the same bytes, but use the regular Lance
    // reader instead of Blob descriptors/handles. Compare through the framework's two-SQL
    // quick-test path before writing or checking the recorded output.
    checkBlob("managed_rows", "SELECT COUNT(*) FROM ${managed}", "SELECT 8")
    checkBlob("external_rows", "SELECT COUNT(*) FROM ${external}", "SELECT 5")
    // Materialize payloads after collect_list, retaining Blob IDs through partial states.
    // Disable the single-BE fused aggregate so this check exercises serialization too.
    String collected = """
        SELECT /*+ SET_VAR(agg_phase=2, enable_bucketed_hash_agg=false) */
               bucket, LENGTH(FL_GET_BINARY(item)), MD5(FL_GET_BINARY(item))
        FROM (SELECT bucket, COLLECT_LIST(payload) AS items FROM ${managed} GROUP BY bucket) t
        LATERAL VIEW EXPLODE(items) exploded AS item
        ORDER BY bucket, 2, 3
    """
    explain {
        sql collected
        contains "update serialize"
        contains "merge finalize"
    }
    checkBlob("collected", collected, """
        SELECT bucket, LENGTH(expected_payload), MD5(expected_payload)
        FROM ${managed} ORDER BY bucket, 2, 3
    """)

    checkBlob("managed", """
        SELECT id, payload IS NULL, LENGTH(FL_GET_BINARY(payload)), MD5(FL_GET_BINARY(payload)),
               LENGTH(FL_GET_BINARY(alternate)), MD5(FL_GET_BINARY(alternate))
        FROM ${managed} ORDER BY id
    """, """
        SELECT id, expected_payload IS NULL, LENGTH(expected_payload), MD5(expected_payload),
               LENGTH(expected_alternate), MD5(expected_alternate)
        FROM ${managed} ORDER BY id
    """)

    // Distinguish an empty payload from a SQL NULL; the pinned fixture writer cannot
    // preserve source Blob NULLs, whose descriptor shapes are covered by BE unit tests.
    checkBlob("nulls", """
        SELECT id, FL_GET_BINARY(IF(id % 2 = 0, NULL, payload)) IS NULL,
               LENGTH(FL_GET_BINARY(IF(id % 2 = 0, NULL, payload)))
        FROM ${managed} ORDER BY id
    """, """
        SELECT id, IF(id % 2 = 0, CAST(NULL AS VARBINARY), expected_payload) IS NULL,
               LENGTH(IF(id % 2 = 0, CAST(NULL AS VARBINARY), expected_payload))
        FROM ${managed} ORDER BY id
    """)
    checkBlob("reordered", """
        SELECT id, LENGTH(FL_GET_BINARY(payload)), MD5(FL_GET_BINARY(payload))
        FROM (SELECT * FROM ${managed} ORDER BY id DESC LIMIT 5) t ORDER BY id DESC
    """, """
        SELECT id, LENGTH(expected_payload), MD5(expected_payload)
        FROM (SELECT * FROM ${managed} ORDER BY id DESC LIMIT 5) t ORDER BY id DESC
    """)
    checkBlob("empty", """
        SELECT id, MD5(FL_GET_BINARY(payload)) FROM ${managed}
        WHERE id < 0 ORDER BY id
    """, """
        SELECT id, MD5(expected_payload) FROM ${managed} WHERE id < 0 ORDER BY id
    """)

    // Repeated rows and two source IDs from one dataset must keep each column's own payload.
    String duplicates = "(SELECT 0 AS copy_id UNION ALL SELECT 1 AS copy_id) copies"
    checkBlob("duplicates", """
        SELECT t.id, copies.copy_id, MD5(FL_GET_BINARY(t.payload)), MD5(FL_GET_BINARY(t.alternate))
        FROM ${managed} t CROSS JOIN ${duplicates} WHERE t.id IN (1, 3, 7)
        ORDER BY t.id, copies.copy_id
    """, """
        SELECT t.id, copies.copy_id, MD5(t.expected_payload), MD5(t.expected_alternate)
        FROM ${managed} t CROSS JOIN ${duplicates} WHERE t.id IN (1, 3, 7)
        ORDER BY t.id, copies.copy_id
    """)

    // Whole-object, sliced-object and repeated external references share the same remote base.
    checkBlob("external", """
        SELECT id, LENGTH(FL_GET_BINARY(payload)), MD5(FL_GET_BINARY(payload))
        FROM ${external} ORDER BY id
    """, """
        SELECT id, LENGTH(expected_payload), MD5(expected_payload) FROM ${external} ORDER BY id
    """)

    String tvf = """s3("uri" = "s3://warehouse/lance/blob_v2/managed.lance",
        "s3.endpoint" = "${endpoint}", "s3.access_key" = "admin", "s3.secret_key" = "password",
        "s3.region" = "us-east-1", "use_path_style" = "true", "format" = "lance")"""
    checkBlob("tvf", """
        SELECT id, LENGTH(FL_GET_BINARY(payload)), MD5(FL_GET_BINARY(alternate))
        FROM ${tvf} WHERE id IN (0, 2, 3, 7) ORDER BY id
    """, """
        SELECT id, LENGTH(expected_payload), MD5(expected_alternate)
        FROM ${managed} WHERE id IN (0, 2, 3, 7) ORDER BY id
    """)

    for (String name : ["missing_external", "local_external"]) {
        String relation = "test_lance_blob.`default`.${name}"
        // Scalar-only scans and good-row filtering must not inspect the bad Blob descriptor.
        checkBlob("${name}_scalar_rows", "SELECT COUNT(*) FROM ${relation}", "SELECT 2")
        checkBlob("${name}_good_row", """
            SELECT MD5(FL_GET_BINARY(payload)) FROM ${relation} WHERE id = 0
        """, "SELECT MD5('readable')")
    }
    // A remote missing object is detected on payload access, after descriptor construction.
    checkBlob("missing_descriptors", """SELECT COUNT(payload)
        FROM test_lance_blob.`default`.missing_external""", "SELECT 2")
    test {
        sql """SELECT FL_GET_BINARY(payload)
               FROM test_lance_blob.`default`.missing_external WHERE id = 1"""
        exception "missing.bin"
    }
    test {
        sql """SELECT payload
               FROM test_lance_blob.`default`.local_external WHERE id = 1"""
        exception "Local external Lance BLOB objects are not supported"
    }
}
