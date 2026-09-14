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

suite("test_lance_nested_null", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance S3 TVF test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String lanceTvf = """
        s3(
            "uri" = "s3://warehouse/lance/nested_null.lance",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "use_path_style" = "true",
            "format" = "lance"
        )
    """

    String originalScannerV2 = sql("SHOW VARIABLES LIKE 'enable_file_scanner_v2'")[0][1]
    try {
        sql "SET enable_file_scanner_v2 = true"
        def columns = sql "DESC FUNCTION ${lanceTvf}"
        assertEquals(7, columns.size())
        assertTrue(columns.every { !it[1].toString().contains("UNSUPPORTED") })
        assertEquals("array<null_type>", columns.find { it[0] == "null_list" }[1].toString())
        assertEquals("map<text,null_type>", columns.find { it[0] == "null_map" }[1].toString())

        qt_nested_null_values """
            SELECT id,
                   null_list IS NULL, COALESCE(size(null_list), -1), null_list[1] IS NULL,
                   null_large_list IS NULL, COALESCE(size(null_large_list), -1),
                   null_fixed_list IS NULL, COALESCE(size(null_fixed_list), -1),
                   struct_element(null_struct, 'empty') IS NULL,
                   struct_element(null_struct, 'value'),
                   nested_list IS NULL, COALESCE(size(nested_list), -1),
                   null_map IS NULL, COALESCE(map_size(null_map), -1), null_map['a'] IS NULL
            FROM ${lanceTvf} ORDER BY id
        """
        qt_nested_null_projection """
            SELECT id, null_list, struct_element(null_struct, 'empty') IS NULL
            FROM ${lanceTvf} WHERE id >= 1 ORDER BY id LIMIT 3
        """
    } finally {
        sql "SET enable_file_scanner_v2 = ${originalScannerV2}"
    }
}
