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

suite("test_lance_local_tvf", "p0,external,external_docker") {
    List<List<Object>> backends = sql """SHOW BACKENDS"""
    assertFalse(backends.isEmpty())
    def backendId = backends[0][0]
    def backendHost = backends[0][1]
    def fixturePath = new File(
            context.config.dataPath,
            "../../docker/thirdparties/docker-compose/iceberg/scripts/preinstalled_data/lance/all_types.lance"
    ).canonicalPath
    def remotePath = "/tmp/doris_lance_local_tvf_all_types.lance"
    assertTrue(new File(fixturePath).isDirectory())

    sshExec("root", backendHost, "rm -rf ${remotePath}", false)
    scpFiles("root", backendHost, fixturePath, remotePath, false)

    def scannerV2Rows = sql """SHOW VARIABLES LIKE 'enable_file_scanner_v2'"""
    assertEquals(1, scannerV2Rows.size())
    String originalScannerV2 = scannerV2Rows[0][1].toString()
    try {
        sql """SET enable_file_scanner_v2 = true"""
        qt_blob_descriptor """
            SELECT blob_col.kind, blob_col.size
            FROM local(
                "file_path" = "${remotePath}",
                "backend_id" = "${backendId}",
                "format" = "lance"
            )
            WHERE row_id = 1
        """
    } finally {
        sql """SET enable_file_scanner_v2 = ${originalScannerV2}"""
        sshExec("root", backendHost, "rm -rf ${remotePath}", false)
    }
}
