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

import org.apache.doris.regression.util.ObjectStorageIamTestUtils

suite("test_gcp_tvf_anonymous") {
    def uri = context.config.otherConfigs.get("objectStorageIamGcpAnonymousUri")?.toString()?.trim()
    if (!uri) {
        logger.info("skip ${name} because objectStorageIamGcpAnonymousUri is not configured")
        return
    }
    assertTrue(uri.startsWith("gs://"), "The anonymous fixture must be a gs:// URI")
    def expectedRows = context.config.otherConfigs.get("objectStorageIamGcpAnonymousExpectedRows")
            ?.toString()?.trim()
    assertTrue(expectedRows != null && expectedRows.isLong() && expectedRows.toLong() > 0,
            "Configure a positive objectStorageIamGcpAnonymousExpectedRows")

    // Each query exercises FE object discovery/schema inference and a BE scan of the public file.
    // Include the legacy S3-compatible spelling and provider-value case/whitespace normalization.
    [
        ["gs.credential_provider_type": "ANONYMOUS"],
        ["gs.credential_provider_type": " anonymous "],
        ["s3.credentials_provider_type": "ANONYMOUS"]
    ].each { authProperties ->
        def result = sql """
            SELECT COUNT(*) FROM s3(
                "uri" = "${uri}",
                "provider" = "GCP",
                ${ObjectStorageIamTestUtils.toSqlProperties(authProperties)},
                "format" = "csv",
                "compress_type" = "gz",
                "column_separator" = "|"
            )
        """
        assertEquals(expectedRows.toLong(), result[0][0], authProperties.toString())
    }
}
