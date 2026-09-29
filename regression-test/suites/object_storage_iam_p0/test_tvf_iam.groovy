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

suite("test_tvf_iam") {
    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    config.authCases.each { authCase ->
        logger.info("run ${name} with ${authCase.name}")
        def result = sql """
            SELECT COUNT(*) FROM s3(
                "uri" = "${config.scheme}://${config.bucket}/${config.dataPath}",
                ${authCase.storageSqlProperties},
                "format" = "csv",
                "compress_type" = "gz",
                "column_separator" = "|",
                "use_path_style" = "false"
            )
        """
        assertEquals(1500, result[0][0], authCase.name)
    }

    if (config.provider == "GCP" && config.authCases.any { it.name == "gcp_default_omitted" }) {
        // The preceding read succeeded with omitted properties. The same private object must not
        // become readable with ANONYMOUS, even on a VM with a usable attached service account.
        test {
            sql """
                SELECT COUNT(*) FROM s3(
                    "uri" = "gs://${config.bucket}/${config.dataPath}",
                    "provider" = "GCP",
                    "gs.endpoint" = "${config.endpoint}",
                    "gs.credential_provider_type" = "ANONYMOUS",
                    "format" = "csv",
                    "compress_type" = "gz",
                    "column_separator" = "|"
                )
            """
            check { result, exception, startTime, endTime ->
                assertTrue(exception != null, "Anonymous access unexpectedly read the private GCS fixture")
                def message = exception.toString().toLowerCase()
                assertTrue(message.contains("403") || message.contains("access denied")
                        || message.contains("accessdenied") || message.contains("forbidden")
                        || message.contains("permission denied"), message)
            }
        }
    }
}
