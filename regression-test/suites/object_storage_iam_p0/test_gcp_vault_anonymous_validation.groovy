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

suite("test_gcp_vault_anonymous_validation") {
    if (!isCloudMode() || !enableStoragevault()) {
        logger.info("skip ${name} because cloud storage vaults are not enabled")
        return
    }

    ["gs.credential_provider_type", "s3.credentials_provider_type", "AWS_CREDENTIALS_PROVIDER_TYPE"].each { key ->
        def vaultName = "gcp_anonymous_${UUID.randomUUID().toString().replace('-', '')}"
        test {
            sql """
                CREATE STORAGE VAULT ${vaultName} PROPERTIES (
                    "type" = "S3",
                    "provider" = "GCP",
                    "s3.endpoint" = "storage.googleapis.com",
                    "s3.region" = "us-east1",
                    "s3.bucket" = "validation-only-bucket",
                    "s3.root.path" = "validation-only-root",
                    "s3_validity_check" = "false",
                    "${key}" = " anonymous "
                )
            """
            exception "Anonymous GCS authentication is not supported for storage vaults"
        }
    }
}
