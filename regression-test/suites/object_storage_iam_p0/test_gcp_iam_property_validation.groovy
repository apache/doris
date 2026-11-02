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

suite("test_gcp_iam_property_validation") {
    ["ADC", "", "unknown"].each { providerType ->
        test {
            sql """
                SELECT * FROM s3(
                    "uri" = "gs://validation-only-bucket/file.csv",
                    "provider" = "GCP",
                    "gs.credential_provider_type" = "${providerType}",
                    "format" = "csv"
                ) LIMIT 1
            """
            exception "gs.credential_provider_type"
        }
    }

    [" ", "invalid-account"].each { account ->
        test {
            sql """
                SELECT * FROM s3(
                    "uri" = "gs://validation-only-bucket/file.csv",
                    "provider" = "GCP",
                    "gs.impersonation_service_account" = "${account}",
                    "format" = "csv"
                ) LIMIT 1
            """
            exception "Invalid GCP service account email"
        }
    }

    [
        "gs.access_key": "gcp-access-key",
        "gs.secret_key": "gcp-secret-key",
        "gs.session_token": "gcp-session-token",
        "s3.access_key": "access-key",
        "s3.secret_key": "secret-key",
        "s3.session_token": "session-token",
        "s3.role_arn": "arn:aws:iam::123456789012:role/test-role",
        "s3.external_id": "external-id",
        "s3.credentials_provider_type": "INSTANCE_PROFILE"
    ].each { key, value ->
        test {
            sql """
                SELECT * FROM s3(
                    "uri" = "gs://validation-only-bucket/file.csv",
                    "provider" = "GCP",
                    "gs.credential_provider_type" = "DEFAULT",
                    "${key}" = "${value}",
                    "format" = "csv"
                ) LIMIT 1
            """
            exception "cannot be used together"
        }
    }

    test {
        sql """
            SELECT * FROM s3(
                "uri" = "gs://validation-only-bucket/file.csv",
                "provider" = "S3",
                "gs.credential_provider_type" = "DEFAULT",
                "format" = "csv"
            ) LIMIT 1
        """
        exception "requires provider=GCP"
    }

    [
        "gs.access_key": "access-key",
        "gs.secret_key": "secret-key",
        "gs.session_token": "session-token",
        "s3.role_arn": "arn:aws:iam::123456789012:role/test-role"
    ].each { key, value ->
        test {
            sql """
                SELECT * FROM s3(
                    "uri" = "gs://validation-only-bucket/file.csv",
                    "provider" = "GCP",
                    "gs.credential_provider_type" = "ANONYMOUS",
                    "${key}" = "${value}",
                    "format" = "csv"
                ) LIMIT 1
            """
            exception "cannot be used together"
        }
    }

    test {
        sql """
            SELECT * FROM s3(
                "uri" = "gs://validation-only-bucket/file.csv",
                "gs.credential_provider_type" = "ANONYMOUS",
                "gs.impersonation_service_account" = "target@project.iam.gserviceaccount.com",
                "format" = "csv"
            ) LIMIT 1
        """
        exception "ANONYMOUS cannot be used with gs.impersonation_service_account"
    }

    // Implicit DEFAULT must enforce the same conflicts as explicit DEFAULT, before any remote I/O.
    ["gs.session_token", "s3.role_arn", "s3.credentials_provider_type"].each { key ->
        test {
            sql """
                SELECT * FROM s3(
                    "uri" = "gs://validation-only-bucket/file.csv",
                    "provider" = "GCP",
                    "${key}" = "conflicting-credential",
                    "format" = "csv"
                ) LIMIT 1
            """
            exception "cannot be used together"
        }
    }

}
