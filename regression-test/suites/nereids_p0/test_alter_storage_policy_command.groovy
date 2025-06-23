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

suite("test_alter_storage_policy_command", "alter,storage_policy") {
    String policyName = "test_alter_storage_policy";
    String resourceName = "test_alter_storage_policy_resource";
    try {
        // Drop existing storage policy and resource if they exist before creating new ones
        try_sql("DROP STORAGE POLICY IF EXISTS ${policyName}")
        try_sql("DROP RESOURCE IF EXISTS ${resourceName}")
        // Create a new resource to be used in the storage policy
        sql """
            CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                "type"="s3",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "AWS_ROOT_PATH" = "regression/cooldown",
                "AWS_ACCESS_KEY" = "${getS3AK()}",
                "AWS_SECRET_KEY" = "${getS3SK()}",
                "AWS_MAX_CONNECTIONS" = "50",
                "AWS_REQUEST_TIMEOUT_MS" = "3000",
                "AWS_CONNECTION_TIMEOUT_MS" = "1000",
                "AWS_BUCKET" = "${getS3BucketName()}",
                "s3_validity_check" = "false"
            );
        """

        // Create a new storage policy to test the SHOW STORAGE POLICY command
        sql """
            CREATE STORAGE POLICY IF NOT EXISTS ${policyName}
            PROPERTIES(
                "storage_resource" = "${resourceName}",
                "cooldown_ttl" = "300"
            )
        """

	// Alter the storage policy
        checkNereidsExecute """
            ALTER STORAGE POLICY ${policyName} 
            PROPERTIES ("cooldown_ttl"="7200")
        """

    } catch (Exception e) {
        // Log any exceptions that occur during testing
        log.error("Failed to execute SHOW STORAGE POLICY command", e)
        throw e
    } finally {
        // Clean up by dropping the storage policy and resource if they still exist
        try_sql("DROP STORAGE POLICY IF EXISTS ${policyName}")
        try_sql("DROP RESOURCE IF EXISTS ${resourceName}")
    }
}
