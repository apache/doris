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

package org.apache.doris.filesystem;

import java.util.Map;

/**
 * Base class for environment-dependent filesystem integration tests.
 *
 * <p>Subclasses must be annotated with {@code @Tag("environment")} and the specific
 * service tag (e.g., {@code @Tag("s3")}), plus an {@code @EnabledIfEnvironmentVariable}
 * guard to prevent execution when credentials are missing.
 *
 * <p>This class is in the API test module. Since Maven does not publish test-jars by default,
 * each consumer module that needs {@link #requireEnv(String)} should either depend on the
 * API test-jar or inline the helper. The static helpers below are kept minimal on purpose.
 */
public abstract class EnvTestBase {

    /**
     * Returns the value of the named environment variable, throwing if missing or empty.
     */
    protected static String requireEnv(String name) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return val;
    }

    protected static Map<String, String> s3Props() {
        return Map.of(
                "AWS_ENDPOINT", requireEnv("DORIS_FS_TEST_S3_ENDPOINT"),
                "AWS_REGION", requireEnv("DORIS_FS_TEST_S3_REGION"),
                "AWS_BUCKET", requireEnv("DORIS_FS_TEST_S3_BUCKET"),
                "AWS_ACCESS_KEY", requireEnv("DORIS_FS_TEST_S3_AK"),
                "AWS_SECRET_KEY", requireEnv("DORIS_FS_TEST_S3_SK")
        );
    }

    protected static Map<String, String> azureProps() {
        return Map.of(
                "AZURE_ACCOUNT_NAME", requireEnv("DORIS_FS_TEST_AZURE_ACCOUNT"),
                "AZURE_ACCOUNT_KEY", requireEnv("DORIS_FS_TEST_AZURE_KEY"),
                "AZURE_CONTAINER", requireEnv("DORIS_FS_TEST_AZURE_CONTAINER")
        );
    }

    protected static Map<String, String> hdfsProps() {
        return Map.of(
                "fs.defaultFS", "hdfs://" + requireEnv("DORIS_FS_TEST_HDFS_HOST")
                        + ":" + requireEnv("DORIS_FS_TEST_HDFS_PORT")
        );
    }

    protected static Map<String, String> cosProps() {
        return Map.of(
                "COS_ENDPOINT", requireEnv("DORIS_FS_TEST_COS_ENDPOINT"),
                "COS_REGION", requireEnv("DORIS_FS_TEST_COS_REGION"),
                "AWS_BUCKET", requireEnv("DORIS_FS_TEST_COS_BUCKET"),
                "COS_ACCESS_KEY", requireEnv("DORIS_FS_TEST_COS_AK"),
                "COS_SECRET_KEY", requireEnv("DORIS_FS_TEST_COS_SK")
        );
    }

    protected static Map<String, String> ossProps() {
        return Map.of(
                "AWS_ENDPOINT", requireEnv("DORIS_FS_TEST_OSS_ENDPOINT"),
                "AWS_REGION", requireEnv("DORIS_FS_TEST_OSS_REGION"),
                "AWS_BUCKET", requireEnv("DORIS_FS_TEST_OSS_BUCKET"),
                "AWS_ACCESS_KEY", requireEnv("DORIS_FS_TEST_OSS_AK"),
                "AWS_SECRET_KEY", requireEnv("DORIS_FS_TEST_OSS_SK")
        );
    }

    protected static Map<String, String> obsProps() {
        return Map.of(
                "AWS_ENDPOINT", requireEnv("DORIS_FS_TEST_OBS_ENDPOINT"),
                "AWS_REGION", requireEnv("DORIS_FS_TEST_OBS_REGION"),
                "AWS_BUCKET", requireEnv("DORIS_FS_TEST_OBS_BUCKET"),
                "AWS_ACCESS_KEY", requireEnv("DORIS_FS_TEST_OBS_AK"),
                "AWS_SECRET_KEY", requireEnv("DORIS_FS_TEST_OBS_SK")
        );
    }
}
