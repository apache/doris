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

package org.apache.doris.filesystem.minio;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class MinioFileSystemPropertiesTest {

    @Test
    void bind_appliesLegacyMinioDefaults() {
        MinioFileSystemProperties properties = MinioFileSystemProperties.of(Map.of(
                "minio.endpoint", "http://127.0.0.1:9000",
                "minio.access_key", "ak",
                "minio.secret_key", "sk"));

        Assertions.assertEquals("http://127.0.0.1:9000", properties.getEndpoint());
        Assertions.assertEquals("us-east-1", properties.getRegion());
        Assertions.assertEquals("false", properties.getUsePathStyle());
        Assertions.assertEquals("MINIO", properties.providerName());
    }

    @Test
    void bind_prefersMinioAliasOverS3AndAwsAliases() {
        MinioFileSystemProperties properties = MinioFileSystemProperties.of(Map.of(
                "minio.endpoint", "http://minio.local:9000",
                "s3.endpoint", "https://s3.example.com",
                "minio.access_key", "minio-ak",
                "AWS_ACCESS_KEY", "aws-ak",
                "minio.secret_key", "minio-sk",
                "AWS_SECRET_KEY", "aws-sk"));

        Assertions.assertEquals("http://minio.local:9000", properties.getEndpoint());
        Assertions.assertEquals("minio-ak", properties.getAccessKey());
        Assertions.assertEquals("minio-sk", properties.getSecretKey());
    }

    @Test
    void bind_acceptsLegacyConverterShapedMap() {
        MinioFileSystemProperties properties = MinioFileSystemProperties.of(Map.of(
                "_STORAGE_TYPE_", "MINIO",
                "AWS_ENDPOINT", "http://127.0.0.1:9000",
                "AWS_REGION", "us-east-1",
                "AWS_ACCESS_KEY", "ak",
                "AWS_SECRET_KEY", "sk",
                "use_path_style", "true"));

        Assertions.assertEquals("http://127.0.0.1:9000", properties.getEndpoint());
        Assertions.assertEquals("true", properties.getUsePathStyle());
    }

    @Test
    void validate_requiresEndpoint() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> MinioFileSystemProperties.of(Map.of(
                        "minio.access_key", "ak",
                        "minio.secret_key", "sk")));
        Assertions.assertTrue(e.getMessage().contains("minio.endpoint is required"),
                e.getMessage());
    }

    @Test
    void validate_rejectsAwsOnlyCredentialOptions() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> MinioFileSystemProperties.of(Map.of(
                        "minio.endpoint", "http://127.0.0.1:9000",
                        "minio.access_key", "ak",
                        "minio.secret_key", "sk",
                        "AWS_CREDENTIALS_PROVIDER_TYPE", "INSTANCE_PROFILE")));
        Assertions.assertTrue(e.getMessage().contains("HMAC"), e.getMessage());
    }

    @Test
    void toS3CompatibleKv_forwardsCanonicalAwsKeys() {
        Map<String, String> kv = MinioFileSystemProperties.of(Map.of(
                "minio.endpoint", "http://127.0.0.1:9000",
                "minio.access_key", "ak",
                "minio.secret_key", "sk",
                "minio.session_token", "token")).toS3CompatibleKv();

        Assertions.assertEquals("http://127.0.0.1:9000", kv.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east-1", kv.get("AWS_REGION"));
        Assertions.assertEquals("ak", kv.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("token", kv.get("AWS_TOKEN"));
        Assertions.assertEquals("false", kv.get("use_path_style"));
        Assertions.assertFalse(kv.containsKey("AWS_CREDENTIALS_PROVIDER_TYPE"));
    }

    @Test
    void toS3CompatibleKv_fallsBackToAnonymousWithoutStaticCredentials() {
        Map<String, String> kv = MinioFileSystemProperties.of(Map.of(
                "minio.endpoint", "http://127.0.0.1:9000")).toS3CompatibleKv();

        Assertions.assertEquals("ANONYMOUS", kv.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
        Assertions.assertFalse(kv.containsKey("AWS_ACCESS_KEY"));
    }

    @Test
    void toString_masksSecretsButNotAccessKey() {
        String rendered = MinioFileSystemProperties.of(Map.of(
                "minio.endpoint", "http://127.0.0.1:9000",
                "minio.access_key", "minio-ak-plain",
                "minio.secret_key", "minio-sk-plain",
                "minio.session_token", "minio-token-plain")).toString();

        Assertions.assertFalse(rendered.contains("minio-sk-plain"), rendered);
        Assertions.assertFalse(rendered.contains("minio-token-plain"), rendered);
        Assertions.assertTrue(rendered.contains("accessKey=minio-ak-plain"), rendered);
    }
}
