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

package org.apache.doris.filesystem.gcs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class GcsFileSystemPropertiesTest {

    @Test
    void bind_appliesLegacyGcsDefaults() {
        GcsFileSystemProperties properties = GcsFileSystemProperties.of(Map.of(
                "gs.access_key", "ak",
                "gs.secret_key", "sk"));

        Assertions.assertEquals("https://storage.googleapis.com", properties.getEndpoint());
        Assertions.assertEquals("us-east1", properties.getRegion());
        Assertions.assertEquals("ak", properties.getAccessKey());
        Assertions.assertEquals("sk", properties.getSecretKey());
        Assertions.assertEquals("false", properties.getUsePathStyle());
        Assertions.assertEquals("100", properties.getMaxConnections());
        Assertions.assertEquals("10000", properties.getRequestTimeoutMs());
        Assertions.assertEquals("10000", properties.getConnectionTimeoutMs());
        Assertions.assertEquals("GCS", properties.providerName());
    }

    @Test
    void bind_prefersGsAliasOverS3AndAwsAliases() {
        GcsFileSystemProperties properties = GcsFileSystemProperties.of(Map.of(
                "gs.endpoint", "https://gs.example.com",
                "s3.endpoint", "https://s3.example.com",
                "AWS_ENDPOINT", "https://aws.example.com",
                "gs.access_key", "gs-ak",
                "AWS_ACCESS_KEY", "aws-ak",
                "gs.secret_key", "gs-sk",
                "AWS_SECRET_KEY", "aws-sk"));

        Assertions.assertEquals("https://gs.example.com", properties.getEndpoint());
        Assertions.assertEquals("gs-ak", properties.getAccessKey());
        Assertions.assertEquals("gs-sk", properties.getSecretKey());
    }

    @Test
    void bind_acceptsLegacyConverterShapedMap() {
        GcsFileSystemProperties properties = GcsFileSystemProperties.of(Map.of(
                "_STORAGE_TYPE_", "GCS",
                "provider", "GCP",
                "AWS_ENDPOINT", "https://storage.googleapis.com",
                "AWS_REGION", "us-east1",
                "AWS_ACCESS_KEY", "ak",
                "AWS_SECRET_KEY", "sk",
                "AWS_MAX_CONNECTIONS", "100",
                "use_path_style", "false"));

        Assertions.assertEquals("https://storage.googleapis.com", properties.getEndpoint());
        Assertions.assertEquals("ak", properties.getAccessKey());
    }

    @Test
    void toS3CompatibleKv_delegatesHmacToCanonicalAwsKeys() {
        Map<String, String> kv = GcsFileSystemProperties.of(Map.of(
                "gs.access_key", "ak",
                "gs.secret_key", "sk")).toS3CompatibleKv();

        Assertions.assertEquals("https://storage.googleapis.com", kv.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east1", kv.get("AWS_REGION"));
        Assertions.assertEquals("ak", kv.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", kv.get("AWS_SECRET_KEY"));
        Assertions.assertFalse(kv.containsKey("AWS_CREDENTIALS_PROVIDER_TYPE"));
    }

    @Test
    void toS3CompatibleKv_fallsBackToAnonymousWithoutStaticCredentials() {
        Map<String, String> kv = GcsFileSystemProperties.of(Map.of(
                "gs.endpoint", "https://storage.googleapis.com")).toS3CompatibleKv();

        Assertions.assertEquals("ANONYMOUS", kv.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
    }

    @Test
    void toMap_tagsGcpProviderForBackendParity() {
        Map<String, String> kv = GcsFileSystemProperties.of(Map.of(
                "gs.access_key", "ak",
                "gs.secret_key", "sk")).toMap();

        Assertions.assertEquals("GCP", kv.get("provider"));
    }

    @Test
    void toHadoopConfigurationMap_anonymousWhenNoStaticCredentials() {
        Map<String, String> cfg = GcsFileSystemProperties.of(Map.of(
                "gs.endpoint", "https://storage.googleapis.com")).toHadoopConfigurationMap();

        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", cfg.get("fs.gs.impl"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
                cfg.get("fs.s3a.aws.credentials.provider"));
    }

    @Test
    void toHadoopConfigurationMap_simpleProviderWithStaticCredentials() {
        Map<String, String> cfg = GcsFileSystemProperties.of(Map.of(
                "gs.access_key", "ak",
                "gs.secret_key", "sk")).toHadoopConfigurationMap();

        Assertions.assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                cfg.get("fs.s3a.aws.credentials.provider"));
        Assertions.assertEquals("ak", cfg.get("fs.s3a.access.key"));
        Assertions.assertEquals("sk", cfg.get("fs.s3a.secret.key"));
    }

    @Test
    void of_rejectsInvalidUsePathStyle() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> GcsFileSystemProperties.of(Map.of(
                        "gs.access_key", "ak",
                        "gs.secret_key", "sk",
                        "gs.use_path_style", "maybe")));
        Assertions.assertTrue(e.getMessage().contains("use_path_style"), e.getMessage());
    }

    @Test
    void of_rejectsSessionTokenWithoutStaticCredentials() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> GcsFileSystemProperties.of(Map.of("gs.session_token", "token")));
        Assertions.assertTrue(e.getMessage().contains("gs.session_token"), e.getMessage());
    }

    @Test
    void validate_rejectsAccessKeyWithoutSecretKey() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> GcsFileSystemProperties.of(Map.of("gs.access_key", "ak")));
        Assertions.assertTrue(e.getMessage().contains("gs.access_key and gs.secret_key"),
                e.getMessage());
    }

    @Test
    void validate_rejectsAwsOnlyCredentialOptions() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> GcsFileSystemProperties.of(Map.of(
                        "gs.access_key", "ak",
                        "gs.secret_key", "sk",
                        "sts.role_arn", "arn:aws:iam::1:role/x")));
        Assertions.assertTrue(e.getMessage().contains("HMAC"), e.getMessage());
    }

    @Test
    void toString_masksSecretsButNotAccessKey() {
        String rendered = GcsFileSystemProperties.of(Map.of(
                "gs.access_key", "gcs-ak-plain",
                "gs.secret_key", "gcs-sk-plain",
                "gs.session_token", "gcs-token-plain")).toString();

        Assertions.assertFalse(rendered.contains("gcs-sk-plain"), rendered);
        Assertions.assertFalse(rendered.contains("gcs-token-plain"), rendered);
        Assertions.assertTrue(rendered.contains("accessKey=gcs-ak-plain"), rendered);
    }

    @Test
    void bind_readsBucketFromS3AliasThenAwsAlias() {
        // Legacy AbstractS3CompatibleProperties.getBucket() reads s3.bucket before AWS_BUCKET for
        // every S3-compatible dialect, so both spellings must bind and s3.bucket must win.
        Assertions.assertEquals("from-s3-alias", GcsFileSystemProperties.of(Map.of(
                "s3.bucket", "from-s3-alias")).getBucket());
        Assertions.assertEquals("from-aws-alias", GcsFileSystemProperties.of(Map.of(
                "AWS_BUCKET", "from-aws-alias")).getBucket());
        Assertions.assertEquals("from-s3-alias", GcsFileSystemProperties.of(Map.of(
                "s3.bucket", "from-s3-alias",
                "AWS_BUCKET", "from-aws-alias")).getBucket());
    }

    @Test
    void region_isFixedAndNotUserConfigurable() {
        // Legacy GCSProperties declares region as a plain constant with no @ConnectorProperty:
        // GCS resolves the region by bucket internally, the value exists only for S3 signing.
        Assertions.assertEquals("us-east1", GcsFileSystemProperties.of(Map.of(
                "gs.region", "europe-west1",
                "AWS_REGION", "europe-west1")).getRegion());
    }
}
