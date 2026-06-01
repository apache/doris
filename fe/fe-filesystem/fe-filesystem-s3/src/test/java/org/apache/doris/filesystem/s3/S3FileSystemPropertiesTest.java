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

package org.apache.doris.filesystem.s3;

import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.HadoopStorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

class S3FileSystemPropertiesTest {

    @Test
    void of_bindsAliasesAndExposesEffectiveViews() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://minio.local");
        raw.put("region", "us-west-2");
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");
        raw.put("s3.session-token", "token");
        raw.put("AWS_BUCKET", "bucket");
        raw.put("s3.root.path", "root");
        raw.put("s3.connection.maximum", "64");
        raw.put("use_path_style", "true");

        S3FileSystemProperties properties = S3FileSystemProperties.of(raw);

        Assertions.assertEquals("https://minio.local", properties.getEndpoint());
        Assertions.assertEquals("us-west-2", properties.getRegion());
        Assertions.assertEquals("ak", properties.getAccessKey());
        Assertions.assertEquals("sk", properties.getSecretKey());
        Assertions.assertEquals("token", properties.getSessionToken());
        Assertions.assertEquals("bucket", properties.getBucket());
        Assertions.assertEquals("root", properties.getRootPath());

        Assertions.assertEquals("https://minio.local", properties.matchedProperties().get("s3.endpoint"));
        Assertions.assertEquals("us-west-2", properties.matchedProperties().get("region"));
        Assertions.assertEquals("ak", properties.matchedProperties().get("s3.access_key"));

        Map<String, String> fsKv = properties.toFileSystemKv();
        Assertions.assertEquals("https://minio.local", fsKv.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-2", fsKv.get("AWS_REGION"));
        Assertions.assertEquals("ak", fsKv.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", fsKv.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("token", fsKv.get("AWS_TOKEN"));
        Assertions.assertEquals("bucket", fsKv.get("AWS_BUCKET"));
        Assertions.assertEquals("root", fsKv.get("AWS_ROOT_PATH"));
        Assertions.assertEquals("64", fsKv.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("true", fsKv.get("use_path_style"));
    }

    @Test
    void of_bindsLegacyS3AliasesToCanonicalFileSystemKv() {
        Map<String, String> raw = new HashMap<>();
        raw.put("access_key", "ak-bare");
        raw.put("secret_key", "sk-bare");
        raw.put("ENDPOINT", "https://endpoint.bare");
        raw.put("REGION", "ap-southeast-1");
        raw.put("session_token", "token");

        Map<String, String> fsKv = S3FileSystemProperties.of(raw).toFileSystemKv();

        Assertions.assertEquals("ak-bare", fsKv.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk-bare", fsKv.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("https://endpoint.bare", fsKv.get("AWS_ENDPOINT"));
        Assertions.assertEquals("ap-southeast-1", fsKv.get("AWS_REGION"));
        Assertions.assertEquals("token", fsKv.get("AWS_TOKEN"));
    }

    @Test
    void of_rejectsPartialStaticCredentialsWithParamRulesMessage() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://minio.local");
        raw.put("s3.access_key", "ak");

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, () -> S3FileSystemProperties.of(raw));

        Assertions.assertTrue(exception.getMessage().contains("Invalid S3 filesystem properties"));
        Assertions.assertTrue(exception.getMessage().contains("s3.access_key and s3.secret_key"));
    }

    @Test
    void of_rejectsExternalIdWithoutRoleArnWithParamRulesMessage() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://minio.local");
        raw.put("s3.external_id", "external");

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, () -> S3FileSystemProperties.of(raw));

        Assertions.assertTrue(exception.getMessage().contains("Invalid S3 filesystem properties"));
        Assertions.assertTrue(exception.getMessage().contains("s3.external_id must be used together with s3.role_arn"));
    }

    @Test
    void of_rejectsMissingLocationWithParamRulesMessage() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, () -> S3FileSystemProperties.of(raw));

        Assertions.assertTrue(exception.getMessage().contains("Invalid S3 filesystem properties"));
        Assertions.assertTrue(exception.getMessage().contains("Either s3.endpoint or s3.region must be set"));
    }

    @Test
    void of_acceptsEndpointOnlyS3CompatibleConfiguration() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://minio.local");
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");

        S3FileSystemProperties properties = S3FileSystemProperties.of(raw);

        Assertions.assertEquals("https://minio.local", properties.getEndpoint());
        Assertions.assertEquals("us-east-1", properties.getRegion());
        Assertions.assertEquals("https://minio.local", properties.toFileSystemKv().get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-east-1", properties.toFileSystemKv().get("AWS_REGION"));
    }

    @Test
    void of_acceptsRegionOnlyS3Configuration() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.region", "us-west-2");
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");

        S3FileSystemProperties properties = S3FileSystemProperties.of(raw);

        Assertions.assertEquals("us-west-2", properties.getRegion());
        Assertions.assertEquals("https://s3.us-west-2.amazonaws.com", properties.getEndpoint());
    }

    @Test
    void of_derivesRegionFromAwsEndpoint() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");

        S3FileSystemProperties properties = S3FileSystemProperties.of(raw);

        Assertions.assertEquals("us-west-2", properties.getRegion());
        Assertions.assertEquals("us-west-2", properties.toFileSystemKv().get("AWS_REGION"));
    }

    @Test
    void toBackendProperties_returnsLegacyAwsBackendMapForAdapters() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://minio.local");
        raw.put("s3.region", "us-west-2");
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");
        raw.put("s3.bucket", "bucket");
        raw.put("s3.root.path", "root");
        raw.put("use_path_style", "true");

        BackendStorageProperties backend = S3FileSystemProperties.of(raw)
                .toBackendProperties()
                .orElseThrow();

        Assertions.assertEquals(BackendStorageKind.S3_COMPATIBLE, backend.backendKind());
        Assertions.assertEquals("https://minio.local", backend.toMap().get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-2", backend.toMap().get("AWS_REGION"));
        Assertions.assertEquals("ak", backend.toMap().get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("sk", backend.toMap().get("AWS_SECRET_KEY"));
        Assertions.assertEquals("bucket", backend.toMap().get("AWS_BUCKET"));
        Assertions.assertEquals("root", backend.toMap().get("AWS_ROOT_PATH"));
        Assertions.assertEquals("true", backend.toMap().get("use_path_style"));
    }

    @Test
    void toHadoopProperties_returnsS3AConfigurationMap() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://minio.local");
        raw.put("s3.region", "us-west-2");
        raw.put("s3.access_key", "ak");
        raw.put("s3.secret_key", "sk");
        raw.put("s3.session_token", "token");
        raw.put("use_path_style", "true");

        HadoopStorageProperties hadoop = S3FileSystemProperties.of(raw)
                .toHadoopProperties()
                .orElseThrow();

        Map<String, String> hadoopMap = hadoop.toHadoopConfigurationMap();
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", hadoopMap.get("fs.s3a.impl"));
        Assertions.assertEquals("https://minio.local", hadoopMap.get("fs.s3a.endpoint"));
        Assertions.assertEquals("us-west-2", hadoopMap.get("fs.s3a.endpoint.region"));
        Assertions.assertEquals("ak", hadoopMap.get("fs.s3a.access.key"));
        Assertions.assertEquals("sk", hadoopMap.get("fs.s3a.secret.key"));
        Assertions.assertEquals("token", hadoopMap.get("fs.s3a.session.token"));
        Assertions.assertEquals("true", hadoopMap.get("fs.s3a.path.style.access"));
    }

    @Test
    void of_bindsAndNormalizesCredentialsProviderType() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        raw.put("s3.credentials_provider_type", "environment");

        S3FileSystemProperties properties = S3FileSystemProperties.of(raw);

        Assertions.assertEquals(S3CredentialsProviderType.ENV, properties.getCredentialsProviderType());
        Assertions.assertEquals("ENV", properties.toFileSystemKv().get("AWS_CREDENTIALS_PROVIDER_TYPE"));
        Assertions.assertEquals(EnvironmentVariableCredentialsProvider.class.getName(),
                properties.toHadoopConfigurationMap().get("fs.s3a.aws.credentials.provider"));
    }

    @Test
    void of_rejectsUnsupportedCredentialsProviderType() {
        Map<String, String> raw = new HashMap<>();
        raw.put("s3.endpoint", "https://s3.us-west-2.amazonaws.com");
        raw.put("s3.credentials_provider_type", "bad-provider");

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class, () -> S3FileSystemProperties.of(raw));

        Assertions.assertTrue(exception.getMessage().contains("Invalid S3 filesystem properties"));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported s3.credentials_provider_type"));
    }
}
