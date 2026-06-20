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

package org.apache.doris.filesystem.cos;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

class CosFileSystemPropertiesTest {

    @Test
    void bind_usesFeCoreCosAliasOrder() {
        CosFileSystemProperties properties = CosFileSystemProperties.of(Map.of(
                "cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com",
                "cos.access_key", "cos-ak",
                "AWS_ACCESS_KEY", "aws-ak",
                "cos.secret_key", "cos-sk",
                "AWS_SECRET_KEY", "aws-sk"));

        Assertions.assertEquals("https://cos.ap-guangzhou.myqcloud.com", properties.getEndpoint());
        Assertions.assertEquals("ap-guangzhou", properties.getRegion());
        Assertions.assertEquals("cos-ak", properties.getAccessKey());
        Assertions.assertEquals("cos-sk", properties.getSecretKey());
    }

    @Test
    void bind_allowsAnonymousWhenAccessKeyAndSecretKeyAreBothBlank() {
        CosFileSystemProperties properties = CosFileSystemProperties.of(Map.of(
                "cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com"));

        Assertions.assertEquals("", properties.getAccessKey());
        Assertions.assertEquals("", properties.getSecretKey());
        Assertions.assertEquals("ap-guangzhou", properties.getRegion());
    }

    @Test
    void toString_masksCredentialsAndNeverLeaksPlaintext() {
        CosFileSystemProperties properties = CosFileSystemProperties.of(Map.of(
                "cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com",
                "cos.access_key", "cos-ak-plain",
                "cos.secret_key", "cos-sk-plain",
                "s3.session_token", "cos-token-plain"));

        String rendered = properties.toString();

        Assertions.assertFalse(rendered.contains("cos-sk-plain"), rendered);
        Assertions.assertFalse(rendered.contains("cos-token-plain"), rendered);
        Assertions.assertTrue(rendered.contains("secretKey=***"), rendered);
        Assertions.assertTrue(rendered.contains("sessionToken=***"), rendered);
        Assertions.assertTrue(rendered.contains("accessKey=cos-ak-plain"), rendered);
        Assertions.assertTrue(rendered.contains("https://cos.ap-guangzhou.myqcloud.com"), rendered);
    }

    @Test
    void provider_sensitivePropertyKeysCoverSecretsButNotAccessKey() {
        Set<String> keys = new CosFileSystemProvider().sensitivePropertyKeys();

        Assertions.assertTrue(keys.contains("cos.secret_key"), keys.toString());
        Assertions.assertTrue(keys.contains("COS_SECRET_KEY"), keys.toString());
        Assertions.assertTrue(keys.contains("COS_SESSION_TOKEN"), keys.toString());
        Assertions.assertFalse(keys.contains("COS_ACCESS_KEY"), keys.toString());
        Assertions.assertFalse(keys.contains("cos.access_key"), keys.toString());
    }

    @Test
    void bind_acceptsLegacyAwsKeysForExistingCosCallers() {
        CosFileSystemProperties properties = CosFileSystemProperties.of(Map.of(
                "AWS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com",
                "AWS_ACCESS_KEY", "aws-ak",
                "AWS_SECRET_KEY", "aws-sk",
                "AWS_BUCKET", "legacy-bucket"));

        Assertions.assertEquals("https://cos.ap-guangzhou.myqcloud.com", properties.getEndpoint());
        Assertions.assertEquals("ap-guangzhou", properties.getRegion());
        Assertions.assertEquals("aws-ak", properties.getAccessKey());
        Assertions.assertEquals("aws-sk", properties.getSecretKey());
        Assertions.assertEquals("legacy-bucket", properties.getBucket());
    }

    @Test
    void toBackendProperties_returnsOnlyAwsCompatibleKeysForBeAdapters() {
        CosFileSystemProperties properties = CosFileSystemProperties.of(Map.of(
                "cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com",
                "cos.access_key", "cos-ak",
                "cos.secret_key", "cos-sk",
                "COS_BUCKET", "cos-bucket",
                "COS_ROLE_ARN", "cos-role"));

        BackendStorageProperties backend = properties.toBackendProperties().orElseThrow();
        Map<String, String> backendMap = backend.toMap();

        Assertions.assertEquals(BackendStorageKind.S3_COMPATIBLE, backend.backendKind());
        Assertions.assertEquals("https://cos.ap-guangzhou.myqcloud.com", backendMap.get("AWS_ENDPOINT"));
        Assertions.assertEquals("ap-guangzhou", backendMap.get("AWS_REGION"));
        Assertions.assertEquals("cos-ak", backendMap.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("cos-sk", backendMap.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("cos-bucket", backendMap.get("AWS_BUCKET"));
        Assertions.assertEquals("cos-role", backendMap.get("AWS_ROLE_ARN"));
        Assertions.assertFalse(backendMap.keySet().stream().anyMatch(key -> key.startsWith("COS_")));
        // Parity with fe-core AbstractS3CompatibleProperties#getAwsCredentialsProviderTypeForBackend:
        // when static credentials are present the type is omitted (BE uses SimpleAWSCredentialsProvider).
        Assertions.assertNull(backendMap.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
    }

    @Test
    void toBackendProperties_emitsAnonymousProviderTypeWhenNoStaticCredentials() {
        CosFileSystemProperties properties = CosFileSystemProperties.of(Map.of(
                "cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com"));

        Map<String, String> backendMap = properties.toBackendProperties().orElseThrow().toMap();

        // Parity with fe-core AbstractS3CompatibleProperties#getAwsCredentialsProviderTypeForBackend:
        // both access key and secret key blank => anonymous access.
        Assertions.assertEquals("ANONYMOUS", backendMap.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
    }

    @Test
    void toMaps_emitCosTuningDefaultsWhenNotConfigured() {
        CosFileSystemProperties properties = CosFileSystemProperties.of(Map.of(
                "cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com"));

        // Parity with fe-core COSProperties defaults (100 / 10000 / 10000). Literal expected values
        // (not DEFAULT_* constants) so that mutating a default in the main class fails this guard.
        Map<String, String> beKv = properties.toMap();
        Assertions.assertEquals("100", beKv.get("AWS_MAX_CONNECTIONS"));
        Assertions.assertEquals("10000", beKv.get("AWS_REQUEST_TIMEOUT_MS"));
        Assertions.assertEquals("10000", beKv.get("AWS_CONNECTION_TIMEOUT_MS"));

        Map<String, String> hadoopKv = properties.toHadoopConfigurationMap();
        Assertions.assertEquals("100", hadoopKv.get("fs.s3a.connection.maximum"));
        Assertions.assertEquals("10000", hadoopKv.get("fs.s3a.connection.request.timeout"));
        Assertions.assertEquals("10000", hadoopKv.get("fs.s3a.connection.timeout"));
    }

    @Test
    void bind_rejectsPartialStaticCredentialsLikeFeCore() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CosFileSystemProperties.of(Map.of(
                        "cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com",
                        "cos.access_key", "ak")));

        Assertions.assertTrue(exception.getMessage().contains(
                "Both the access key and the secret key must be set"));
    }

    @Test
    void bind_requiresEndpointAndRegionAfterNormalization() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CosFileSystemProperties.of(Map.of(
                        "cos.endpoint", "https://cos.myqcloud.com")));

        Assertions.assertTrue(exception.getMessage().contains("Region is not set"));
    }

    @Test
    void provider_bindReturnsCosTypedProperties() throws IOException {
        CosFileSystemProvider provider = new CosFileSystemProvider();
        CosFileSystemProperties properties = provider.bind(Map.of(
                "cos.endpoint", "https://cos.ap-guangzhou.myqcloud.com"));
        FileSystem fileSystem = provider.create(properties);

        Assertions.assertEquals("COS", properties.providerName());
        Assertions.assertEquals(StorageKind.OBJECT_STORAGE, properties.kind());
        Assertions.assertEquals(FileSystemType.S3, properties.type());
        Assertions.assertInstanceOf(CosFileSystem.class, fileSystem);
        Assertions.assertEquals(S3CompatibleFileSystem.class, fileSystem.getClass().getSuperclass());
    }

    @Test
    void provider_supportsLegacyCosEndpointKey() {
        CosFileSystemProvider provider = new CosFileSystemProvider();

        Assertions.assertTrue(provider.supports(Map.of(
                "COS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com")));
    }

    @Test
    void provider_supportsLegacyAwsEndpointKeyForCosDomain() {
        CosFileSystemProvider provider = new CosFileSystemProvider();

        Assertions.assertTrue(provider.supports(Map.of(
                "AWS_ENDPOINT", "https://cos.ap-guangzhou.myqcloud.com")));
    }
}
