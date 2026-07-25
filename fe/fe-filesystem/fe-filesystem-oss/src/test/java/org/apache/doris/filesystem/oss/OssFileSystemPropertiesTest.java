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

package org.apache.doris.filesystem.oss;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.BackendStorageKind;
import org.apache.doris.filesystem.properties.BackendStorageProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

class OssFileSystemPropertiesTest {

    @Test
    void bind_usesFeCoreOssAliasOrder() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "oss-ak",
                "AWS_ACCESS_KEY", "aws-ak",
                "oss.secret_key", "oss-sk",
                "AWS_SECRET_KEY", "aws-sk"));

        Assertions.assertEquals("https://oss-cn-hangzhou.aliyuncs.com", properties.getEndpoint());
        Assertions.assertEquals("cn-hangzhou", properties.getRegion());
        Assertions.assertEquals("oss-ak", properties.getAccessKey());
        Assertions.assertEquals("oss-sk", properties.getSecretKey());
    }

    @Test
    void bind_allowsAnonymousWhenAccessKeyAndSecretKeyAreBothBlank() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com"));

        Assertions.assertEquals("", properties.getAccessKey());
        Assertions.assertEquals("", properties.getSecretKey());
        Assertions.assertEquals("cn-hangzhou", properties.getRegion());
    }

    @Test
    void toString_masksCredentialsAndNeverLeaksPlaintext() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "oss-ak-plain",
                "oss.secret_key", "oss-sk-plain",
                "s3.session_token", "oss-token-plain"));

        String rendered = properties.toString();

        Assertions.assertFalse(rendered.contains("oss-sk-plain"), rendered);
        Assertions.assertFalse(rendered.contains("oss-token-plain"), rendered);
        Assertions.assertTrue(rendered.contains("secretKey=***"), rendered);
        Assertions.assertTrue(rendered.contains("sessionToken=***"), rendered);
        Assertions.assertTrue(rendered.contains("accessKey=oss-ak-plain"), rendered);
        Assertions.assertTrue(rendered.contains("https://oss-cn-hangzhou.aliyuncs.com"), rendered);
    }

    @Test
    void provider_sensitivePropertyKeysCoverSecretsButNotAccessKey() {
        Set<String> keys = new OssFileSystemProvider().sensitivePropertyKeys();

        Assertions.assertTrue(keys.contains("oss.secret_key"), keys.toString());
        Assertions.assertTrue(keys.contains("OSS_SECRET_KEY"), keys.toString());
        Assertions.assertFalse(keys.contains("OSS_ACCESS_KEY"), keys.toString());
        Assertions.assertFalse(keys.contains("oss.access_key"), keys.toString());
    }

    @Test
    void bind_acceptsLegacyAwsKeysForExistingOssCallers() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "AWS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com",
                "AWS_ACCESS_KEY", "aws-ak",
                "AWS_SECRET_KEY", "aws-sk",
                "AWS_BUCKET", "legacy-bucket"));

        Assertions.assertEquals("https://oss-cn-hangzhou.aliyuncs.com", properties.getEndpoint());
        Assertions.assertEquals("cn-hangzhou", properties.getRegion());
        Assertions.assertEquals("aws-ak", properties.getAccessKey());
        Assertions.assertEquals("aws-sk", properties.getSecretKey());
        Assertions.assertEquals("legacy-bucket", properties.getBucket());
    }

    @Test
    void bind_buildsInternalEndpointFromRegionWhenEndpointMissing() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.region", "cn-hangzhou"));

        Assertions.assertEquals("oss-cn-hangzhou-internal.aliyuncs.com", properties.getEndpoint());
        Assertions.assertEquals("cn-hangzhou", properties.getRegion());
    }

    @Test
    void toBackendProperties_returnsOnlyAwsCompatibleKeysForBeAdapters() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "oss-ak",
                "oss.secret_key", "oss-sk",
                "OSS_BUCKET", "oss-bucket",
                "OSS_ROLE_ARN", "oss-role"));

        BackendStorageProperties backend = properties.toBackendProperties().orElseThrow();
        Map<String, String> backendMap = backend.toMap();

        Assertions.assertEquals(BackendStorageKind.S3_COMPATIBLE, backend.backendKind());
        Assertions.assertEquals("https://oss-cn-hangzhou.aliyuncs.com", backendMap.get("AWS_ENDPOINT"));
        Assertions.assertEquals("cn-hangzhou", backendMap.get("AWS_REGION"));
        Assertions.assertEquals("oss-ak", backendMap.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("oss-sk", backendMap.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("oss-bucket", backendMap.get("AWS_BUCKET"));
        Assertions.assertEquals("oss-role", backendMap.get("AWS_ROLE_ARN"));
        Assertions.assertFalse(backendMap.keySet().stream().anyMatch(key -> key.startsWith("OSS_")));
    }

    @Test
    void bind_rejectsPartialStaticCredentialsLikeFeCore() {
        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> OssFileSystemProperties.of(Map.of(
                        "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                        "oss.access_key", "ak")));

        Assertions.assertTrue(exception.getMessage().contains(
                "Both the access key and the secret key must be set"));
    }

    @Test
    void provider_bindReturnsOssTypedProperties() throws IOException {
        OssFileSystemProvider provider = new OssFileSystemProvider();
        OssFileSystemProperties properties = provider.bind(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com"));
        FileSystem fileSystem = provider.create(properties);

        Assertions.assertEquals("OSS", properties.providerName());
        Assertions.assertEquals(StorageKind.OBJECT_STORAGE, properties.kind());
        Assertions.assertEquals(FileSystemType.S3, properties.type());
        Assertions.assertInstanceOf(OssFileSystem.class, fileSystem);
        Assertions.assertEquals(S3CompatibleFileSystem.class, fileSystem.getClass().getSuperclass());
    }

    @Test
    void provider_supportsLegacyOssEndpointKey() {
        OssFileSystemProvider provider = new OssFileSystemProvider();

        Assertions.assertTrue(provider.supports(Map.of(
                "OSS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com")));
    }

    @Test
    void provider_supportsLegacyAwsEndpointKeyForOssDomain() {
        OssFileSystemProvider provider = new OssFileSystemProvider();

        Assertions.assertTrue(provider.supports(Map.of(
                "AWS_ENDPOINT", "https://oss-cn-hangzhou.aliyuncs.com")));
    }

    @Test
    void objStorageDoesNotExposeLegacyToS3PropsTranslator() {
        for (Method method : OssObjStorage.class.getDeclaredMethods()) {
            Assertions.assertNotEquals("toS3Props", method.getName());
        }
    }

    @Test
    void bind_roleArnAndRoleSessionName() {
        OssFileSystemProperties p = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "ak",
                "oss.secret_key", "sk",
                "oss.role_arn", "acs:ram::123:role/r",
                "oss.role_session_name", "my-session",
                "oss.external_id", "ext-id"));

        Assertions.assertEquals("acs:ram::123:role/r", p.getRoleArn());
        Assertions.assertEquals("my-session", p.getRoleSessionName());
        Assertions.assertEquals("ext-id", p.getExternalId());
    }

    @Test
    void bind_roleSessionName_defaultsToDorisSession() {
        OssFileSystemProperties p = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "ak",
                "oss.secret_key", "sk",
                "oss.role_arn", "acs:ram::123:role/r"));

        Assertions.assertEquals("doris-session", p.getRoleSessionName());
    }

    @Test
    void bind_ecsRamRoleName() {
        OssFileSystemProperties p = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.ecs_ram_role_name", "my-ecs-role"));

        Assertions.assertEquals("my-ecs-role", p.getEcsRamRoleName());
        Assertions.assertEquals("", p.getAccessKey());
    }

    @Test
    void validate_roleArnAndEcsRamRoleAreMutuallyExclusive() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                OssFileSystemProperties.of(Map.of(
                        "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                        "oss.access_key", "ak",
                        "oss.secret_key", "sk",
                        "oss.role_arn", "acs:ram::123:role/r",
                        "oss.ecs_ram_role_name", "my-role")));
    }

    @Test
    void validate_legacyRoleArnAliasesResolved() {
        OssFileSystemProperties p = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "ak",
                "oss.secret_key", "sk",
                "OSS_ROLE_ARN", "acs:ram::123:role/legacy"));

        Assertions.assertEquals("acs:ram::123:role/legacy", p.getRoleArn());
    }

    @Test
    void bind_oidcRrsaProperties() {
        OssFileSystemProperties p = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "OSS_ROLE_ARN", "acs:ram::123:role/r",
                "oss.oidc_provider_arn", "acs:ram::123:oidc-provider/p",
                "oss.oidc_token_file", "/var/run/secrets/token",
                "oss.role_session_name", "doris-k8s"));

        Assertions.assertEquals("acs:ram::123:role/r", p.getRoleArn());
        Assertions.assertEquals("acs:ram::123:oidc-provider/p", p.getOidcProviderArn());
        Assertions.assertEquals("/var/run/secrets/token", p.getOidcTokenFile());
        Assertions.assertEquals("doris-k8s", p.getRoleSessionName());
    }

    @Test
    void validate_oidcProviderArnRequiresTokenFileTogether() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                OssFileSystemProperties.of(Map.of(
                        "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                        "oss.oidc_provider_arn", "acs:ram::123:oidc-provider/p"
                        // oss.oidc_token_file missing — must be set together
                )));
    }

    @Test
    void validate_ecsRamRoleAndOidcAreMutuallyExclusive() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                OssFileSystemProperties.of(Map.of(
                        "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                        "oss.ecs_ram_role_name", "my-role",
                        "oss.oidc_provider_arn", "acs:ram::123:oidc-provider/p",
                        "oss.oidc_token_file", "/var/run/secrets/token")));
    }

    @Test
    void bind_credentialsProviderType_instanceProfile() {
        OssFileSystemProperties p = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.credentials_provider", "instance_profile"));

        Assertions.assertEquals(OssCredentialsProviderType.INSTANCE_PROFILE,
                p.getCredentialsProviderType());
    }

    @Test
    void bind_credentialsProviderType_aliases() {
        Assertions.assertEquals(OssCredentialsProviderType.INSTANCE_PROFILE,
                OssCredentialsProviderType.fromString("ecs"));
        Assertions.assertEquals(OssCredentialsProviderType.OIDC,
                OssCredentialsProviderType.fromString("rrsa"));
        Assertions.assertEquals(OssCredentialsProviderType.OIDC,
                OssCredentialsProviderType.fromString("WEB_IDENTITY"));
        Assertions.assertEquals(OssCredentialsProviderType.DEFAULT,
                OssCredentialsProviderType.fromString(""));
        Assertions.assertEquals(OssCredentialsProviderType.DEFAULT,
                OssCredentialsProviderType.fromString(null));
    }

    @Test
    void validate_unsupportedCredentialsProvider() {
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                OssFileSystemProperties.of(Map.of(
                        "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                        "oss.credentials_provider", "INVALID_TYPE")));
    }
}
