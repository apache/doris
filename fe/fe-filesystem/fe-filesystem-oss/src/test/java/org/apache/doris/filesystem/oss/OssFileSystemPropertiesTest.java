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
import org.apache.doris.filesystem.properties.FsCacheKeys;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.spi.S3CompatibleFileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
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

    /**
     * DORIS-29438: the endpoint host is a DNS name, so an upper/mixed case spelling must resolve to
     * the same effective endpoint as the lowercase one. The pre-fix code matched the
     * standard-endpoint pattern case-sensitively, so a mixed case spelling was treated as
     * "non-standard" and silently rewritten to the internal endpoint, while the lowercase spelling
     * was left alone. The case cannot simply be accepted either: the effective endpoint is the value
     * handed to the BE as {@code AWS_ENDPOINT}, where the presigned-URL conversion matches
     * {@code -internal.aliyuncs.com} case-sensitively.
     */
    @Test
    void bind_normalisesEndpointHostCase() {
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "oss-cn-beijing.aliyuncs.com")).getEndpoint());

        // A mixed case public spelling stays the public endpoint (it is not silently switched to
        // -internal the way the pre-fix code did) and is normalised to lowercase.
        OssFileSystemProperties upperPublic = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "OSS-CN-BEIJING.ALIYUNCS.COM"));
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", upperPublic.getEndpoint());
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com",
                upperPublic.toBackendProperties().orElseThrow().toMap().get("AWS_ENDPOINT"));

        // A mixed case internal spelling keeps denoting the same (internal) host, and the BE can
        // still recognise it when converting the endpoint for a public presigned URL.
        Assertions.assertEquals("oss-cn-beijing-internal.aliyuncs.com", OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "OSS-CN-BEIJING-INTERNAL.ALIYUNCS.COM")).getEndpoint());
    }

    @Test
    void bind_buildsInternalEndpointFromRegionWhenEndpointMissing() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.region", "cn-hangzhou"));

        Assertions.assertEquals("oss-cn-hangzhou-internal.aliyuncs.com", properties.getEndpoint());
        Assertions.assertEquals("cn-hangzhou", properties.getRegion());
    }

    @Test
    void bind_usesSameDlfAccessPublicSemanticsAsMetastore() {
        for (String truthy : java.util.Arrays.asList("yes", "ON", "y")) {
            OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                    "dlf.region", "cn-hangzhou",
                    "dlf.access.public", truthy));

            Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", properties.getEndpoint());
        }
    }

    @Test
    void bind_acceptsCanonicalDlfCredentialsAndToken() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "dlf.catalog.endpoint", "dlf.cn-hangzhou.aliyuncs.com",
                "dlf.catalog.accessKeyId", "dlf-ak",
                "dlf.catalog.accessKeySecret", "dlf-sk",
                "dlf.catalog.securityToken", "dlf-token"));

        Assertions.assertEquals("oss-cn-hangzhou-internal.aliyuncs.com", properties.getEndpoint());
        Assertions.assertEquals("cn-hangzhou", properties.getRegion());
        Assertions.assertEquals("dlf-ak", properties.getAccessKey());
        Assertions.assertEquals("dlf-sk", properties.getSecretKey());
        Assertions.assertEquals("dlf-token", properties.getSessionToken());
    }

    @Test
    void bind_derivesRegionFromDlfVpcEndpoint() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "dlf.catalog.endpoint", "https://dlf-vpc.cn-shanghai.aliyuncs.com"));

        Assertions.assertEquals("cn-shanghai", properties.getRegion());
        Assertions.assertEquals("oss-cn-shanghai-internal.aliyuncs.com", properties.getEndpoint());
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
        // Parity with fe-core AbstractS3CompatibleProperties#getAwsCredentialsProviderTypeForBackend:
        // when static credentials are present the type is omitted (BE uses SimpleAWSCredentialsProvider).
        Assertions.assertNull(backendMap.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
    }

    @Test
    void toBackendProperties_emitsAnonymousProviderTypeWhenNoStaticCredentials() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com"));

        Map<String, String> backendMap = properties.toBackendProperties().orElseThrow().toMap();

        // Parity with fe-core AbstractS3CompatibleProperties#getAwsCredentialsProviderTypeForBackend:
        // both access key and secret key blank => anonymous access.
        Assertions.assertEquals("ANONYMOUS", backendMap.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
    }

    @Test
    void toMaps_emitOssTuningDefaultsWhenNotConfigured() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com"));

        // Parity with fe-core OSSProperties defaults (100 / 10000 / 10000). Literal expected values
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
    void toHadoopConfigurationMap_keysFileSystemCacheByCredentialFingerprint() {
        OssFileSystemProperties properties = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "ak",
                "oss.secret_key", "sk"));
        OssFileSystemProperties otherCredentials = OssFileSystemProperties.of(Map.of(
                "oss.endpoint", "https://oss-cn-hangzhou.aliyuncs.com",
                "oss.access_key", "other-ak",
                "oss.secret_key", "other-sk"));

        Map<String, String> hadoopKv = properties.toHadoopConfigurationMap();

        // The Hadoop FileSystem cache stays on. Instead of the retired blanket
        // fs.<scheme>.impl.disable.cache=true, every scheme this storage can be opened with carries
        // its credential fingerprint, which the Doris-patched FileSystem folds into its cache key.
        for (String scheme : List.of("oss", "s3", "s3a")) {
            Assertions.assertNull(hadoopKv.get("fs." + scheme + ".impl.disable.cache"), scheme);
            Assertions.assertEquals(properties.fsCacheFingerprint(),
                    hadoopKv.get(FsCacheKeys.fsCacheKeyProperty(scheme)), scheme);
        }
        // Never the shared, scheme-less name: per-scheme names are what keep a merge lossless.
        Assertions.assertNull(hadoopKv.get(FsCacheKeys.FS_CACHE_KEY_PROPERTY));
        Assertions.assertNotEquals(properties.fsCacheFingerprint(), otherCredentials.fsCacheFingerprint());
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

    // ------------------------------------------------------------------
    // uri-derived endpoint/region (legacy AbstractS3CompatibleProperties
    // setEndpointIfPossible leg 2). Expected values are hardcoded from the
    // legacy fe-core S3URI algorithm — do not "fix" them to look nicer.
    // ------------------------------------------------------------------

    @Test
    void uriOnly_virtualHostedAliyuncsUri_derivesEndpointAndRegion() {
        Map<String, String> raw = new HashMap<>();
        raw.put("uri", "https://mybucket.oss-cn-hangzhou.aliyuncs.com/data/file.csv");
        raw.put("oss.access_key", "ak");
        raw.put("oss.secret_key", "sk");

        OssFileSystemProperties properties = OssFileSystemProperties.of(raw);

        Assertions.assertEquals("oss-cn-hangzhou.aliyuncs.com", properties.getEndpoint());
        Assertions.assertEquals("cn-hangzhou", properties.getRegion());
    }

    @Test
    void uriPlusExplicitEndpoint_explicitEndpointWins() {
        Map<String, String> raw = new HashMap<>();
        raw.put("uri", "https://mybucket.oss-cn-hangzhou.aliyuncs.com/data/file.csv");
        raw.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        raw.put("oss.access_key", "ak");
        raw.put("oss.secret_key", "sk");

        OssFileSystemProperties properties = OssFileSystemProperties.of(raw);

        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", properties.getEndpoint());
        Assertions.assertEquals("cn-beijing", properties.getRegion());
    }

    @Test
    void unparsableUri_isSwallowed_thenRegionNotSetFires() {
        Map<String, String> raw = new HashMap<>();
        raw.put("uri", "oss-cn-hangzhou.aliyuncs.com/bucket/file.csv"); // no scheme
        raw.put("oss.access_key", "ak");
        raw.put("oss.secret_key", "sk");

        IllegalArgumentException exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> OssFileSystemProperties.of(raw));
        Assertions.assertTrue(exception.getMessage().contains("Region is not set"), exception.getMessage());
    }
}
