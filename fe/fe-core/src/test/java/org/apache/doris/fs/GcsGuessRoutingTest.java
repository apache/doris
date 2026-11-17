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

package org.apache.doris.fs;

import org.apache.doris.cloud.proto.Cloud.ObjectStoreInfoPB;
import org.apache.doris.datasource.storage.CloudObjectStoreAdapter;
import org.apache.doris.datasource.storage.ObjectStorageCredentialAdapter;
import org.apache.doris.datasource.storage.S3ThriftAdapter;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.gcs.GcsFileSystemProperties;
import org.apache.doris.filesystem.gcs.GcsFileSystemProvider;
import org.apache.doris.filesystem.minio.MinioFileSystemProperties;
import org.apache.doris.filesystem.minio.MinioFileSystemProvider;
import org.apache.doris.filesystem.s3.S3FileSystemProperties;
import org.apache.doris.filesystem.s3.S3FileSystemProvider;
import org.apache.doris.thrift.TS3StorageParam;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GcsGuessRoutingTest {
    private static final FileSystem GCS_FILESYSTEM = Mockito.mock(FileSystem.class);
    private static final FileSystem S3_FILESYSTEM = Mockito.mock(FileSystem.class);
    private static final FileSystem MINIO_FILESYSTEM = Mockito.mock(FileSystem.class);
    private static final String[] ENDPOINTS = {
            "https://storage.googleapis.com", "https://storage.googleapis.com:443",
            "https://storage.googleapis.com/", "storage.googleapis.com:443/",
            "https://storage.us-east1.rep.googleapis.com", "https://us-east1-storage.googleapis.com"};

    @Test
    public void testExplicitS3KeepsAwsAuthenticationInPrimaryAndAllBindings() {
        for (String endpoint : ENDPOINTS) {
            for (String provider : new String[] {"S3", "s3"}) {
                Map<String, String> props = properties(endpoint);
                props.put("provider", provider);
                assertS3Bindings(props, "DEFAULT");
                props.put("s3.access_key", "ak");
                props.put("s3.secret_key", "sk");
                assertS3Bindings(props, "DEFAULT");
                props.remove("s3.access_key");
                props.remove("s3.secret_key");
                props.put("s3.credentials_provider_type", "ANONYMOUS");
                assertS3Bindings(props, "ANONYMOUS");
            }
        }
    }

    @Test
    public void testProviderNamesPreserveLegacySchemalessUriError() {
        for (String provider : new String[] {"S3", "OSS", "COS", "OBS", "AZURE", "GCS", "GCP"}) {
            Map<String, String> props = properties("https://storage.googleapis.com");
            props.put("provider", provider);
            props.put("uri", "bucket/regression/load/data/example_0.csv");
            props.put("s3.access_key", "ak");
            props.put("s3.secret_key", "sk");
            IllegalArgumentException primary = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> StorageAdapter.of(props), provider);
            Assertions.assertTrue(primary.getMessage().contains("Invalid uri: bucket/"), primary.getMessage());
            IllegalArgumentException all = Assertions.assertThrows(
                    IllegalArgumentException.class, () -> StorageAdapter.ofAll(props), provider);
            Assertions.assertTrue(all.getMessage().contains("Invalid uri: bucket/"), all.getMessage());
        }
    }

    @Test
    public void testGcpAndGcsAliasesSelectTheSameBindings() {
        for (String provider : new String[] {"GCP", "GCS", "gcp", "gcs"}) {
            Map<String, String> props = properties("https://storage.googleapis.com");
            props.put("provider", provider);
            bindings(props, "GCS");
            props.put("s3.access_key", "ak");
            props.put("s3.secret_key", "sk");
            bindings(props, "GCS");
        }
    }

    @Test
    public void testExplicitHdfsFlagIsNotOverriddenByProviderName() {
        Map<String, String> props = properties("https://storage.googleapis.com");
        props.put("provider", "S3");
        props.put("fs.hdfs.support", "true");
        props.put("uri", "hdfs://namenode/path");
        Assertions.assertEquals("HDFS", StorageAdapter.of(props).getStorageName());
        List<StorageAdapter> all = StorageAdapter.ofAll(props);
        Assertions.assertEquals(1, all.size());
        Assertions.assertEquals("HDFS", all.get(0).getStorageName());
    }

    @Test
    public void testInferredGcsEndpointsBindNativeAndHmacWithoutMinio() {
        for (String endpoint : ENDPOINTS) {
            Map<String, String> props = properties(endpoint);
            for (StorageAdapter adapter : bindings(props, "GCS")) {
                Assertions.assertEquals("DEFAULT",
                        adapter.getBackendConfigProperties().get("gs.credential_provider_type"));
            }
            props.put("s3.access_key", "ak");
            props.put("s3.secret_key", "sk");
            for (StorageAdapter adapter : bindings(props, "GCS")) {
                Assertions.assertFalse(adapter.getBackendConfigProperties().containsKey("gs.credential_provider_type"));
                Assertions.assertEquals("ak", adapter.getBackendConfigProperties().get("AWS_ACCESS_KEY"));
            }
        }
    }

    @Test
    public void testInferredNativeGcsStillValidatesEndpoint() {
        for (String endpoint : new String[] {"http://storage.googleapis.com",
                "https://storage.googleapis.com:8443", "https://storage.googleapis.com/bucket",
                "https://storage.googleapis.com?query=value", "https://bucket.storage.googleapis.com"}) {
            Map<String, String> props = properties(endpoint);
            Assertions.assertThrows(IllegalArgumentException.class, () -> StorageAdapter.of(props), endpoint);
            Assertions.assertThrows(IllegalArgumentException.class, () -> StorageAdapter.ofAll(props), endpoint);
        }
    }

    @Test
    public void testNativeGcsHintsWithoutEndpointDoNotBindS3OrMinio() {
        for (Map.Entry<String, String> hint : Map.of(
                "provider", "GCP", "gs.credential_provider_type", "DEFAULT",
                "gs.impersonation_service_account", "target@test.iam.gserviceaccount.com",
                "warehouse", "gs://bucket/warehouse").entrySet()) {
            Map<String, String> props = new HashMap<>();
            props.put("s3.region", "us-east1");
            props.put("_STORAGE_TYPE_", "S3");
            props.put(hint.getKey(), hint.getValue());
            bindings(props, "GCS");
        }
        bindings(Map.of("s3.region", "us-east1", "gs.impersonation_service_account", ""), "GCS");
        bindings(Map.of("s3.region", "us-east-1"), "S3");
    }

    @Test
    public void testNativeCredentialsCannotBeIgnoredByS3Selection() {
        for (Map<String, String> selection : List.of(Map.of("provider", "S3"),
                Map.of("fs.s3.support", "true"), Map.of("fs.minio.support", "true"))) {
            for (String key : new String[] {"gs.credential_provider_type", "gs.impersonation_service_account",
                    "GS.CREDENTIAL_PROVIDER_TYPE", "GS.IMPERSONATION_SERVICE_ACCOUNT"}) {
                Map<String, String> props = properties("https://storage.googleapis.com");
                props.putAll(selection);
                // Even an explicit empty native key must not disappear into AWS/HMAC binding.
                props.put(key, "");
                assertNativeCredentialRejected(() -> StorageAdapter.of(props));
                assertNativeCredentialRejected(() -> StorageAdapter.ofAll(props));
                for (boolean gcsFirst : new boolean[] {false, true}) {
                    FileSystemPluginManager manager = objectStorageManager(gcsFirst);
                    assertNativeCredentialRejected(() -> manager.createFileSystem(props));
                }
                assertNativeCredentialRejected(() -> StorageAdapter.ofProvider("S3", props));
                assertNativeCredentialRejected(() -> StorageAdapter.ofProvider("MINIO", props));
            }
        }
    }

    @Test
    public void testDirectCreationSelectsGcsRegardlessOfRegistrationOrder() throws Exception {
        for (boolean gcsFirst : new boolean[] {false, true}) {
            FileSystemPluginManager manager = objectStorageManager(gcsFirst);
            for (String endpoint : ENDPOINTS) {
                Map<String, String> props = properties(endpoint);
                Assertions.assertSame(GCS_FILESYSTEM, manager.createFileSystem(props));
                props.put("s3.access_key", "ak");
                props.put("s3.secret_key", "sk");
                Assertions.assertSame(GCS_FILESYSTEM, manager.createFileSystem(props));
                props.put("provider", "S3");
                Assertions.assertSame(S3_FILESYSTEM, manager.createFileSystem(props));
            }
            for (Map.Entry<String, String> hint : Map.of("provider", "GCP",
                    "gs.credential_provider_type", "DEFAULT",
                    "gs.impersonation_service_account", "",
                    "warehouse", "gs://bucket/warehouse").entrySet()) {
                Map<String, String> props = new HashMap<>(Map.of("s3.region", "us-east1",
                        "_STORAGE_TYPE_", "S3", "minio.endpoint", "http://localhost:9000"));
                props.put(hint.getKey(), hint.getValue());
                Assertions.assertSame(GCS_FILESYSTEM, manager.createFileSystem(props));
                Assertions.assertEquals("GCS", manager.bindPrimary(props).providerName());
                Assertions.assertEquals(1, manager.bindAll(props).size());
            }
        }
    }

    @Test
    public void testS3RejectsNativeCredentialsWithoutGcsPlugin() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        objectStorageManager(false).getProviders().stream()
                .filter(provider -> "S3".equals(provider.name())).forEach(manager::registerProvider);
        Map<String, String> props = properties("https://storage.googleapis.com");
        props.put("fs.s3.support", "true");
        props.put("gs.credential_provider_type", "DEFAULT");
        assertNativeCredentialRejected(() -> manager.bindPrimary(props));
        assertNativeCredentialRejected(() -> manager.bindAll(props));
        assertNativeCredentialRejected(() -> manager.createFileSystem(props));
    }

    @Test
    public void testExplicitBindingsKeepExistingMultiProviderSemantics() {
        FileSystemPluginManager manager = objectStorageManager(false);
        Map<String, String> props = properties("https://storage.googleapis.com");
        props.put("fs.gcs.support", "true");
        props.put("fs.s3.support", "true");
        props.put("s3.access_key", "ak");
        props.put("s3.secret_key", "sk");
        Assertions.assertEquals("S3", manager.bindPrimary(props).providerName());
        Assertions.assertEquals(List.of("S3", "GCS"), manager.bindAll(props).stream()
                .map(binding -> binding.providerName()).collect(Collectors.toList()));
    }

    private static void assertNativeCredentialRejected(org.junit.jupiter.api.function.Executable action) {
        IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class, action);
        Assertions.assertTrue(failure.getMessage().contains("Native GCP authentication"), failure.getMessage());
    }

    private static FileSystemPluginManager objectStorageManager(boolean gcsFirst) {
        GcsFileSystemProvider gcs = Mockito.spy(new GcsFileSystemProvider());
        S3FileSystemProvider s3 = Mockito.spy(new S3FileSystemProvider());
        MinioFileSystemProvider minio = Mockito.spy(new MinioFileSystemProvider());
        // Keep real selection and binding; only replace client construction to avoid I/O.
        try {
            Mockito.doReturn(GCS_FILESYSTEM).when(gcs).create(Mockito.any(GcsFileSystemProperties.class));
            Mockito.doReturn(S3_FILESYSTEM).when(s3).create(Mockito.any(S3FileSystemProperties.class));
            Mockito.doReturn(MINIO_FILESYSTEM).when(minio).create(Mockito.any(MinioFileSystemProperties.class));
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
        FileSystemPluginManager manager = new FileSystemPluginManager();
        if (gcsFirst) {
            manager.registerProvider(minio);
            manager.registerProvider(s3);
            manager.registerProvider(gcs);
        } else {
            manager.registerProvider(gcs);
            manager.registerProvider(s3);
            manager.registerProvider(minio);
        }
        return manager;
    }

    @Test
    public void testTypedIdentityConnectsPluginCloudAndBeWithoutLosingAlterPresence() {
        Map<String, String> properties = Map.of("provider", "GCP",
                "gs.credential_provider_type", "COMPUTE_ENGINE",
                "gs.impersonation_service_account", "target@test.iam.gserviceaccount.com");
        ObjectStoreInfoPB.Builder cloud = CloudObjectStoreAdapter.getObjStoreInfoPB(properties);
        TS3StorageParam thrift = S3ThriftAdapter.getS3TStorageParam(properties);
        Assertions.assertEquals("COMPUTE_ENGINE", cloud.getCredential().getGcpCredential()
                .getCredentialProviderType().name());
        Assertions.assertEquals("COMPUTE_ENGINE", thrift.getCredential().getGcpCredential()
                .getCredentialProviderType().name());
        Assertions.assertEquals("target@test.iam.gserviceaccount.com", thrift.getCredential()
                .getGcpCredential().getImpersonationServiceAccount());
        Assertions.assertEquals(Map.of("gs.credential_provider_type", "COMPUTE_ENGINE",
                "gs.impersonation_service_account", "target@test.iam.gserviceaccount.com"),
                ObjectStorageCredentialAdapter.toProperties("GCP", cloud.getCredential()));
        CloudObjectStoreAdapter.applyCredentialPatch(cloud, Map.of("gs.impersonation_service_account", ""));
        Assertions.assertFalse(cloud.getCredential().getGcpCredential().hasCredentialProviderType());
        Assertions.assertTrue(cloud.getCredential().getGcpCredential().hasImpersonationServiceAccount());
        Assertions.assertEquals("", cloud.getCredential().getGcpCredential().getImpersonationServiceAccount());
        CloudObjectStoreAdapter.applyCredentialPatch(cloud, Map.of("provider", "GCP"));
        Assertions.assertFalse(cloud.hasCredential());
    }

    private static Map<String, String> properties(String endpoint) {
        Map<String, String> props = new HashMap<>();
        props.put("s3.endpoint", endpoint);
        props.put("s3.region", "us-east1");
        props.put("uri", "s3://bucket/key");
        return props;
    }

    private static void assertS3Bindings(Map<String, String> props, String mode) {
        for (StorageAdapter adapter : bindings(props, "S3")) {
            Map<String, String> backend = adapter.getBackendConfigProperties();
            Assertions.assertEquals(mode, backend.get("AWS_CREDENTIALS_PROVIDER_TYPE"));
            Assertions.assertFalse(backend.containsKey("gs.credential_provider_type"));
        }
    }

    private static List<StorageAdapter> bindings(Map<String, String> props, String expectedProvider) {
        StorageAdapter primary = StorageAdapter.of(props);
        Assertions.assertEquals(expectedProvider, primary.getSpiProperties().providerName());
        // bindAll also supplies the default HDFS filesystem for catalog callers.
        List<StorageAdapter> objectBindings = StorageAdapter.ofAll(props).stream()
                .filter(adapter -> !"HDFS".equals(adapter.getStorageName())).collect(Collectors.toList());
        Assertions.assertEquals(1, objectBindings.size(), props.toString());
        Assertions.assertEquals(expectedProvider, objectBindings.get(0).getSpiProperties().providerName());
        return List.of(primary, objectBindings.get(0));
    }
}
