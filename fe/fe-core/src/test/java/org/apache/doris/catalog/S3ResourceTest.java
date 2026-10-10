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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.storage.CloudObjectStoreAdapter;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.datasource.storage.S3ThriftAdapter;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TGcpCredential;
import org.apache.doris.thrift.TS3StorageParam;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class S3ResourceTest {
    private static final Logger LOG = LogManager.getLogger(S3ResourceTest.class);
    private String name;
    private String type;

    private String s3Endpoint;
    private String s3Region;
    private String s3RootPath;
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3MaxConnections;
    private String s3ReqTimeoutMs;
    private String s3ConnTimeoutMs;
    private String s3Bucket;
    private Map<String, String> s3Properties;

    @BeforeEach
    public void setUp() {
        name = "s3";
        type = "s3";
        s3Endpoint = "http://aaa";
        s3Region = "bj";
        s3RootPath = "/path/to/root";
        s3AccessKey = "xxx";
        s3SecretKey = "yyy";
        s3MaxConnections = "50";
        s3ReqTimeoutMs = "3000";
        s3ConnTimeoutMs = "1000";
        s3Bucket = "test-bucket";
        s3Properties = new HashMap<>();
        s3Properties.put("type", type);
        s3Properties.put("AWS_ENDPOINT", s3Endpoint);
        s3Properties.put("AWS_REGION", s3Region);
        s3Properties.put("AWS_ROOT_PATH", s3RootPath);
        s3Properties.put("AWS_ACCESS_KEY", s3AccessKey);
        s3Properties.put("AWS_SECRET_KEY", s3SecretKey);
        s3Properties.put("AWS_BUCKET", s3Bucket);
        s3Properties.put("s3_validity_check", "false");
    }

    @Test
    public void testFromStmt() throws UserException {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                    .thenReturn(true);

            // resource with default settings
            CreateResourceCommand createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
            createResourceCommand.getInfo().validate();

            S3Resource s3Resource = (S3Resource) Resource.fromCommand(createResourceCommand);
            Assertions.assertEquals(name, s3Resource.getName());
            Assertions.assertEquals(type, s3Resource.getType().name().toLowerCase());
            Assertions.assertEquals(s3Endpoint, s3Resource.getProperty(S3ResourceCompat.ENDPOINT));
            Assertions.assertEquals(s3Region, s3Resource.getProperty(S3ResourceCompat.REGION));
            Assertions.assertEquals(s3RootPath, s3Resource.getProperty(S3ResourceCompat.ROOT_PATH));
            Assertions.assertEquals(s3AccessKey, s3Resource.getProperty(S3ResourceCompat.ACCESS_KEY));
            Assertions.assertEquals(s3SecretKey, s3Resource.getProperty(S3ResourceCompat.SECRET_KEY));
            Assertions.assertEquals(s3MaxConnections, s3Resource.getProperty(S3ResourceCompat.MAX_CONNECTIONS));
            Assertions.assertEquals(s3ReqTimeoutMs, s3Resource.getProperty(S3ResourceCompat.REQUEST_TIMEOUT_MS));
            Assertions.assertEquals(s3ConnTimeoutMs, s3Resource.getProperty(S3ResourceCompat.CONNECTION_TIMEOUT_MS));

            // with no default settings
            s3Properties.put(S3ResourceCompat.MAX_CONNECTIONS, "100");
            s3Properties.put(S3ResourceCompat.REQUEST_TIMEOUT_MS, "2000");
            s3Properties.put(S3ResourceCompat.CONNECTION_TIMEOUT_MS, "2000");
            s3Properties.put(S3ResourceCompat.VALIDITY_CHECK, "false");

            createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
            createResourceCommand.getInfo().validate();

            s3Resource = (S3Resource) Resource.fromCommand(createResourceCommand);
            Assertions.assertEquals(name, s3Resource.getName());
            Assertions.assertEquals(type, s3Resource.getType().name().toLowerCase());
            Assertions.assertEquals(s3Endpoint, s3Resource.getProperty(S3ResourceCompat.ENDPOINT));
            Assertions.assertEquals(s3Region, s3Resource.getProperty(S3ResourceCompat.REGION));
            Assertions.assertEquals(s3RootPath, s3Resource.getProperty(S3ResourceCompat.ROOT_PATH));
            Assertions.assertEquals(s3AccessKey, s3Resource.getProperty(S3ResourceCompat.ACCESS_KEY));
            Assertions.assertEquals(s3SecretKey, s3Resource.getProperty(S3ResourceCompat.SECRET_KEY));
            Assertions.assertEquals("100", s3Resource.getProperty(S3ResourceCompat.MAX_CONNECTIONS));
            Assertions.assertEquals("2000", s3Resource.getProperty(S3ResourceCompat.REQUEST_TIMEOUT_MS));
            Assertions.assertEquals("2000", s3Resource.getProperty(S3ResourceCompat.CONNECTION_TIMEOUT_MS));
        }
    }

    @Test
    public void testAbnormalResource() throws UserException {
        Assertions.assertThrows(DdlException.class, () -> {
            try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
                Env env = Mockito.mock(Env.class);
                AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
                mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
                Mockito.when(env.getAccessManager()).thenReturn(accessManager);
                Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                        .thenReturn(true);

                s3Properties.remove("AWS_ENDPOINT");

                CreateResourceCommand createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
                createResourceCommand.getInfo().validate();

                Resource.fromCommand(createResourceCommand);
            }
        });
    }

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. write
        // Path path = Files.createFile(Paths.get("./s3Resource"));
        Path path = Paths.get("./s3Resource");
        DataOutputStream s3Dos = new DataOutputStream(Files.newOutputStream(path));

        S3Resource s3Resource1 = new S3Resource("s3_1");
        s3Resource1.write(s3Dos);

        ImmutableMap<String, String> properties = ImmutableMap.of(
                "AWS_ENDPOINT", "aaa",
                "AWS_REGION", "bbb",
                "AWS_ROOT_PATH", "/path/to/root",
                "AWS_ACCESS_KEY", "xxx",
                "AWS_SECRET_KEY", "yyy",
                "AWS_BUCKET", "test-bucket",
                "s3_validity_check", "false"
        );
        S3Resource s3Resource2 = new S3Resource("s3_2");
        s3Resource2.setProperties(properties);
        s3Resource2.write(s3Dos);

        s3Dos.flush();
        s3Dos.close();

        // 2. read
        DataInputStream s3Dis = new DataInputStream(Files.newInputStream(path));
        S3Resource rS3Resource1 = (S3Resource) S3Resource.read(s3Dis);
        S3Resource rS3Resource2 = (S3Resource) S3Resource.read(s3Dis);

        Assertions.assertEquals("s3_1", rS3Resource1.getName());
        Assertions.assertEquals("s3_2", rS3Resource2.getName());

        Assertions.assertEquals("aaa", rS3Resource2.getProperty(S3ResourceCompat.ENDPOINT));
        Assertions.assertEquals("aaa",
                CloudObjectStoreAdapter.getObjStoreInfoPB(rS3Resource2.getCopiedProperties()).getEndpoint());
        Assertions.assertEquals(rS3Resource2.getProperty(S3ResourceCompat.REGION), "bbb");
        Assertions.assertEquals(rS3Resource2.getProperty(S3ResourceCompat.ROOT_PATH), "/path/to/root");
        Assertions.assertEquals(rS3Resource2.getProperty(S3ResourceCompat.ACCESS_KEY), "xxx");
        Assertions.assertEquals(rS3Resource2.getProperty(S3ResourceCompat.SECRET_KEY), "yyy");
        Assertions.assertEquals(rS3Resource2.getProperty(S3ResourceCompat.MAX_CONNECTIONS), "50");
        Assertions.assertEquals(rS3Resource2.getProperty(S3ResourceCompat.REQUEST_TIMEOUT_MS), "3000");
        Assertions.assertEquals(rS3Resource2.getProperty(S3ResourceCompat.CONNECTION_TIMEOUT_MS), "1000");

        // 3. delete
        s3Dis.close();
        Files.deleteIfExists(path);
    }

    @Test
    public void testConcurrentAltersPreserveBothUpdates() throws Exception {
        CountDownLatch firstReady = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean(true);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        S3Resource resource = new S3Resource("concurrent_alter") {
            @Override
            public void writeLock() {
                if (first.compareAndSet(true, false)) {
                    firstReady.countDown();
                    try {
                        Assertions.assertTrue(releaseFirst.await(10, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
                super.writeLock();
            }
        };
        resource.setProperties(ImmutableMap.copyOf(s3Properties));
        Thread updateConnections = new Thread(() -> {
            try {
                resource.modifyProperties(ImmutableMap.of(S3ResourceCompat.MAX_CONNECTIONS, "77"));
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        Thread updateTimeout = new Thread(() -> {
            secondStarted.countDown();
            try {
                resource.modifyProperties(ImmutableMap.of(S3ResourceCompat.REQUEST_TIMEOUT_MS, "8888"));
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        try {
            updateConnections.start();
            Assertions.assertTrue(firstReady.await(10, TimeUnit.SECONDS));
            updateTimeout.start();
            Assertions.assertTrue(secondStarted.await(10, TimeUnit.SECONDS));
            // The second ALTER either waits for the first one's monitor, or (before the fix)
            // finishes and is subsequently overwritten by the first one's stale snapshot.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (updateTimeout.isAlive() && updateTimeout.getState() != Thread.State.BLOCKED
                    && System.nanoTime() < deadline) {
                Thread.yield();
            }
            Assertions.assertTrue(!updateTimeout.isAlive() || updateTimeout.getState() == Thread.State.BLOCKED);
        } finally {
            releaseFirst.countDown();
            updateConnections.join(10000);
            updateTimeout.join(10000);
        }
        Assertions.assertFalse(updateConnections.isAlive());
        Assertions.assertFalse(updateTimeout.isAlive());
        Assertions.assertNull(failure.get());
        Assertions.assertEquals("77", resource.getProperty(S3ResourceCompat.MAX_CONNECTIONS));
        Assertions.assertEquals("8888", resource.getProperty(S3ResourceCompat.REQUEST_TIMEOUT_MS));
    }

    @Test
    public void testModifyProperties() throws Exception {
        ImmutableMap<String, String> properties = ImmutableMap.of(
                "AWS_ENDPOINT", "aaa",
                "AWS_REGION", "bbb",
                "AWS_ROOT_PATH", "/path/to/root",
                "AWS_ACCESS_KEY", "xxx",
                "AWS_SECRET_KEY", "yyy",
                "AWS_BUCKET", "test-bucket",
                "s3_validity_check", "false"
        );
        S3Resource s3Resource = new S3Resource("t_source");
        s3Resource.setProperties(properties);
        FeConstants.runningUnitTest = true;

        Map<String, String> modify = new HashMap<>();
        modify.put("s3.access_key", "aaa");
        s3Resource.modifyProperties(modify);

        modify.clear();
        modify.put(S3ResourceCompat.ENDPOINT, "new-endpoint");
        s3Resource.modifyProperties(modify);
        Assertions.assertEquals("new-endpoint", s3Resource.getProperty(S3ResourceCompat.ENDPOINT));
        Assertions.assertEquals("new-endpoint", s3Resource.getProperty(S3ResourceCompat.Env.ENDPOINT));
        Assertions.assertEquals("new-endpoint",
                CloudObjectStoreAdapter.getObjStoreInfoPB(s3Resource.getCopiedProperties()).getEndpoint());

        modify.clear();
        modify.put(S3ResourceCompat.Env.ENDPOINT, "http://other-endpoint");
        s3Resource.modifyProperties(modify);
        Assertions.assertEquals("http://other-endpoint", s3Resource.getProperty(S3ResourceCompat.ENDPOINT));
        Assertions.assertEquals("http://other-endpoint", s3Resource.getProperty(S3ResourceCompat.Env.ENDPOINT));
    }

    @Test
    public void testExplicitSchemeIsPreserved() throws DdlException {
        ImmutableMap<String, String> properties = ImmutableMap.of(
                "AWS_ENDPOINT", "https://aaa",
                "AWS_REGION", "bbb",
                "AWS_ROOT_PATH", "/path/to/root",
                "AWS_ACCESS_KEY", "xxx",
                "AWS_SECRET_KEY", "yyy",
                "AWS_BUCKET", "test-bucket",
                "s3_validity_check", "false"
        );
        S3Resource s3Resource = new S3Resource("s3_2");
        s3Resource.setProperties(properties);
        Assertions.assertEquals(s3Resource.getProperty(S3ResourceCompat.ENDPOINT), "https://aaa");
    }

    @Test
    public void testPingS3() {
        try {
            String accessKey = System.getenv("ACCESS_KEY");
            String secretKey = System.getenv("SECRET_KEY");
            String bucket = System.getenv("BUCKET");
            String endpoint = System.getenv("ENDPOINT");
            String region = System.getenv("REGION");
            String provider = System.getenv("PROVIDER");

            Assumptions.assumeTrue(!Strings.isNullOrEmpty(accessKey), "ACCESS_KEY isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(secretKey), "SECRET_KEY isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(bucket), "BUCKET isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(endpoint), "ENDPOINT isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(region), "REGION isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(provider), "PROVIDER isNullOrEmpty.");

            Map<String, String> properties = new HashMap<>();
            properties.put("s3.endpoint", endpoint);
            properties.put("s3.region", region);
            properties.put("s3.access_key", accessKey);
            properties.put("s3.secret_key", secretKey);
            properties.put("provider", provider);
            S3Resource.pingS3(bucket, "fe_ut_prefix", properties);
        } catch (DdlException e) {
            LOG.info("testPingS3 exception:", e);
            Assertions.assertTrue(false, e.getMessage());
        }
    }

    @Test
    public void testPingS3WithRoleArn() {
        try {
            String endpoint = System.getenv("ENDPOINT");
            String region = System.getenv("REGION");
            String provider = System.getenv("PROVIDER");

            String roleArn = System.getenv("ROLE_ARN");
            String externalId = System.getenv("EXTERNAL_ID");
            String bucket = System.getenv("BUCKET");

            Assumptions.assumeTrue(!Strings.isNullOrEmpty(endpoint), "ENDPOINT isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(region), "REGION isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(provider), "PROVIDER isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(roleArn), "ROLE_ARN isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(externalId), "EXTERNAL_ID isNullOrEmpty.");
            Assumptions.assumeTrue(!Strings.isNullOrEmpty(bucket), "BUCKET isNullOrEmpty.");

            Map<String, String> properties = new HashMap<>();
            properties.put("s3.endpoint", endpoint);
            properties.put("s3.region", region);
            properties.put("s3.role_arn", roleArn);
            properties.put("s3.external_id", externalId);
            properties.put("provider", provider);
            S3Resource.pingS3(bucket, "fe_ut_role_prefix", properties);
        } catch (DdlException e) {
            LOG.info("testPingS3WithRoleArn exception:", e);
            Assertions.assertTrue(false, e.getMessage());
        }
    }

    @Test
    public void testGsEndpointAndAlterAliases() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("provider", "gcp");
        properties.put("s3.endpoint", "https://old.example.com");
        properties.put("gs.endpoint", "https://storage.googleapis.com");
        properties.put("s3.region", "us-central1");
        properties.put("s3.bucket", "bucket");
        properties.put("s3.root.path", "prefix");
        properties.put("s3.access_key", "access");
        properties.put("s3.secret_key", "secret");
        properties.put("s3_validity_check", "false");
        properties.put("type", "s3");
        S3Resource resource = new S3Resource("gcp_resource");
        resource.setProperties(ImmutableMap.copyOf(properties));
        Assertions.assertEquals("us-central1", resource.getProperty(S3ResourceCompat.REGION));
        Assertions.assertEquals("bucket", resource.getProperty(S3ResourceCompat.BUCKET));
        Assertions.assertEquals("prefix", resource.getProperty(S3ResourceCompat.ROOT_PATH));
        Assertions.assertEquals("https://storage.googleapis.com", resource.getProperty(S3ResourceCompat.ENDPOINT));
        Assertions.assertEquals("https://storage.googleapis.com",
                S3ThriftAdapter.getS3TStorageParam(resource.getCopiedProperties()).getEndpoint());
        Assertions.assertFalse(resource.getCopiedProperties().containsKey("gs.endpoint"));
        resource.modifyProperties(ImmutableMap.of("gs.endpoint", "https://custom.example.com",
                "s3_validity_check", "false"));
        Assertions.assertEquals("https://custom.example.com", resource.getProperty(S3ResourceCompat.ENDPOINT));
        resource.modifyProperties(ImmutableMap.of("s3.endpoint", "https://storage.googleapis.com",
                "s3_validity_check", "false"));
        Assertions.assertEquals("https://storage.googleapis.com", resource.getProperty(S3ResourceCompat.ENDPOINT));
        Assertions.assertEquals("https://storage.googleapis.com", properties.get("gs.endpoint"));
        Assertions.assertEquals("https://old.example.com", properties.get("s3.endpoint"));
    }

    @Test
    public void testExplicitS3DefaultChainSurvivesGcsPolicyAndVaultCreation() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("provider", "S3");
        properties.put("type", "s3");
        properties.put("s3.endpoint", "https://storage.googleapis.com");
        properties.put("s3.region", "us-central1");
        properties.put("s3.bucket", "bucket");
        properties.put("s3.root.path", "prefix");
        AtomicBoolean pinged = new AtomicBoolean();
        try (MockedStatic<S3Resource> resourceMock = Mockito.mockStatic(S3Resource.class, Mockito.CALLS_REAL_METHODS);
                MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
            resourceMock.when(() -> S3Resource.pingS3(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
                    .thenAnswer(invocation -> {
                        Map<String, String> pingProperties = invocation.getArgument(2);
                        StorageAdapter adapter = StorageAdapter.of(pingProperties);
                        Assertions.assertEquals("S3", adapter.getSpiProperties().providerName());
                        Assertions.assertEquals("DEFAULT", adapter.getBackendConfigProperties()
                                .get("AWS_CREDENTIALS_PROVIDER_TYPE"));
                        pinged.set(true);
                        return null;
                    });
            CreateResourceCommand command = new CreateResourceCommand(
                    new CreateResourceInfo(false, false, "s3_vault", ImmutableMap.copyOf(properties)));
            command.getInfo().analyzeResourceType();
            S3StorageVault vault = new S3StorageVault("s3_vault", false, false, command);
            Assertions.assertTrue(pinged.get());
            Map<String, String> stored = vault.getCopiedProperties();
            Assertions.assertFalse(S3ThriftAdapter.getS3TStorageParam(stored).isSetCredProviderType());
            Assertions.assertFalse(CloudObjectStoreAdapter.getObjStoreInfoPB(stored).hasCredProviderType());
            // Anonymous remains an explicit choice and remains forbidden for GCS vaults.
            stored.put("s3.credentials_provider_type", "ANONYMOUS");
            Assertions.assertEquals(org.apache.doris.thrift.TCredProviderType.ANONYMOUS,
                    S3ThriftAdapter.getS3TStorageParam(stored).getCredProviderType());
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> CloudObjectStoreAdapter.getObjStoreInfoPB(stored));
        }
    }

    @Test
    public void testInferredGcpAliasesForResourceAndVault() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "s3");
        properties.put("gs.endpoint", "https://storage.googleapis.com");
        properties.put("gs.credential_provider_type", "DEFAULT");
        properties.put("s3.bucket", "bucket");
        properties.put("s3.root.path", "prefix");
        properties.put("s3_validity_check", "false");
        S3Resource resource = new S3Resource("inferred_gcp");
        resource.setProperties(ImmutableMap.copyOf(properties));
        Assertions.assertEquals("GCP", resource.getProperty("provider"));
        Assertions.assertEquals(properties.get("gs.endpoint"), resource.getProperty("s3.endpoint"));
        resource.modifyProperties(ImmutableMap.of("gs.connection.timeout", "789"));
        Assertions.assertEquals("789", resource.getProperty("s3.connection.timeout"));
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
            CreateResourceCommand command = new CreateResourceCommand(
                    new CreateResourceInfo(false, false, "gcp_vault", ImmutableMap.copyOf(properties)));
            command.getInfo().analyzeResourceType();
            S3StorageVault vault = new S3StorageVault("gcp_vault", false, false, command);
            Assertions.assertEquals("https://storage.googleapis.com",
                    CloudObjectStoreAdapter.getObjStoreInfoPB(vault.getCopiedProperties()).getEndpoint());
            Assertions.assertTrue(CloudObjectStoreAdapter.getObjStoreInfoPB(vault.getCopiedProperties())
                    .getCredential().hasGcpCredential());
        }
        // Legacy persisted resources can have an inferred identity without a provider field.
        Field field = S3Resource.class.getDeclaredField("properties");
        field.setAccessible(true);
        Map<String, String> legacy = resource.getCopiedProperties();
        legacy.remove("provider");
        field.set(resource, legacy);
        resource.modifyProperties(ImmutableMap.of("gs.connection.timeout", "987"));
        Assertions.assertEquals("987", resource.getProperty("s3.connection.timeout"));
        Assertions.assertEquals("GCP", resource.getProperty("provider"));
    }

    @Test
    public void testGsEndpointForStorageVault() throws Exception {
        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (String provider : new String[] {"GCP", "gcp", "S3", "OSS"}) {
                Map<String, String> properties = new HashMap<>(s3Properties);
                properties.put("provider", provider);
                properties.put("gs.endpoint", "https://storage.googleapis.com");
                properties.put("gs.access_key", "gcp-access");
                properties.put("gs.secret_key", "gcp-secret");
                properties.put("gs.use_path_style", "true");
                properties.put("s3.root.path", "prefix");
                CreateResourceCommand command = new CreateResourceCommand(
                        new CreateResourceInfo(false, false, "gcp_vault", ImmutableMap.copyOf(properties)));
                command.getInfo().analyzeResourceType();
                S3StorageVault vault = new S3StorageVault("gcp_vault", false, false, command);
                vault.checkCreationProperties(ImmutableMap.copyOf(properties));
                Assertions.assertEquals("prefix", vault.getCopiedProperties().get(S3ResourceCompat.ROOT_PATH));
                String expectedEndpoint = "GCP".equalsIgnoreCase(provider)
                        ? "https://storage.googleapis.com" : s3Endpoint;
                Assertions.assertEquals(expectedEndpoint,
                        CloudObjectStoreAdapter.getObjStoreInfoPB(vault.getCopiedProperties()).getEndpoint());
                Assertions.assertEquals("GCP".equalsIgnoreCase(provider) ? "gcp-access" : s3AccessKey,
                        CloudObjectStoreAdapter.getObjStoreInfoPB(vault.getCopiedProperties()).getAk());
                Assertions.assertEquals("GCP".equalsIgnoreCase(provider) ? "gcp-secret" : s3SecretKey,
                        CloudObjectStoreAdapter.getObjStoreInfoPB(vault.getCopiedProperties()).getSk());
                Assertions.assertEquals("GCP".equalsIgnoreCase(provider),
                        CloudObjectStoreAdapter.getObjStoreInfoPB(vault.getCopiedProperties()).getUsePathStyle());
            }
        }
    }

    @Test
    public void testStorageVaultRejectsNonAllowlistedAliases() throws Exception {
        StorageVaultMgr manager = new StorageVaultMgr(null);
        for (String key : new String[] {"gs.endpoint", S3ResourceCompat.ENDPOINT, S3ResourceCompat.Env.ENDPOINT,
                "gs.access_key", "gs.secret_key", "gs.session_token", "gs.connection.maximum",
                "gs.connection.request.timeout", "gs.connection.timeout", "gs.use_path_style",
                "gs.force_parsing_by_standard_uri"}) {
            try {
                manager.alterStorageVault(StorageVault.StorageVaultType.S3,
                        ImmutableMap.of(key, "https://custom.example.com"), "gcp_vault");
                Assertions.fail("Storage Vault must reject properties outside its ALTER whitelist: " + key);
            } catch (IllegalArgumentException exception) {
                Assertions.assertEquals("Alter property " + key + " is not allowed.", exception.getMessage());
            }
        }
    }

    @Test
    public void testBlankGsEndpointPreservesResourceAliases() throws Exception {
        for (String blank : new String[] {"", " \t"}) {
            for (String endpointKey : new String[] {S3ResourceCompat.ENDPOINT, S3ResourceCompat.Env.ENDPOINT}) {
                Map<String, String> properties = new HashMap<>(s3Properties);
                properties.put("provider", "GCP");
                properties.remove(S3ResourceCompat.Env.ENDPOINT);
                properties.put(endpointKey, s3Endpoint);
                properties.put("gs.endpoint", blank);
                properties.put("gs.connection.timeout", blank);
                S3Resource resource = new S3Resource("gcp_resource");
                resource.setProperties(ImmutableMap.copyOf(properties));
                Assertions.assertEquals(s3Endpoint, resource.getProperty(S3ResourceCompat.ENDPOINT));
                Assertions.assertFalse(resource.getCopiedProperties().containsKey("gs.endpoint"));
                Assertions.assertEquals(Integer.parseInt(s3ConnTimeoutMs),
                        S3ThriftAdapter.getS3TStorageParam(resource.getCopiedProperties()).getConnTimeoutMs());
                Map<String, String> updated = new HashMap<>();
                updated.put("gs.endpoint", blank);
                updated.put("s3_validity_check", "false");
                resource.modifyProperties(updated);
                Assertions.assertEquals(s3Endpoint, resource.getProperty(S3ResourceCompat.ENDPOINT));
                Assertions.assertEquals(blank, updated.get("gs.endpoint"));
                Assertions.assertFalse(resource.getCopiedProperties().containsKey("gs.connection.timeout"));
            }
        }
    }

    @Test
    public void testGsEndpointCannotAlterPolicyResource() throws Exception {
        S3Resource resource = new S3Resource("gcp_resource");
        Map<String, String> properties = new HashMap<>(s3Properties);
        properties.put("provider", "GCP");
        resource.setProperties(ImmutableMap.copyOf(properties));
        resource.references.put("policy", Resource.ReferenceType.POLICY);
        try {
            resource.modifyProperties(ImmutableMap.of("gs.endpoint", "https://custom.example.com"));
            Assertions.fail("A gs.endpoint alias must not bypass the policy endpoint restriction");
        } catch (DdlException exception) {
            Assertions.assertTrue(exception.getMessage().contains(S3ResourceCompat.ENDPOINT));
        }
        Assertions.assertEquals(s3Endpoint, resource.getProperty(S3ResourceCompat.ENDPOINT));
    }

    @Test
    public void testGsEndpointDoesNotOverrideOtherProviders() throws Exception {
        for (String provider : new String[] {"S3", "OSS", "AZURE"}) {
            Map<String, String> properties = new HashMap<>(s3Properties);
            if (!provider.isEmpty()) {
                properties.put("provider", provider);
            }
            properties.put("gs.endpoint", "https://storage.googleapis.com");
            S3Resource resource = new S3Resource("other_resource");
            resource.setProperties(ImmutableMap.copyOf(properties));
            Assertions.assertEquals(s3Endpoint, resource.getProperty(S3ResourceCompat.ENDPOINT));
            resource.modifyProperties(ImmutableMap.of("gs.endpoint", "https://custom.example.com"));
            Assertions.assertEquals(s3Endpoint, resource.getProperty(S3ResourceCompat.ENDPOINT));
            Assertions.assertEquals(s3Endpoint, resource.getProperty(S3ResourceCompat.Env.ENDPOINT));
        }
    }

    @Test
    public void testGsEndpointUsesEffectiveProviderOnAlter() throws Exception {
        S3Resource resource = new S3Resource("gcp_resource");
        Map<String, String> properties = new HashMap<>(s3Properties);
        properties.put("provider", "GCP");
        resource.setProperties(ImmutableMap.copyOf(properties));
        resource.modifyProperties(ImmutableMap.of("provider", "S3", "gs.endpoint", "https://ignored.example.com"));
        Assertions.assertEquals("S3", resource.getProperty("provider"));
        Assertions.assertEquals(s3Endpoint, resource.getProperty(S3ResourceCompat.ENDPOINT));
        resource.modifyProperties(ImmutableMap.of("provider", "gcp", "gs.endpoint", "https://storage.googleapis.com"));
        Assertions.assertEquals("gcp", resource.getProperty("provider"));
        Assertions.assertEquals("https://storage.googleapis.com", resource.getProperty(S3ResourceCompat.ENDPOINT));
        resource.modifyProperties(ImmutableMap.of("provider", "", "gs.endpoint", "https://custom.example.com"));
        Assertions.assertEquals("gcp", resource.getProperty("provider"));
        Assertions.assertEquals("https://custom.example.com", resource.getProperty(S3ResourceCompat.ENDPOINT));
    }

    @Test
    public void testGsAliasesMatchConnectorAndResourceProtocols() throws Exception {
        for (String provider : new String[] {"GCP", "gcp"}) {
            Map<String, String> properties = new HashMap<>(s3Properties);
            properties.put("provider", provider);
            properties.put("gs.endpoint", "https://storage.googleapis.com");
            properties.put("gs.access_key", "gcp-access");
            properties.put("gs.secret_key", "gcp-secret");
            properties.put("gs.session_token", "gcp-token");
            properties.put("gs.connection.maximum", "123");
            properties.put("gs.connection.request.timeout", "456");
            properties.put("gs.connection.timeout", "789");
            properties.put("gs.use_path_style", "true");
            properties.put("gs.force_parsing_by_standard_uri", "true");
            properties.put(S3ResourceCompat.SECRET_KEY, "other-secret");
            properties.put(S3ResourceCompat.CONNECTION_TIMEOUT_MS, "999");
            Map<String, String> original = new HashMap<>(properties);
            S3CompatibleFileSystemProperties connector = (S3CompatibleFileSystemProperties) StorageAdapter.of(properties).getSpiProperties();

            S3Resource resource = new S3Resource("gcp_resource");
            resource.setProperties(ImmutableMap.copyOf(properties));
            Map<String, String> normalized = resource.getCopiedProperties();
            Assertions.assertFalse(normalized.keySet().stream().anyMatch(key -> key.startsWith("gs.")));
            Assertions.assertEquals("true", normalized.get("force_parsing_by_standard_uri"));
            TS3StorageParam thrift = S3ThriftAdapter.getS3TStorageParam(normalized);
            Assertions.assertEquals(connector.getEndpoint(), thrift.getEndpoint());
            Assertions.assertEquals(connector.getAccessKey(), thrift.getAk());
            Assertions.assertEquals(connector.getSecretKey(), thrift.getSk());
            Assertions.assertEquals(connector.getSessionToken(), thrift.getToken());
            Assertions.assertEquals(Integer.parseInt(connector.getMaxConnections()), thrift.getMaxConn());
            Assertions.assertEquals(Integer.parseInt(connector.getRequestTimeoutMs()), thrift.getRequestTimeoutMs());
            Assertions.assertEquals(Integer.parseInt(connector.getConnectionTimeoutMs()), thrift.getConnTimeoutMs());
            Assertions.assertEquals(Boolean.parseBoolean(connector.getUsePathStyle()), thrift.isUsePathStyle());
            Assertions.assertEquals(connector.getAccessKey(), CloudObjectStoreAdapter.getObjStoreInfoPB(normalized).getAk());
            Assertions.assertEquals(connector.getSecretKey(), CloudObjectStoreAdapter.getObjStoreInfoPB(normalized).getSk());
            Assertions.assertEquals(thrift.isUsePathStyle(), CloudObjectStoreAdapter.getObjStoreInfoPB(normalized).getUsePathStyle());
            Assertions.assertEquals(original, properties);

            resource.modifyProperties(ImmutableMap.of("gs.secret_key", "new-secret",
                    "gs.connection.timeout", "321", "gs.use_path_style", "false"));
            thrift = S3ThriftAdapter.getS3TStorageParam(resource.getCopiedProperties());
            Assertions.assertEquals("new-secret", thrift.getSk());
            Assertions.assertEquals(321, thrift.getConnTimeoutMs());
            Assertions.assertFalse(thrift.isUsePathStyle());
            resource.modifyProperties(ImmutableMap.of(S3ResourceCompat.SECRET_KEY, "canonical-secret",
                    S3ResourceCompat.SESSION_TOKEN, ""));
            connector = (S3CompatibleFileSystemProperties) StorageAdapter.of(resource.getCopiedProperties()).getSpiProperties();
            Assertions.assertEquals("canonical-secret", connector.getSecretKey());
            Assertions.assertEquals("", connector.getSessionToken());
        }
    }

    @Test
    public void testBlankGsCredentialAndConnectionAliases() throws Exception {
        for (String blank : new String[] {"", " \t"}) {
            Map<String, String> properties = new HashMap<>(s3Properties);
            properties.put("provider", "GCP");
            properties.put("gs.access_key", blank);
            properties.put("gs.secret_key", blank);
            properties.put("gs.session_token", blank);
            properties.put(S3ResourceCompat.SESSION_TOKEN, "canonical-token");
            properties.put("gs.connection.maximum", blank);
            properties.put(S3ResourceCompat.MAX_CONNECTIONS, "123");
            properties.put("gs.use_path_style", blank);
            properties.put(S3ResourceCompat.USE_PATH_STYLE, "true");
            S3Resource resource = new S3Resource("gcp_resource");
            resource.setProperties(ImmutableMap.copyOf(properties));
            resource.modifyProperties(ImmutableMap.of("gs.secret_key", blank, "gs.connection.maximum", blank));
            TS3StorageParam thrift = S3ThriftAdapter.getS3TStorageParam(resource.getCopiedProperties());
            Assertions.assertEquals(s3AccessKey, thrift.getAk());
            Assertions.assertEquals(s3SecretKey, thrift.getSk());
            Assertions.assertEquals("canonical-token", thrift.getToken());
            Assertions.assertEquals(123, thrift.getMaxConn());
            Assertions.assertTrue(thrift.isUsePathStyle());
            Assertions.assertFalse(resource.getCopiedProperties().keySet().stream().anyMatch(key -> key.startsWith("gs.")));
        }
    }

    @Test
    public void testGsAliasesDoNotOverrideOtherProviders() throws Exception {
        for (String provider : new String[] {"S3", "OSS", "AZURE", ""}) {
            Map<String, String> properties = new HashMap<>(s3Properties);
            properties.put("provider", provider);
            properties.put("gs.access_key", "ignored-access");
            properties.put("gs.secret_key", "ignored-secret");
            properties.put("gs.connection.timeout", "789");
            properties.put("gs.use_path_style", "true");
            S3Resource resource = new S3Resource("other_resource");
            resource.setProperties(ImmutableMap.copyOf(properties));
            resource.modifyProperties(ImmutableMap.of("gs.secret_key", "also-ignored",
                    "gs.connection.timeout", "321"));
            TS3StorageParam thrift = S3ThriftAdapter.getS3TStorageParam(resource.getCopiedProperties());
            Assertions.assertEquals(s3AccessKey, thrift.getAk());
            Assertions.assertEquals(s3SecretKey, thrift.getSk());
            Assertions.assertEquals(Integer.parseInt(s3ConnTimeoutMs), thrift.getConnTimeoutMs());
            Assertions.assertFalse(thrift.isUsePathStyle());
        }
    }

    @Test
    public void testAlterClearsGcpImpersonation() throws Exception {
        MetaContext previousContext = MetaContext.get();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();
        try {
            for (String providerType : new String[] {"DEFAULT", "COMPUTE_ENGINE"}) {
                try (MockedStatic<S3Resource> resourceMock = Mockito.mockStatic(
                        S3Resource.class, Mockito.CALLS_REAL_METHODS)) {
                    S3Resource resource = createGcpResourceWithImpersonation(providerType);
                    resource.modifyProperties(ImmutableMap.of(S3ResourceCompat.CONNECTION_TIMEOUT_MS, "2000"));
                    Assertions.assertEquals("reader@project.iam.gserviceaccount.com",
                            resource.getProperty("gs.impersonation_service_account"));
                    AtomicBoolean pinged = new AtomicBoolean();
                    resourceMock.when(() -> S3Resource.pingS3(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
                            .thenAnswer(invocation -> {
                                Map<String, String> pingProperties = invocation.getArgument(2);
                                Assertions.assertEquals("", pingProperties.get("gs.impersonation_service_account"));
                                TGcpCredential credential = S3ThriftAdapter.getS3TStorageParam(pingProperties)
                                        .getCredential().getGcpCredential();
                                Assertions.assertEquals(providerType, credential.getCredentialProviderType().name());
                                Assertions.assertFalse(credential.isSetImpersonationServiceAccount());
                                pinged.set(true);
                                return null;
                            });
                    long previousVersion = resource.getVersion();
                    resource.modifyProperties(ImmutableMap.of("gs.impersonation_service_account", "",
                            S3ResourceCompat.VALIDITY_CHECK, "true"));
                    Assertions.assertTrue(pinged.get());
                    Assertions.assertEquals(previousVersion + 1, resource.getVersion());
                    Assertions.assertEquals("", resource.getProperty("gs.impersonation_service_account"));

                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    try (DataOutputStream output = new DataOutputStream(bytes)) {
                        resource.write(output);
                    }
                    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
                        S3Resource restored = (S3Resource) Resource.read(input);
                        Assertions.assertEquals(resource.getCopiedProperties(), restored.getCopiedProperties());
                        TGcpCredential credential = S3ThriftAdapter.getS3TStorageParam(restored.getCopiedProperties())
                                .getCredential().getGcpCredential();
                        Assertions.assertEquals(providerType, credential.getCredentialProviderType().name());
                        Assertions.assertFalse(credential.isSetImpersonationServiceAccount());
                    }
                }
            }
        } finally {
            if (previousContext == null) {
                MetaContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testFailedAlterPreservesGcpImpersonation() throws Exception {
        try (MockedStatic<S3Resource> resourceMock = Mockito.mockStatic(S3Resource.class, Mockito.CALLS_REAL_METHODS)) {
            resourceMock.when(() -> S3Resource.pingS3(Mockito.anyString(), Mockito.anyString(), Mockito.anyMap()))
                    .thenAnswer(invocation -> {
                        Map<String, String> pingProperties = invocation.getArgument(2);
                        Assertions.assertEquals("", pingProperties.get("gs.impersonation_service_account"));
                        throw new DdlException("source credential cannot access bucket");
                    });
            for (String providerType : new String[] {"DEFAULT", "COMPUTE_ENGINE"}) {
                S3Resource resource = createGcpResourceWithImpersonation(providerType);
                Map<String, String> previousProperties = resource.getCopiedProperties();
                long previousVersion = resource.getVersion();
                Assertions.assertThrows(DdlException.class, () -> resource.modifyProperties(
                        ImmutableMap.of("gs.impersonation_service_account", "",
                                S3ResourceCompat.VALIDITY_CHECK, "true")));
                Assertions.assertEquals(previousProperties, resource.getCopiedProperties());
                Assertions.assertEquals(previousVersion, resource.getVersion());
            }
        }
    }

    private S3Resource createGcpResourceWithImpersonation(String providerType) throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put("provider", "GCP");
        properties.put(S3ResourceCompat.ENDPOINT, "https://storage.googleapis.com");
        properties.put(S3ResourceCompat.REGION, "us-central1");
        properties.put(S3ResourceCompat.BUCKET, "bucket");
        properties.put(S3ResourceCompat.ROOT_PATH, "prefix");
        properties.put(S3ResourceCompat.VALIDITY_CHECK, "false");
        properties.put("gs.credential_provider_type", providerType);
        properties.put("gs.impersonation_service_account", "reader@project.iam.gserviceaccount.com");
        S3Resource resource = new S3Resource("gcp_resource");
        resource.setProperties(ImmutableMap.copyOf(properties));
        return resource;
    }

    @Test
    public void testGsCredentialAliasesCannotBypassNativeAuthValidation() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("provider", "GCP");
        properties.put("gs.endpoint", "https://storage.googleapis.com");
        properties.put(S3ResourceCompat.BUCKET, "bucket");
        properties.put(S3ResourceCompat.VALIDITY_CHECK, "false");
        properties.put("gs.credential_provider_type", "DEFAULT");
        for (String key : new String[] {"gs.access_key", "gs.secret_key", "gs.session_token",
                S3ResourceCompat.ACCESS_KEY, S3ResourceCompat.SECRET_KEY, S3ResourceCompat.SESSION_TOKEN}) {
            S3Resource resource = new S3Resource("gcp_resource");
            Map<String, String> conflicting = new HashMap<>(properties);
            conflicting.put(key, "conflicting-credential");
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> resource.setProperties(ImmutableMap.copyOf(conflicting)));

            resource.setProperties(ImmutableMap.copyOf(properties));
            Map<String, String> before = resource.getCopiedProperties();
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> resource.modifyProperties(ImmutableMap.of(key, "conflicting-credential")));
            Assertions.assertEquals(before, resource.getCopiedProperties());
            Assertions.assertEquals("DEFAULT", resource.getProperty("gs.credential_provider_type"));
        }
        S3Resource resource = new S3Resource("gcp_resource");
        properties.remove("gs.credential_provider_type");
        properties.put("gs.access_key", "access");
        properties.put("gs.secret_key", "secret");
        resource.setProperties(ImmutableMap.copyOf(properties));
        Assertions.assertThrows(IllegalArgumentException.class, () -> resource.modifyProperties(
                ImmutableMap.of("gs.credential_provider_type", "DEFAULT")));
        // Empty AK/SK updates are ignored by Resource ALTER and cannot clear stored credentials.
        Assertions.assertThrows(IllegalArgumentException.class, () -> resource.modifyProperties(
                ImmutableMap.of("gs.credential_provider_type", "DEFAULT",
                        S3ResourceCompat.ACCESS_KEY, "", S3ResourceCompat.SECRET_KEY, "")));
    }

    @Test
    public void testAlterOverridesStoredGsAliases() throws Exception {
        Map<String, String> properties = new HashMap<>(s3Properties);
        properties.put("provider", "GCP");
        S3Resource resource = new S3Resource("gcp_resource");
        resource.setProperties(ImmutableMap.copyOf(properties));
        // Simulate a resource persisted before gs.* normalization was introduced.
        Field field = S3Resource.class.getDeclaredField("properties");
        field.setAccessible(true);
        Map<String, String> legacy = resource.getCopiedProperties();
        legacy.put("gs.access_key", "old-access");
        legacy.put("gs.secret_key", "old-secret");
        legacy.put("gs.session_token", "old-token");
        legacy.put("gs.connection.timeout", "789");
        field.set(resource, legacy);
        resource.modifyProperties(ImmutableMap.of(S3ResourceCompat.ACCESS_KEY, "new-access",
                S3ResourceCompat.SECRET_KEY, "new-secret",
                S3ResourceCompat.SESSION_TOKEN, ""));
        Map<String, String> normalized = resource.getCopiedProperties();
        S3CompatibleFileSystemProperties connector = (S3CompatibleFileSystemProperties) StorageAdapter.of(normalized).getSpiProperties();
        Assertions.assertEquals("new-access", connector.getAccessKey());
        Assertions.assertEquals("new-secret", connector.getSecretKey());
        Assertions.assertEquals("", connector.getSessionToken());
        Assertions.assertEquals(connector.getAccessKey(), S3ThriftAdapter.getS3TStorageParam(normalized).getAk());
        Assertions.assertEquals(connector.getSecretKey(), S3ThriftAdapter.getS3TStorageParam(normalized).getSk());
        Assertions.assertEquals(connector.getSessionToken(), S3ThriftAdapter.getS3TStorageParam(normalized).getToken());
        Assertions.assertEquals(connector.getSecretKey(), CloudObjectStoreAdapter.getObjStoreInfoPB(normalized).getSk());
        Assertions.assertEquals(789, S3ThriftAdapter.getS3TStorageParam(normalized).getConnTimeoutMs());
        Assertions.assertFalse(normalized.containsKey("gs.access_key"));
        Assertions.assertFalse(normalized.containsKey("gs.secret_key"));
        Assertions.assertFalse(normalized.containsKey("gs.session_token"));
        Assertions.assertFalse(normalized.containsKey("gs.connection.timeout"));

        // Within the same ALTER input, gs.* still takes precedence over s3.*.
        resource.modifyProperties(ImmutableMap.of("gs.secret_key", "updated-gs-secret",
                S3ResourceCompat.SECRET_KEY, "ignored-s3-secret"));
        Assertions.assertEquals("updated-gs-secret", resource.getProperty(S3ResourceCompat.SECRET_KEY));
    }

    @Test
    public void testBlankStoredGsAliasesFallBackToAlterValues() throws Exception {
        for (String blank : new String[] {"", " \t"}) {
            Map<String, String> properties = new HashMap<>(s3Properties);
            properties.put("provider", "GCP");
            S3Resource resource = new S3Resource("gcp_resource");
            resource.setProperties(ImmutableMap.copyOf(properties));
            Field field = S3Resource.class.getDeclaredField("properties");
            field.setAccessible(true);
            Map<String, String> legacy = resource.getCopiedProperties();
            legacy.put("gs.secret_key", blank);
            legacy.put("gs.session_token", blank);
            field.set(resource, legacy);

            resource.modifyProperties(ImmutableMap.of(S3ResourceCompat.SECRET_KEY, "new-secret",
                    S3ResourceCompat.SESSION_TOKEN, ""));
            Map<String, String> normalized = resource.getCopiedProperties();
            S3CompatibleFileSystemProperties connector = (S3CompatibleFileSystemProperties) StorageAdapter.of(normalized).getSpiProperties();
            Assertions.assertEquals("new-secret", connector.getSecretKey());
            Assertions.assertEquals("", connector.getSessionToken());
            Assertions.assertEquals(connector.getSecretKey(), S3ThriftAdapter.getS3TStorageParam(normalized).getSk());
            Assertions.assertFalse(normalized.containsKey("gs.secret_key"));
            Assertions.assertFalse(normalized.containsKey("gs.session_token"));
        }
    }

    @Test
    public void testStoredGsEndpointCannotChangePolicyResourceOnAlter() throws Exception {
        Map<String, String> properties = new HashMap<>(s3Properties);
        properties.put("provider", "GCP");
        S3Resource resource = new S3Resource("gcp_resource");
        resource.setProperties(ImmutableMap.copyOf(properties));
        resource.references.put("policy", Resource.ReferenceType.POLICY);
        Field field = S3Resource.class.getDeclaredField("properties");
        field.setAccessible(true);
        Map<String, String> legacy = resource.getCopiedProperties();
        legacy.put("gs.endpoint", "https://different.example.com");
        field.set(resource, legacy);

        Assertions.assertThrows(DdlException.class, () -> resource.modifyProperties(
                ImmutableMap.of(S3ResourceCompat.SECRET_KEY, "new-secret")));
        Assertions.assertEquals(legacy, resource.getCopiedProperties());
    }
}
