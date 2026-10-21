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
import org.apache.doris.datasource.property.storage.GCSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TS3StorageParam;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

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

    @Before
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
    public void testFromStmt(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // resource with default settings
        CreateResourceCommand createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
        createResourceCommand.getInfo().validate();

        S3Resource s3Resource = (S3Resource) Resource.fromCommand(createResourceCommand);
        Assert.assertEquals(name, s3Resource.getName());
        Assert.assertEquals(type, s3Resource.getType().name().toLowerCase());
        Assert.assertEquals(s3Endpoint, s3Resource.getProperty(S3Properties.ENDPOINT));
        Assert.assertEquals(s3Region, s3Resource.getProperty(S3Properties.REGION));
        Assert.assertEquals(s3RootPath, s3Resource.getProperty(S3Properties.ROOT_PATH));
        Assert.assertEquals(s3AccessKey, s3Resource.getProperty(S3Properties.ACCESS_KEY));
        Assert.assertEquals(s3SecretKey, s3Resource.getProperty(S3Properties.SECRET_KEY));
        Assert.assertEquals(s3MaxConnections, s3Resource.getProperty(S3Properties.MAX_CONNECTIONS));
        Assert.assertEquals(s3ReqTimeoutMs, s3Resource.getProperty(S3Properties.REQUEST_TIMEOUT_MS));
        Assert.assertEquals(s3ConnTimeoutMs, s3Resource.getProperty(S3Properties.CONNECTION_TIMEOUT_MS));

        // with no default settings
        s3Properties.put(S3Properties.MAX_CONNECTIONS, "100");
        s3Properties.put(S3Properties.REQUEST_TIMEOUT_MS, "2000");
        s3Properties.put(S3Properties.CONNECTION_TIMEOUT_MS, "2000");
        s3Properties.put(S3Properties.VALIDITY_CHECK, "false");

        createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
        createResourceCommand.getInfo().validate();

        s3Resource = (S3Resource) Resource.fromCommand(createResourceCommand);
        Assert.assertEquals(name, s3Resource.getName());
        Assert.assertEquals(type, s3Resource.getType().name().toLowerCase());
        Assert.assertEquals(s3Endpoint, s3Resource.getProperty(S3Properties.ENDPOINT));
        Assert.assertEquals(s3Region, s3Resource.getProperty(S3Properties.REGION));
        Assert.assertEquals(s3RootPath, s3Resource.getProperty(S3Properties.ROOT_PATH));
        Assert.assertEquals(s3AccessKey, s3Resource.getProperty(S3Properties.ACCESS_KEY));
        Assert.assertEquals(s3SecretKey, s3Resource.getProperty(S3Properties.SECRET_KEY));
        Assert.assertEquals("100", s3Resource.getProperty(S3Properties.MAX_CONNECTIONS));
        Assert.assertEquals("2000", s3Resource.getProperty(S3Properties.REQUEST_TIMEOUT_MS));
        Assert.assertEquals("2000", s3Resource.getProperty(S3Properties.CONNECTION_TIMEOUT_MS));
    }

    @Test(expected = DdlException.class)
    public void testAbnormalResource(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };
        s3Properties.remove("AWS_ENDPOINT");

        CreateResourceCommand createResourceCommand = new CreateResourceCommand(new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(s3Properties)));
        createResourceCommand.getInfo().validate();

        Resource.fromCommand(createResourceCommand);
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

        Assert.assertEquals("s3_1", rS3Resource1.getName());
        Assert.assertEquals("s3_2", rS3Resource2.getName());

        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.ENDPOINT), "aaa");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.Env.ENDPOINT), "aaa");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.REGION), "bbb");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.ROOT_PATH), "/path/to/root");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.ACCESS_KEY), "xxx");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.SECRET_KEY), "yyy");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.MAX_CONNECTIONS), "50");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.REQUEST_TIMEOUT_MS), "3000");
        Assert.assertEquals(rS3Resource2.getProperty(S3Properties.CONNECTION_TIMEOUT_MS), "1000");

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
                        Assert.assertTrue(releaseFirst.await(10, TimeUnit.SECONDS));
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
                resource.modifyProperties(ImmutableMap.of(S3Properties.MAX_CONNECTIONS, "77"));
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        Thread updateTimeout = new Thread(() -> {
            secondStarted.countDown();
            try {
                resource.modifyProperties(ImmutableMap.of(S3Properties.REQUEST_TIMEOUT_MS, "8888"));
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        try {
            updateConnections.start();
            Assert.assertTrue(firstReady.await(10, TimeUnit.SECONDS));
            updateTimeout.start();
            Assert.assertTrue(secondStarted.await(10, TimeUnit.SECONDS));
            // The second ALTER either waits for the first one's monitor, or (before the fix)
            // finishes and is subsequently overwritten by the first one's stale snapshot.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (updateTimeout.isAlive() && updateTimeout.getState() != Thread.State.BLOCKED
                    && System.nanoTime() < deadline) {
                Thread.yield();
            }
            Assert.assertTrue(!updateTimeout.isAlive() || updateTimeout.getState() == Thread.State.BLOCKED);
        } finally {
            releaseFirst.countDown();
            updateConnections.join(10000);
            updateTimeout.join(10000);
        }
        Assert.assertFalse(updateConnections.isAlive());
        Assert.assertFalse(updateTimeout.isAlive());
        Assert.assertNull(failure.get());
        Assert.assertEquals("77", resource.getProperty(S3Properties.MAX_CONNECTIONS));
        Assert.assertEquals("8888", resource.getProperty(S3Properties.REQUEST_TIMEOUT_MS));
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
        modify.put(S3Properties.ENDPOINT, "new-endpoint");
        s3Resource.modifyProperties(modify);
        Assert.assertEquals("new-endpoint", s3Resource.getProperty(S3Properties.ENDPOINT));
        Assert.assertEquals("new-endpoint", s3Resource.getProperty(S3Properties.Env.ENDPOINT));

        modify.clear();
        modify.put(S3Properties.Env.ENDPOINT, "http://other-endpoint");
        s3Resource.modifyProperties(modify);
        Assert.assertEquals("http://other-endpoint", s3Resource.getProperty(S3Properties.ENDPOINT));
        Assert.assertEquals("http://other-endpoint", s3Resource.getProperty(S3Properties.Env.ENDPOINT));
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
        Assert.assertEquals(s3Resource.getProperty(S3Properties.ENDPOINT), "https://aaa");
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

            Assume.assumeTrue("ACCESS_KEY isNullOrEmpty.", !Strings.isNullOrEmpty(accessKey));
            Assume.assumeTrue("SECRET_KEY isNullOrEmpty.", !Strings.isNullOrEmpty(secretKey));
            Assume.assumeTrue("BUCKET isNullOrEmpty.", !Strings.isNullOrEmpty(bucket));
            Assume.assumeTrue("ENDPOINT isNullOrEmpty.", !Strings.isNullOrEmpty(endpoint));
            Assume.assumeTrue("REGION isNullOrEmpty.", !Strings.isNullOrEmpty(region));
            Assume.assumeTrue("PROVIDER isNullOrEmpty.", !Strings.isNullOrEmpty(provider));

            Map<String, String> properties = new HashMap<>();
            properties.put("s3.endpoint", endpoint);
            properties.put("s3.region", region);
            properties.put("s3.access_key", accessKey);
            properties.put("s3.secret_key", secretKey);
            properties.put("provider", provider);
            S3Resource.pingS3(bucket, "fe_ut_prefix", properties);
        } catch (DdlException e) {
            LOG.info("testPingS3 exception:", e);
            Assert.assertTrue(e.getMessage(), false);
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

            Assume.assumeTrue("ENDPOINT isNullOrEmpty.", !Strings.isNullOrEmpty(endpoint));
            Assume.assumeTrue("REGION isNullOrEmpty.", !Strings.isNullOrEmpty(region));
            Assume.assumeTrue("PROVIDER isNullOrEmpty.", !Strings.isNullOrEmpty(provider));
            Assume.assumeTrue("ROLE_ARN isNullOrEmpty.", !Strings.isNullOrEmpty(roleArn));
            Assume.assumeTrue("EXTERNAL_ID isNullOrEmpty.", !Strings.isNullOrEmpty(externalId));
            Assume.assumeTrue("BUCKET isNullOrEmpty.", !Strings.isNullOrEmpty(bucket));

            Map<String, String> properties = new HashMap<>();
            properties.put("s3.endpoint", endpoint);
            properties.put("s3.region", region);
            properties.put("s3.role_arn", roleArn);
            properties.put("s3.external_id", externalId);
            properties.put("provider", provider);
            S3Resource.pingS3(bucket, "fe_ut_role_prefix", properties);
        } catch (DdlException e) {
            LOG.info("testPingS3WithRoleArn exception:", e);
            Assert.assertTrue(e.getMessage(), false);
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
        Assert.assertEquals("us-central1", resource.getProperty(S3Properties.REGION));
        Assert.assertEquals("bucket", resource.getProperty(S3Properties.BUCKET));
        Assert.assertEquals("prefix", resource.getProperty(S3Properties.ROOT_PATH));
        Assert.assertEquals("https://storage.googleapis.com", resource.getProperty(S3Properties.ENDPOINT));
        Assert.assertEquals("https://storage.googleapis.com",
                S3Properties.getS3TStorageParam(resource.getCopiedProperties()).getEndpoint());
        Assert.assertFalse(resource.getCopiedProperties().containsKey("gs.endpoint"));
        resource.modifyProperties(ImmutableMap.of("gs.endpoint", "https://custom.example.com",
                "s3_validity_check", "false"));
        Assert.assertEquals("https://custom.example.com", resource.getProperty(S3Properties.ENDPOINT));
        resource.modifyProperties(ImmutableMap.of("s3.endpoint", "https://storage.googleapis.com",
                "s3_validity_check", "false"));
        Assert.assertEquals("https://storage.googleapis.com", resource.getProperty(S3Properties.ENDPOINT));
        Assert.assertEquals("https://storage.googleapis.com", properties.get("gs.endpoint"));
        Assert.assertEquals("https://old.example.com", properties.get("s3.endpoint"));
    }

    @Test
    public void testGsEndpointForStorageVault(@Mocked Env env) throws Exception {
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
            Assert.assertEquals("prefix", vault.getCopiedProperties().get(S3Properties.ROOT_PATH));
            String expectedEndpoint = "GCP".equalsIgnoreCase(provider)
                    ? "https://storage.googleapis.com" : s3Endpoint;
            Assert.assertEquals(expectedEndpoint,
                    S3Properties.getObjStoreInfoPB(vault.getCopiedProperties()).getEndpoint());
            Assert.assertEquals("GCP".equalsIgnoreCase(provider) ? "gcp-access" : s3AccessKey,
                    S3Properties.getObjStoreInfoPB(vault.getCopiedProperties()).getAk());
            Assert.assertEquals("GCP".equalsIgnoreCase(provider) ? "gcp-secret" : s3SecretKey,
                    S3Properties.getObjStoreInfoPB(vault.getCopiedProperties()).getSk());
            Assert.assertEquals("GCP".equalsIgnoreCase(provider),
                    S3Properties.getObjStoreInfoPB(vault.getCopiedProperties()).getUsePathStyle());
        }
    }

    @Test
    public void testStorageVaultRejectsNonAllowlistedAliases() throws Exception {
        StorageVaultMgr manager = new StorageVaultMgr(null);
        for (String key : new String[] {"gs.endpoint", S3Properties.ENDPOINT, S3Properties.Env.ENDPOINT,
                "gs.access_key", "gs.secret_key", "gs.session_token", "gs.connection.maximum",
                "gs.connection.request.timeout", "gs.connection.timeout", "gs.use_path_style",
                "gs.force_parsing_by_standard_uri"}) {
            try {
                manager.alterStorageVault(StorageVault.StorageVaultType.S3,
                        ImmutableMap.of(key, "https://custom.example.com"), "gcp_vault");
                Assert.fail("Storage Vault must reject properties outside its ALTER whitelist: " + key);
            } catch (IllegalArgumentException exception) {
                Assert.assertEquals("Alter property " + key + " is not allowed.", exception.getMessage());
            }
        }
    }

    @Test
    public void testBlankGsEndpointPreservesResourceAliases() throws Exception {
        for (String blank : new String[] {"", " \t"}) {
            for (String endpointKey : new String[] {S3Properties.ENDPOINT, S3Properties.Env.ENDPOINT}) {
                Map<String, String> properties = new HashMap<>(s3Properties);
                properties.put("provider", "GCP");
                properties.remove(S3Properties.Env.ENDPOINT);
                properties.put(endpointKey, s3Endpoint);
                properties.put("gs.endpoint", blank);
                properties.put("gs.connection.timeout", blank);
                S3Resource resource = new S3Resource("gcp_resource");
                resource.setProperties(ImmutableMap.copyOf(properties));
                Assert.assertEquals(s3Endpoint, resource.getProperty(S3Properties.ENDPOINT));
                Assert.assertFalse(resource.getCopiedProperties().containsKey("gs.endpoint"));
                Assert.assertEquals(Integer.parseInt(s3ConnTimeoutMs),
                        S3Properties.getS3TStorageParam(resource.getCopiedProperties()).getConnTimeoutMs());
                Map<String, String> updated = new HashMap<>();
                updated.put("gs.endpoint", blank);
                updated.put("s3_validity_check", "false");
                resource.modifyProperties(updated);
                Assert.assertEquals(s3Endpoint, resource.getProperty(S3Properties.ENDPOINT));
                Assert.assertEquals(blank, updated.get("gs.endpoint"));
                Assert.assertFalse(resource.getCopiedProperties().containsKey("gs.connection.timeout"));
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
            Assert.fail("A gs.endpoint alias must not bypass the policy endpoint restriction");
        } catch (DdlException exception) {
            Assert.assertTrue(exception.getMessage().contains(S3Properties.ENDPOINT));
        }
        Assert.assertEquals(s3Endpoint, resource.getProperty(S3Properties.ENDPOINT));
    }

    @Test
    public void testGsEndpointDoesNotOverrideOtherProviders() throws Exception {
        for (String provider : new String[] {"S3", "OSS", "AZURE", ""}) {
            Map<String, String> properties = new HashMap<>(s3Properties);
            if (!provider.isEmpty()) {
                properties.put("provider", provider);
            }
            properties.put("gs.endpoint", "https://storage.googleapis.com");
            S3Resource resource = new S3Resource("other_resource");
            resource.setProperties(ImmutableMap.copyOf(properties));
            Assert.assertEquals(s3Endpoint, resource.getProperty(S3Properties.ENDPOINT));
            resource.modifyProperties(ImmutableMap.of("gs.endpoint", "https://custom.example.com"));
            Assert.assertEquals(s3Endpoint, resource.getProperty(S3Properties.ENDPOINT));
            Assert.assertEquals(s3Endpoint, resource.getProperty(S3Properties.Env.ENDPOINT));
        }
    }

    @Test
    public void testGsEndpointUsesEffectiveProviderOnAlter() throws Exception {
        S3Resource resource = new S3Resource("gcp_resource");
        Map<String, String> properties = new HashMap<>(s3Properties);
        properties.put("provider", "GCP");
        resource.setProperties(ImmutableMap.copyOf(properties));
        resource.modifyProperties(ImmutableMap.of("provider", "S3", "gs.endpoint", "https://ignored.example.com"));
        Assert.assertEquals("S3", resource.getProperty("provider"));
        Assert.assertEquals(s3Endpoint, resource.getProperty(S3Properties.ENDPOINT));
        resource.modifyProperties(ImmutableMap.of("provider", "gcp", "gs.endpoint", "https://storage.googleapis.com"));
        Assert.assertEquals("gcp", resource.getProperty("provider"));
        Assert.assertEquals("https://storage.googleapis.com", resource.getProperty(S3Properties.ENDPOINT));
        resource.modifyProperties(ImmutableMap.of("provider", "", "gs.endpoint", "https://custom.example.com"));
        Assert.assertEquals("gcp", resource.getProperty("provider"));
        Assert.assertEquals("https://custom.example.com", resource.getProperty(S3Properties.ENDPOINT));
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
            properties.put(S3Properties.SECRET_KEY, "other-secret");
            properties.put(S3Properties.CONNECTION_TIMEOUT_MS, "999");
            Map<String, String> original = new HashMap<>(properties);
            GCSProperties connector = (GCSProperties) StorageProperties.createPrimary(properties);

            S3Resource resource = new S3Resource("gcp_resource");
            resource.setProperties(ImmutableMap.copyOf(properties));
            Map<String, String> normalized = resource.getCopiedProperties();
            Assert.assertFalse(normalized.keySet().stream().anyMatch(key -> key.startsWith("gs.")));
            Assert.assertEquals("true", normalized.get("force_parsing_by_standard_uri"));
            TS3StorageParam thrift = S3Properties.getS3TStorageParam(normalized);
            Assert.assertEquals(connector.getEndpoint(), thrift.getEndpoint());
            Assert.assertEquals(connector.getAccessKey(), thrift.getAk());
            Assert.assertEquals(connector.getSecretKey(), thrift.getSk());
            Assert.assertEquals(connector.getSessionToken(), thrift.getToken());
            Assert.assertEquals(Integer.parseInt(connector.getMaxConnections()), thrift.getMaxConn());
            Assert.assertEquals(Integer.parseInt(connector.getRequestTimeoutS()), thrift.getRequestTimeoutMs());
            Assert.assertEquals(Integer.parseInt(connector.getConnectionTimeoutS()), thrift.getConnTimeoutMs());
            Assert.assertEquals(Boolean.parseBoolean(connector.getUsePathStyle()), thrift.isUsePathStyle());
            Assert.assertEquals(connector.getAccessKey(), S3Properties.getObjStoreInfoPB(normalized).getAk());
            Assert.assertEquals(connector.getSecretKey(), S3Properties.getObjStoreInfoPB(normalized).getSk());
            Assert.assertEquals(thrift.isUsePathStyle(), S3Properties.getObjStoreInfoPB(normalized).getUsePathStyle());
            Assert.assertEquals(original, properties);

            resource.modifyProperties(ImmutableMap.of("gs.secret_key", "new-secret",
                    "gs.connection.timeout", "321", "gs.use_path_style", "false"));
            thrift = S3Properties.getS3TStorageParam(resource.getCopiedProperties());
            Assert.assertEquals("new-secret", thrift.getSk());
            Assert.assertEquals(321, thrift.getConnTimeoutMs());
            Assert.assertFalse(thrift.isUsePathStyle());
            resource.modifyProperties(ImmutableMap.of(S3Properties.SECRET_KEY, "canonical-secret",
                    S3Properties.SESSION_TOKEN, ""));
            connector = (GCSProperties) StorageProperties.createPrimary(resource.getCopiedProperties());
            Assert.assertEquals("canonical-secret", connector.getSecretKey());
            Assert.assertEquals("", connector.getSessionToken());
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
            properties.put(S3Properties.SESSION_TOKEN, "canonical-token");
            properties.put("gs.connection.maximum", blank);
            properties.put(S3Properties.MAX_CONNECTIONS, "123");
            properties.put("gs.use_path_style", blank);
            properties.put(S3Properties.USE_PATH_STYLE, "true");
            S3Resource resource = new S3Resource("gcp_resource");
            resource.setProperties(ImmutableMap.copyOf(properties));
            resource.modifyProperties(ImmutableMap.of("gs.secret_key", blank, "gs.connection.maximum", blank));
            TS3StorageParam thrift = S3Properties.getS3TStorageParam(resource.getCopiedProperties());
            Assert.assertEquals(s3AccessKey, thrift.getAk());
            Assert.assertEquals(s3SecretKey, thrift.getSk());
            Assert.assertEquals("canonical-token", thrift.getToken());
            Assert.assertEquals(123, thrift.getMaxConn());
            Assert.assertTrue(thrift.isUsePathStyle());
            Assert.assertFalse(resource.getCopiedProperties().keySet().stream().anyMatch(key -> key.startsWith("gs.")));
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
            TS3StorageParam thrift = S3Properties.getS3TStorageParam(resource.getCopiedProperties());
            Assert.assertEquals(s3AccessKey, thrift.getAk());
            Assert.assertEquals(s3SecretKey, thrift.getSk());
            Assert.assertEquals(Integer.parseInt(s3ConnTimeoutMs), thrift.getConnTimeoutMs());
            Assert.assertFalse(thrift.isUsePathStyle());
        }
    }

    @Test
    public void testGsCredentialAliasesCannotBypassNativeAuthValidation() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("provider", "GCP");
        properties.put("gs.endpoint", "https://storage.googleapis.com");
        properties.put(S3Properties.BUCKET, "bucket");
        properties.put(S3Properties.VALIDITY_CHECK, "false");
        properties.put(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT");
        for (String key : new String[] {"gs.access_key", "gs.secret_key", "gs.session_token",
                S3Properties.ACCESS_KEY, S3Properties.SECRET_KEY, S3Properties.SESSION_TOKEN}) {
            S3Resource resource = new S3Resource("gcp_resource");
            Map<String, String> conflicting = new HashMap<>(properties);
            conflicting.put(key, "conflicting-credential");
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> resource.setProperties(ImmutableMap.copyOf(conflicting)));

            resource.setProperties(ImmutableMap.copyOf(properties));
            Map<String, String> before = resource.getCopiedProperties();
            Assert.assertThrows(IllegalArgumentException.class,
                    () -> resource.modifyProperties(ImmutableMap.of(key, "conflicting-credential")));
            Assert.assertEquals(before, resource.getCopiedProperties());
            Assert.assertEquals("DEFAULT", resource.getProperty(GCSProperties.CREDENTIAL_PROVIDER_TYPE));
        }
        S3Resource resource = new S3Resource("gcp_resource");
        properties.remove(GCSProperties.CREDENTIAL_PROVIDER_TYPE);
        properties.put("gs.access_key", "access");
        properties.put("gs.secret_key", "secret");
        resource.setProperties(ImmutableMap.copyOf(properties));
        Assert.assertThrows(IllegalArgumentException.class, () -> resource.modifyProperties(
                ImmutableMap.of(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT")));
        // Empty AK/SK updates are ignored by Resource ALTER and cannot clear stored credentials.
        Assert.assertThrows(IllegalArgumentException.class, () -> resource.modifyProperties(
                ImmutableMap.of(GCSProperties.CREDENTIAL_PROVIDER_TYPE, "DEFAULT",
                        S3Properties.ACCESS_KEY, "", S3Properties.SECRET_KEY, "")));
    }

    @Test
    public void testGsAliasesTakePrecedenceAfterMergingAlter() throws Exception {
        Map<String, String> properties = new HashMap<>(s3Properties);
        properties.put("provider", "GCP");
        S3Resource resource = new S3Resource("gcp_resource");
        resource.setProperties(ImmutableMap.copyOf(properties));
        // Simulate a resource persisted before gs.* normalization was introduced.
        Field field = S3Resource.class.getDeclaredField("properties");
        field.setAccessible(true);
        Map<String, String> legacy = resource.getCopiedProperties();
        legacy.put("gs.secret_key", "old-secret");
        legacy.put("gs.session_token", "old-token");
        legacy.put("gs.connection.timeout", "789");
        field.set(resource, legacy);
        resource.modifyProperties(ImmutableMap.of(S3Properties.SECRET_KEY, "new-secret",
                S3Properties.SESSION_TOKEN, ""));
        Map<String, String> normalized = resource.getCopiedProperties();
        GCSProperties connector = (GCSProperties) StorageProperties.createPrimary(normalized);
        Assert.assertEquals("old-secret", connector.getSecretKey());
        Assert.assertEquals("old-token", connector.getSessionToken());
        Assert.assertEquals(connector.getSecretKey(), S3Properties.getS3TStorageParam(normalized).getSk());
        Assert.assertEquals(connector.getSessionToken(), S3Properties.getS3TStorageParam(normalized).getToken());
        Assert.assertEquals(connector.getSecretKey(), S3Properties.getObjStoreInfoPB(normalized).getSk());
        Assert.assertEquals(789, S3Properties.getS3TStorageParam(normalized).getConnTimeoutMs());
        Assert.assertFalse(normalized.containsKey("gs.secret_key"));
        Assert.assertFalse(normalized.containsKey("gs.session_token"));
        Assert.assertFalse(normalized.containsKey("gs.connection.timeout"));

        // Within the same ALTER input, gs.* also takes precedence over s3.*.
        resource.modifyProperties(ImmutableMap.of("gs.secret_key", "updated-gs-secret",
                S3Properties.SECRET_KEY, "ignored-s3-secret"));
        Assert.assertEquals("updated-gs-secret", resource.getProperty(S3Properties.SECRET_KEY));
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

            resource.modifyProperties(ImmutableMap.of(S3Properties.SECRET_KEY, "new-secret",
                    S3Properties.SESSION_TOKEN, ""));
            Map<String, String> normalized = resource.getCopiedProperties();
            GCSProperties connector = (GCSProperties) StorageProperties.createPrimary(normalized);
            Assert.assertEquals("new-secret", connector.getSecretKey());
            Assert.assertEquals("", connector.getSessionToken());
            Assert.assertEquals(connector.getSecretKey(), S3Properties.getS3TStorageParam(normalized).getSk());
            Assert.assertFalse(normalized.containsKey("gs.secret_key"));
            Assert.assertFalse(normalized.containsKey("gs.session_token"));
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

        Assert.assertThrows(DdlException.class, () -> resource.modifyProperties(
                ImmutableMap.of(S3Properties.SECRET_KEY, "new-secret")));
        Assert.assertEquals(legacy, resource.getCopiedProperties());
    }
}
