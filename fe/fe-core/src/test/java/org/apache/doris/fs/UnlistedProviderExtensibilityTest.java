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

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.S3CompatibleFileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

/**
 * Guards the out-of-tree plugin contract: a provider that is NOT in
 * {@code StorageRegistry.Provider} must be routable by bindPrimary/bindAll (appended after the
 * known set), must never preempt the built-in fourteen, and an S3-compatible one must join
 * the S3 family id so TypeId-dispatched consumers need no code change.
 */
public class UnlistedProviderExtensibilityTest {

    private static FileSystemPluginManager manager;

    /** Minimal S3-compatible typed props for a hypothetical out-of-tree "XYZ" object store. */
    private static final class XyzProps implements S3CompatibleFileSystemProperties {
        private final Map<String, String> raw;

        XyzProps(Map<String, String> raw) {
            this.raw = raw;
        }

        @Override
        public String providerName() {
            return "XYZ";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.OBJECT_STORAGE;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.S3;
        }

        @Override
        public Map<String, String> rawProperties() {
            return raw;
        }

        @Override
        public Map<String, String> matchedProperties() {
            return raw;
        }

        @Override
        public Set<String> getSupportedSchemes() {
            return ImmutableSet.of("s3", "xyz");
        }

        @Override
        public Set<String> legacyCacheSchemes() {
            return ImmutableSet.of("s3");
        }

        @Override
        public String getEndpoint() {
            return raw.get("xyz.endpoint");
        }

        @Override
        public String getRegion() {
            return raw.get("xyz.region");
        }

        @Override
        public String getAccessKey() {
            return raw.get("xyz.access_key");
        }

        @Override
        public String getSecretKey() {
            return raw.get("xyz.secret_key");
        }

        @Override
        public String getSessionToken() {
            return null;
        }

        @Override
        public String getRoleArn() {
            return null;
        }

        @Override
        public String getExternalId() {
            return null;
        }

        @Override
        public String getBucket() {
            return null;
        }

        @Override
        public String getRootPath() {
            return null;
        }

        @Override
        public String getMaxConnections() {
            return "50";
        }

        @Override
        public String getRequestTimeoutMs() {
            return "3000";
        }

        @Override
        public String getConnectionTimeoutMs() {
            return "1000";
        }

        @Override
        public String getUsePathStyle() {
            return "false";
        }
    }

    private static final class XyzProvider implements FileSystemProvider<XyzProps> {
        @Override
        public String name() {
            return "XYZ";
        }

        @Override
        public boolean supports(Map<String, String> properties) {
            return supportsExplicit(properties) || supportsGuess(properties);
        }

        @Override
        public boolean supportsExplicit(Map<String, String> properties) {
            return Boolean.parseBoolean(properties.getOrDefault("fs.xyz.support", "false"));
        }

        @Override
        public boolean supportsGuess(Map<String, String> properties) {
            String endpoint = properties.get("xyz.endpoint");
            return endpoint != null && endpoint.contains("xyz.example.com");
        }

        @Override
        public XyzProps bind(Map<String, String> properties) {
            return new XyzProps(properties);
        }

        @Override
        public FileSystem create(XyzProps properties) {
            throw new UnsupportedOperationException("not needed for routing tests");
        }

        @Override
        public FileSystem create(Map<String, String> properties) {
            throw new UnsupportedOperationException("not needed for routing tests");
        }
    }

    /** Non-S3 protocol stub: must stay UNKNOWN and keep self-declared name/schemes defaults. */
    private static final class FooProps implements FileSystemProperties {
        private final Map<String, String> raw;

        FooProps(Map<String, String> raw) {
            this.raw = raw;
        }

        @Override
        public String providerName() {
            return "FOO";
        }

        @Override
        public StorageKind kind() {
            return StorageKind.OBJECT_STORAGE;
        }

        @Override
        public FileSystemType type() {
            return FileSystemType.S3;
        }

        @Override
        public Map<String, String> rawProperties() {
            return raw;
        }

        @Override
        public Map<String, String> matchedProperties() {
            return raw;
        }
    }

    private static final class FooProvider implements FileSystemProvider<FooProps> {
        @Override
        public String name() {
            return "FOO";
        }

        @Override
        public boolean supports(Map<String, String> properties) {
            return supportsExplicit(properties);
        }

        @Override
        public boolean supportsExplicit(Map<String, String> properties) {
            return Boolean.parseBoolean(properties.getOrDefault("fs.foo.support", "false"));
        }

        @Override
        public FooProps bind(Map<String, String> properties) {
            return new FooProps(properties);
        }

        @Override
        public FileSystem create(FooProps properties) {
            throw new UnsupportedOperationException("not needed for routing tests");
        }

        @Override
        public FileSystem create(Map<String, String> properties) {
            throw new UnsupportedOperationException("not needed for routing tests");
        }
    }

    @BeforeAll
    static void setUp() {
        manager = new FileSystemPluginManager();
        manager.loadBuiltins();
        manager.registerProvider(new XyzProvider());
        manager.registerProvider(new FooProvider());
        StorageAdapter.initPluginManager(manager);
    }

    @AfterAll
    static void tearDown() {
        // Other test classes in the same fork must fall back to the builtin registry.
        StorageAdapter.initPluginManager(null);
    }

    @Test
    public void testExplicitFlagRoutesUnlistedProvider() {
        Assertions.assertEquals("XYZ",
                manager.bindPrimary(ImmutableMap.of("fs.xyz.support", "true",
                        "xyz.endpoint", "https://oss.internal")).providerName());
    }

    @Test
    public void testGuessRoutesUnlistedProviderWhenNoBuiltinClaims() {
        Assertions.assertEquals("XYZ",
                manager.bindPrimary(ImmutableMap.of(
                        "xyz.endpoint", "https://bucket.xyz.example.com",
                        "xyz.access_key", "ak", "xyz.secret_key", "sk")).providerName());
    }

    @Test
    public void testUnlistedProviderNeverPreemptsBuiltins() {
        // registerProvider puts the fakes at index 0 of the provider list, but routing walks
        // the StorageRegistry.Provider order first — a genuine S3 map must still route to S3.
        Assertions.assertEquals("S3",
                manager.bindPrimary(ImmutableMap.of(
                        "s3.endpoint", "s3.us-east-1.amazonaws.com",
                        "s3.access_key", "ak", "s3.secret_key", "sk")).providerName());
    }

    @Test
    public void testBindAllAppendsUnlistedAfterKnownWithHdfsPad() {
        java.util.List<FileSystemProperties> all = manager.bindAll(ImmutableMap.of(
                "xyz.endpoint", "https://bucket.xyz.example.com"));
        // default-HDFS pad stays at index 0, unlisted match appended after the known set
        Assertions.assertEquals("HDFS", all.get(0).providerName());
        Assertions.assertEquals("XYZ", all.get(all.size() - 1).providerName());
    }

    @Test
    public void testS3CompatibleUnlistedProviderJoinsS3Family() {
        StorageAdapter adapter = StorageAdapter.of(ImmutableMap.of(
                "fs.xyz.support", "true", "xyz.endpoint", "https://bucket.xyz.example.com"));
        Assertions.assertEquals(StorageTypeId.S3, adapter.getType());
        // self-declared metadata flows through the facade untouched
        Assertions.assertEquals("S3", adapter.getStorageName());
        Assertions.assertEquals(ImmutableSet.of("s3"), adapter.schemas());
    }

    @Test
    public void testNonS3UnlistedProviderStaysUnknown() {
        StorageAdapter adapter = StorageAdapter.of(ImmutableMap.of("fs.foo.support", "true"));
        Assertions.assertEquals(StorageTypeId.UNKNOWN, adapter.getType());
        Assertions.assertEquals("FOO", adapter.getStorageName());
        Assertions.assertTrue(adapter.schemas().isEmpty());
    }
}
