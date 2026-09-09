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

import org.apache.doris.common.util.DatasourcePrintableMap;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

public class FileSystemPluginManagerTest {

    @Test
    public void registerProvider_registersProviderSensitiveKeysForMasking() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
                return false;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                return null;
            }

            @Override
            public Set<String> sensitivePropertyKeys() {
                return Collections.singleton("PLUGIN_MANAGER_TEST_SECRET_ALIAS");
            }
        });

        Assertions.assertTrue(
                DatasourcePrintableMap.SENSITIVE_KEY.contains("PLUGIN_MANAGER_TEST_SECRET_ALIAS"));
    }

    // ---- bindAll (P0-T02 / D-009): raw map -> List<fe-filesystem FileSystemProperties> ----

    @Test
    public void bindAll_collectsTypedPropertiesFromEverySupportingProvider() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties s3Props = new FakeFsProps("S3");
        FileSystemProperties hdfsLikeProps = new FakeFsProps("HDFSLIKE");
        manager.registerProvider(bindingProvider("A", s3Props));
        manager.registerProvider(bindingProvider("B", hdfsLikeProps));

        List<FileSystemProperties> bound = manager.bindAll(new HashMap<>());

        // bindAll returns ALL supporting providers' bound props (unlike createFileSystem's first-match).
        Assertions.assertEquals(2, bound.size());
        Assertions.assertTrue(bound.contains(s3Props));
        Assertions.assertTrue(bound.contains(hdfsLikeProps));
    }

    @Test
    public void bindAll_skipsProvidersThatDoNotSupportTheProperties() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties supported = new FakeFsProps("S3");
        manager.registerProvider(bindingProvider("supports", supported));
        manager.registerProvider(nonSupportingProvider("ignored"));

        List<FileSystemProperties> bound = manager.bindAll(new HashMap<>());

        Assertions.assertEquals(1, bound.size());
        Assertions.assertSame(supported, bound.get(0));
    }

    @Test
    public void bindAll_skipsLegacyProvidersWithoutTypedBinding() {
        // HDFS/broker/local providers match their props but have not migrated bind() -> the
        // default throws UnsupportedOperationException. They contribute no typed FileSystemProperties
        // (the connector covers them via raw fs./dfs./hadoop. passthrough), so bindAll must skip
        // them rather than blow up -- matching legacy createAll's object-store-only Hadoop scope.
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties typed = new FakeFsProps("S3");
        manager.registerProvider(bindingProvider("typed", typed));
        manager.registerProvider(legacyProviderThatSupportsButCannotBind("legacyHdfs"));

        List<FileSystemProperties> bound = manager.bindAll(new HashMap<>());

        Assertions.assertEquals(1, bound.size());
        Assertions.assertSame(typed, bound.get(0));
    }

    @Test
    public void bindAll_returnsEmptyListWhenNoProviderSupports() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(nonSupportingProvider("none1"));
        manager.registerProvider(nonSupportingProvider("none2"));

        List<FileSystemProperties> bound = manager.bindAll(new HashMap<>());

        Assertions.assertTrue(bound.isEmpty());
    }

    @Test
    public void bindAll_marksOnlyTheRealHdfsFallbackCreationPath() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.loadBuiltins();
        Map<String, String> raw = Map.of("azure.account_name", "account", "azure.account_key", "key");

        List<FileSystemProperties> result = manager.bindAll(raw);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("HDFS", result.get(0).providerName());
        Assertions.assertTrue(result.get(0).isSyntheticDefault());
        Assertions.assertEquals("AZURE", result.get(1).providerName());
        Assertions.assertFalse(result.get(1).isSyntheticDefault());
        Assertions.assertEquals(raw, result.get(0).rawProperties());
    }

    @Test
    public void bindAll_keepsHdfsOnlyFallback() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.loadBuiltins();

        List<FileSystemProperties> result = manager.bindAll(Map.of());

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("HDFS", result.get(0).providerName());
        Assertions.assertTrue(result.get(0).isSyntheticDefault());
    }

    @Test
    public void bindAll_doesNotMarkExplicitOrUriMatchedHdfsAsSynthetic() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.loadBuiltins();
        for (Map<String, String> raw : List.of(
                Map.of("fs.hdfs.support", "true"),
                Map.of("uri", "hdfs://namenode/warehouse"),
                Map.of("URI", "viewfs://mount/warehouse"),
                Map.of("hadoop.username", "test-user"))) {
            List<FileSystemProperties> result = manager.bindAll(raw);

            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals("HDFS", result.get(0).providerName());
            Assertions.assertFalse(result.get(0).isSyntheticDefault());
        }
    }

    @Test
    public void bindAll_preservesRealMixedAzureAndHdfsBindings() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.loadBuiltins();
        Map<String, String> raw = Map.of("azure.account_name", "account", "azure.account_key", "key",
                "uri", "hdfs://namenode/warehouse");

        List<FileSystemProperties> result = manager.bindAll(raw);

        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.stream().anyMatch(binding -> "HDFS".equals(binding.providerName())));
        Assertions.assertTrue(result.stream().anyMatch(binding -> "AZURE".equals(binding.providerName())));
        Assertions.assertTrue(result.stream().noneMatch(FileSystemProperties::isSyntheticDefault));
    }

    @Test
    public void bindVended_keepsTypedBindingWithoutRebindingOrDefaultHdfs() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties azure = new FakeFsProps("AZURE");
        manager.registerProvider(vendedProvider("AZURE", (credentials, catalog) -> Optional.of(azure)));
        manager.registerProvider(nonSupportingProvider("HDFS"));

        List<FileSystemProperties> result = manager.bindVended(Map.of("token", "temporary"), Map.of()).get();

        Assertions.assertEquals(List.of(azure), result);
        // Both raw binding implementations throw: neither Azure rebinding nor default HDFS is allowed.
    }

    @Test
    public void bindVended_declinesUnknownDialectWithoutRunningRawBinding() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(vendedProvider("AZURE", (credentials, catalog) -> Optional.empty()));

        Assertions.assertFalse(manager.bindVended(Map.of("other.token", "temporary"), Map.of()).isPresent());
    }

    @Test
    public void bindVended_propagatesRecognizedCredentialFailure() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        StoragePropertiesException failure = new StoragePropertiesException("Invalid provider credential");
        manager.registerProvider(vendedProvider("AZURE", (credentials, catalog) -> {
            throw failure;
        }));

        Assertions.assertSame(failure, Assertions.assertThrows(StoragePropertiesException.class,
                () -> manager.bindVended(Map.of("token", "invalid"), Map.of())));
    }

    @Test
    public void bindVended_preservesOtherProvidersAndTheirExclusivity() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties azure = new FakeFsProps("AZURE");
        FileSystemProperties jfs = new FakeFsProps("JFS");
        FileSystemProperties ossHdfs = new FakeFsProps("OSS_HDFS");
        FileSystemProperties s3 = new FakeFsProps("S3");
        manager.registerProvider(vendedProvider("AZURE", (credentials, catalog) -> Optional.of(azure)));
        manager.registerProvider(bindingProvider("JFS", jfs));
        manager.registerProvider(bindingProvider("HDFS", new FakeFsProps("HDFS")));
        manager.registerProvider(bindingProvider("OSS_HDFS", ossHdfs));
        manager.registerProvider(bindingProvider("OSS", new FakeFsProps("OSS")));
        manager.registerProvider(bindingProvider("S3", s3));

        List<FileSystemProperties> result = manager.bindVended(Map.of("token", "temporary"), Map.of()).get();

        Assertions.assertEquals(List.of(azure, jfs, ossHdfs, s3), result);
    }

    @Test
    public void bindVended_preservesExplicitFlagSuppressionOfGuessProviders() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties azure = new FakeFsProps("AZURE");
        manager.registerProvider(vendedProvider("AZURE", (credentials, catalog) -> Optional.of(azure)));
        manager.registerProvider(bindingProvider("S3", new FakeFsProps("S3")));

        List<FileSystemProperties> result = manager.bindVended(
                Map.of("token", "temporary", "fs.azure.support", "true"), Map.of()).get();

        Assertions.assertEquals(List.of(azure), result);
    }

    @Test
    public void bindVended_supportsUnlistedProviderAndSuppliesCatalogContext() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties custom = new FakeFsProps("CUSTOM");
        Map<String, String> token = Map.of("custom.token", "temporary");
        Map<String, String> connection = Map.of("custom.endpoint", "endpoint");
        manager.registerProvider(vendedProvider("CUSTOM", (credentials, catalog) -> {
            Assertions.assertSame(token, credentials);
            Assertions.assertSame(connection, catalog);
            return Optional.of(custom);
        }));

        Assertions.assertEquals(List.of(custom), manager.bindVended(token, connection).get());
    }

    // ---- helpers ----

    private static FileSystemProvider<FileSystemProperties> vendedProvider(String name,
            BiFunction<Map<String, String>, Map<String, String>, Optional<FileSystemProperties>> bindVended) {
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean supports(Map<String, String> properties) {
                return true;
            }

            @Override
            public boolean supportsGuess(Map<String, String> properties) {
                return true;
            }

            @Override
            public Optional<FileSystemProperties> bindVended(Map<String, String> credentials,
                    Map<String, String> catalogProperties) {
                return bindVended.apply(credentials, catalogProperties);
            }

            @Override
            public FileSystemProperties bind(Map<String, String> properties) {
                throw new AssertionError("Vended binding must not be rebound through a raw property map");
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                throw new AssertionError("Binding must not create a client");
            }
        };
    }

    private static FileSystemProvider<FileSystemProperties> bindingProvider(
            String name, FileSystemProperties bound) {
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
                return true;
            }

            @Override
            public boolean supportsGuess(Map<String, String> properties) {
                // Out-of-tree providers are selected by supportsExplicit/supportsGuess since upstream
                // #66004; supports() alone only feeds createFileSystem(Map) and is warned about.
                return true;
            }

            @Override
            public FileSystemProperties bind(Map<String, String> properties) {
                return bound;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                return null;
            }

            @Override
            public String name() {
                return name;
            }
        };
    }

    private static FileSystemProvider<FileSystemProperties> nonSupportingProvider(String name) {
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
                return false;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                return null;
            }

            @Override
            public String name() {
                return name;
            }
        };
    }

    private static FileSystemProvider<FileSystemProperties> legacyProviderThatSupportsButCannotBind(
            String name) {
        // No bind() override -> inherits the default that throws UnsupportedOperationException.
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
                return true;
            }

            @Override
            public boolean supportsGuess(Map<String, String> properties) {
                // Out-of-tree providers are selected by supportsExplicit/supportsGuess since upstream
                // #66004; supports() alone only feeds createFileSystem(Map) and is warned about.
                return true;
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                return null;
            }

            @Override
            public String name() {
                return name;
            }
        };
    }

    private static final class FakeFsProps implements FileSystemProperties {
        private final String name;

        private FakeFsProps(String name) {
            this.name = name;
        }

        @Override
        public String providerName() {
            return name;
        }

        @Override
        public StorageKind kind() {
            return null;
        }

        @Override
        public FileSystemType type() {
            return null;
        }

        @Override
        public Map<String, String> rawProperties() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, String> matchedProperties() {
            return Collections.emptyMap();
        }
    }
}
