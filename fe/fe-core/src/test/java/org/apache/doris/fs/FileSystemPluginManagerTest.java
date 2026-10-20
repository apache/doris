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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    // NOTE: real object-store providers (S3/OSS/COS/OBS) are runtime directory-loaded plugins
    // (Env.loadPlugins), NOT on fe-core's unit-test classpath (fe-core pom: "fe-filesystem impl
    // modules: runtime dependencies removed in Phase 4 P4.1"). End-to-end binding against the real
    // providers is therefore covered by P1-T06 (docker / full plugin classpath), not here.

    @Test
    public void specificPluginSupersedesFallbackInPrimaryAndAllBindings() throws Exception {
        for (boolean specificFirst : new boolean[] {false, true}) {
            FileSystemPluginManager manager = new FileSystemPluginManager();
            FileSystemProvider<?> specific = routingProvider("custom", Set.of("s3"));
            FileSystemProvider<?> general = routingProvider("S3", Set.of());
            manager.registerProvider(specificFirst ? specific : general);
            manager.registerProvider(specificFirst ? general : specific);
            Map<String, String> props = Map.of("custom.match", "true", "S3.match", "true");
            Assertions.assertEquals("custom", manager.bindPrimary(props).providerName());
            Assertions.assertEquals(1, manager.bindAll(props).size());
            Assertions.assertEquals("custom", manager.bindAll(props).get(0).providerName());
            Assertions.assertEquals("custom", Assertions.assertThrows(UnsupportedOperationException.class,
                    () -> manager.createFileSystem(props)).getMessage());

            // A nonmatching specific plugin must not remove the general fallback.
            Assertions.assertEquals("S3", manager.bindPrimary(Map.of("S3.match", "true")).providerName());
        }
    }

    @Test
    public void explicitFlagsWinAndMultipleExplicitFlagsRemainSupported() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(routingProvider("custom", Set.of("s3")));
        manager.registerProvider(routingProvider("S3", Set.of()));
        Map<String, String> props = new HashMap<>(Map.of("custom.match", "true", "S3.match", "true"));
        props.put("fs.s3.support", "true");
        Assertions.assertEquals("S3", manager.bindPrimary(props).providerName());
        Assertions.assertEquals(1, manager.bindAll(props).size());
        Assertions.assertEquals("S3", Assertions.assertThrows(UnsupportedOperationException.class,
                () -> manager.createFileSystem(props)).getMessage());
        props.put("fs.custom.support", "true");
        Assertions.assertEquals(2, manager.bindAll(props).size());
    }

    @Test
    public void objectStorageGuessPreservesDefaultHdfsAndExplicitFlagsDisableIt() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(routingProvider("S3", Set.of()));
        manager.registerProvider(routingProvider("HDFS", Set.of()));
        List<FileSystemProperties> bindings = manager.bindAll(Map.of("provider", "s3", "S3.match", "true"));
        Assertions.assertEquals(2, bindings.size());
        Assertions.assertEquals("HDFS", bindings.get(0).providerName());
        Assertions.assertEquals("S3", bindings.get(1).providerName());
        Assertions.assertEquals(1, manager.bindAll(Map.of("fs.s3.support", "true")).size());
    }

    @Test
    public void providerNameDoesNotOverrideExistingMatchOrder() throws Exception {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(routingProvider("S3", Set.of()));
        manager.registerProvider(routingProvider("HDFS", Set.of()));
        Map<String, String> props = new HashMap<>(Map.of(
                "provider", "S3", "S3.match", "true", "HDFS.match", "true"));
        Assertions.assertEquals("HDFS", manager.bindPrimary(props).providerName());
        Assertions.assertEquals(2, manager.bindAll(props).size());
        Assertions.assertEquals("HDFS", Assertions.assertThrows(UnsupportedOperationException.class,
                () -> manager.createFileSystem(props)).getMessage());

        props.put("fs.hdfs.support", "true");
        Assertions.assertEquals("HDFS", manager.bindPrimary(props).providerName());
        Assertions.assertEquals(1, manager.bindAll(props).size());
        Assertions.assertEquals("HDFS", Assertions.assertThrows(UnsupportedOperationException.class,
                () -> manager.createFileSystem(props)).getMessage());
    }

    @Test
    public void unrelatedGuessesAreNotEvaluatedBeforePrimaryMatch() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(routingProvider("S3", Set.of()));
        manager.registerProvider(new FileSystemProvider<FileSystemProperties>() {
            @Override
            public String name() {
                return "unused";
            }

            @Override
            public boolean supports(Map<String, String> properties) {
                throw new AssertionError("A later unrelated provider must not be probed");
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                throw new AssertionError("A later unrelated provider must not be created");
            }

            @Override
            public boolean supportsGuess(Map<String, String> properties) {
                throw new AssertionError("A later unrelated provider must not be probed");
            }
        });
        Assertions.assertEquals("S3", manager.bindPrimary(Map.of("S3.match", "true")).providerName());
    }

    @Test
    public void nonSelectedPluginStillValidatesItsOwnedProperties() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(routingProvider("custom", Set.of()));
        manager.registerProvider(routingProvider("S3", Set.of()));
        Map<String, String> props = Map.of("provider", "S3", "custom.invalid", "true");
        Assertions.assertThrows(IllegalArgumentException.class, () -> manager.bindPrimary(props));
        Assertions.assertThrows(IllegalArgumentException.class, () -> manager.bindAll(props));
        Assertions.assertThrows(IllegalArgumentException.class, () -> manager.createFileSystem(props));
    }

    private static FileSystemProvider<FileSystemProperties> routingProvider(String name, Set<String> fallbacks) {
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public Set<String> fallbackProviders() {
                return fallbacks;
            }

            @Override
            public void validateProperties(Map<String, String> properties) {
                if (properties.containsKey(name + ".invalid")) {
                    throw new IllegalArgumentException("Invalid " + name + " properties");
                }
            }

            @Override
            public boolean supportsExplicit(Map<String, String> properties) {
                return "true".equals(properties.get("fs." + name.toLowerCase(java.util.Locale.ROOT) + ".support"));
            }

            @Override
            public boolean supportsGuess(Map<String, String> properties) {
                return properties.containsKey(name + ".match");
            }

            @Override
            public boolean supports(Map<String, String> properties) {
                return supportsExplicit(properties) || supportsGuess(properties);
            }

            @Override
            public FileSystemProperties bind(Map<String, String> properties) {
                return new FakeFsProps(name);
            }

            @Override
            public FileSystem create(Map<String, String> properties) {
                throw new UnsupportedOperationException(name);
            }

            @Override
            public String name() {
                return name;
            }
        };
    }

    // ---- helpers ----

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
