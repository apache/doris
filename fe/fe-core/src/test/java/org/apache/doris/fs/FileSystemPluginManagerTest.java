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
import org.apache.doris.filesystem.properties.StorageProperties;
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

    // ---- bindAll (P0-T02 / D-009): raw map -> List<fe-filesystem StorageProperties> ----

    @Test
    public void bindAll_collectsTypedPropertiesFromEverySupportingProvider() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties s3Props = new FakeFsProps("S3");
        FileSystemProperties hdfsLikeProps = new FakeFsProps("HDFSLIKE");
        manager.registerProvider(bindingProvider("A", s3Props));
        manager.registerProvider(bindingProvider("B", hdfsLikeProps));

        List<StorageProperties> bound = manager.bindAll(new HashMap<>());

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

        List<StorageProperties> bound = manager.bindAll(new HashMap<>());

        Assertions.assertEquals(1, bound.size());
        Assertions.assertSame(supported, bound.get(0));
    }

    @Test
    public void bindAll_skipsLegacyProvidersWithoutTypedBinding() {
        // HDFS/broker/local providers support() their props but have not migrated bind() -> the
        // default throws UnsupportedOperationException. They contribute no typed StorageProperties
        // (the connector covers them via raw fs./dfs./hadoop. passthrough), so bindAll must skip
        // them rather than blow up -- matching legacy createAll's object-store-only Hadoop scope.
        FileSystemPluginManager manager = new FileSystemPluginManager();
        FileSystemProperties typed = new FakeFsProps("S3");
        manager.registerProvider(bindingProvider("typed", typed));
        manager.registerProvider(legacyProviderThatSupportsButCannotBind("legacyHdfs"));

        List<StorageProperties> bound = manager.bindAll(new HashMap<>());

        Assertions.assertEquals(1, bound.size());
        Assertions.assertSame(typed, bound.get(0));
    }

    @Test
    public void bindAll_returnsEmptyListWhenNoProviderSupports() {
        FileSystemPluginManager manager = new FileSystemPluginManager();
        manager.registerProvider(nonSupportingProvider("none1"));
        manager.registerProvider(nonSupportingProvider("none2"));

        List<StorageProperties> bound = manager.bindAll(new HashMap<>());

        Assertions.assertTrue(bound.isEmpty());
    }

    // NOTE: real object-store providers (S3/OSS/COS/OBS) are runtime directory-loaded plugins
    // (Env.loadPlugins), NOT on fe-core's unit-test classpath (fe-core pom: "fe-filesystem impl
    // modules: runtime dependencies removed in Phase 4 P4.1"). End-to-end binding against the real
    // providers is therefore covered by P1-T06 (docker / full plugin classpath), not here.

    // ---- helpers ----

    private static FileSystemProvider<FileSystemProperties> bindingProvider(
            String name, FileSystemProperties bound) {
        return new FileSystemProvider<FileSystemProperties>() {
            @Override
            public boolean supports(Map<String, String> properties) {
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
