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

package org.apache.doris.connector;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageKind;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.fs.FileSystemPluginManager;
import org.apache.doris.kerberos.ExecutionAuthenticator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Design S2: pins that {@link DefaultConnectorContext#getStorageProperties()} binds the catalog's raw storage
 * map directly through {@link FileSystemFactory#bindAllStorageProperties} (the live plugin-loaded manager),
 * sourced straight from the raw-props supplier — with no fe-core {@code StorageProperties.createAll} parse /
 * {@code getOrigProps()} round-trip on this path. The raw supplier is responsible for merging the catalog's
 * derived storage defaults and honoring the vended gate (see {@code CatalogProperty.getEffectiveRawStorageProperties}).
 */
public class DefaultConnectorContextStoragePropsTest {

    private static final Supplier<ExecutionAuthenticator> NOOP_AUTH =
            () -> new ExecutionAuthenticator() {};

    @AfterEach
    public void resetFactory() {
        // The wiring test injects a live manager; restore the "no live manager" default for other tests.
        FileSystemFactory.initPluginManager(null);
    }

    @Test
    public void getStorageProperties_emptyWhenNoRawSupplier() {
        // 2-arg ctor -> empty raw supplier -> empty list (REST/vended/non-plugin/local-FS warehouse), so
        // non-plugin paths are unaffected and there is no NPE. MUTATION: null / throw -> red.
        Assertions.assertTrue(new DefaultConnectorContext("c", 1L).getStorageProperties().isEmpty());
    }

    @Test
    public void getStorageProperties_emptyWhenRawSupplierEmpty() {
        // A REST/vended or credential-less catalog: the raw supplier yields an empty map -> empty list, so no
        // static storage is bound. MUTATION: dropping the isEmpty() short-circuit -> reaches the factory -> red.
        DefaultConnectorContext ctx = new DefaultConnectorContext("c", 1L, NOOP_AUTH,
                Collections::emptyMap, Collections::emptyMap);
        Assertions.assertTrue(ctx.getStorageProperties().isEmpty());
    }

    @Test
    public void getStorageProperties_bindsRawCatalogMapViaLiveManager() {
        // The raw catalog map is bound as-is through the live plugin-loaded manager.
        Map<String, String> raw = new HashMap<>();
        raw.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        raw.put("oss.access_key", "ak");
        raw.put("oss.secret_key", "sk");

        // Inject a live manager whose provider captures the raw map it is asked to bind.
        CapturingProvider provider = new CapturingProvider();
        FileSystemPluginManager mgr = new FileSystemPluginManager();
        mgr.registerProvider(provider);
        FileSystemFactory.initPluginManager(mgr);

        // 5-arg ctor: the typed supplier (unused by getStorageProperties, kept for other consumers) is empty;
        // the raw supplier is exactly what this path binds — no getOrigProps() round-trip.
        DefaultConnectorContext ctx = new DefaultConnectorContext("c", 1L, NOOP_AUTH,
                Collections::emptyMap, () -> raw);
        List<org.apache.doris.filesystem.properties.StorageProperties> result = ctx.getStorageProperties();

        // The connector received the props bound from the raw supplier's map.
        // MUTATION: returning the default empty / not reaching the factory / a filtered map -> red.
        Assertions.assertEquals(1, result.size());
        Assertions.assertNotNull(provider.capturedRawMap, "getStorageProperties() must bind via the factory");
        Assertions.assertEquals("ak", provider.capturedRawMap.get("oss.access_key"),
                "must bind the raw catalog map from the raw supplier");
        Assertions.assertEquals("oss-cn-beijing.aliyuncs.com", provider.capturedRawMap.get("oss.endpoint"));
    }

    private static final class CapturingProvider implements FileSystemProvider<FileSystemProperties> {
        private Map<String, String> capturedRawMap;

        @Override
        public boolean supports(Map<String, String> properties) {
            return true;
        }

        @Override
        public FileSystemProperties bind(Map<String, String> properties) {
            this.capturedRawMap = properties;
            return new FakeFsProps();
        }

        @Override
        public FileSystem create(Map<String, String> properties) {
            return null;
        }

        @Override
        public String name() {
            return "capturing";
        }
    }

    private static final class FakeFsProps implements FileSystemProperties {
        @Override
        public String providerName() {
            return "FAKE";
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
