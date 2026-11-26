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

package org.apache.doris.connector.trino;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tests {@link TrinoCatalogProperties} — the typed holder for everything a user writes in
 * {@code CREATE CATALOG} for a trino-connector catalog.
 */
public class TrinoCatalogPropertiesTest {

    private static Map<String, String> minimal() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(TrinoCatalogProperties.CONNECTOR_NAME, "postgresql");
        return m;
    }

    @Test
    public void bindsBothKeysAndDefaults() {
        Map<String, String> m = minimal();
        m.put(TrinoCatalogProperties.PLUGIN_DIR, "/custom/catalog/dir");
        TrinoCatalogProperties p = TrinoCatalogProperties.of(m);
        Assertions.assertEquals("postgresql", p.getConnectorName());
        Assertions.assertEquals("/custom/catalog/dir", p.getPluginDirOverride());
    }

    @Test
    public void pluginDirOverrideDefaultsToEmpty() {
        Assertions.assertEquals("", TrinoCatalogProperties.of(minimal()).getPluginDirOverride());
    }

    // ===== required-ness =====

    @Test
    public void missingConnectorNameFailsNamingTheKey() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> TrinoCatalogProperties.of(Collections.emptyMap()));
        Assertions.assertTrue(e.getMessage().contains(TrinoCatalogProperties.CONNECTOR_NAME), e.getMessage());
    }

    @Test
    public void emptyConnectorNameCountsAsMissing() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TrinoCatalogProperties.of(ImmutableMap.of(TrinoCatalogProperties.CONNECTOR_NAME, "")));
    }

    /**
     * The binder treats a blank value as unset, where the hand-written check only tested
     * {@code isEmpty()}. A whitespace-only name could never have worked; it now fails at CREATE.
     */
    @Test
    public void blankConnectorNameCountsAsMissing() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> TrinoCatalogProperties.of(ImmutableMap.of(TrinoCatalogProperties.CONNECTOR_NAME, "   ")));
    }

    // ===== the trino.* passthrough map =====

    @Test
    public void trinoPropertiesAreStrippedOfThePrefix() {
        Map<String, String> m = minimal();
        m.put("trino.hive.metastore.uri", "thrift://host:9083");
        m.put("trino.case-insensitive-name-matching", "true");
        Map<String, String> trino = TrinoCatalogProperties.of(m).getTrinoProperties();
        Assertions.assertEquals("thrift://host:9083", trino.get("hive.metastore.uri"));
        Assertions.assertEquals("true", trino.get("case-insensitive-name-matching"));
        // connector.name stays in the map: BE reads it back out of this very payload.
        Assertions.assertEquals("postgresql", trino.get("connector.name"));
    }

    @Test
    public void nonTrinoKeysDoNotEnterTheTrinoProperties() {
        Map<String, String> m = minimal();
        m.put("type", "trino-connector");
        m.put("s3.endpoint", "http://minio:9000");
        m.put("meta.cache.ttl-second", "60");
        Map<String, String> trino = TrinoCatalogProperties.of(m).getTrinoProperties();
        Assertions.assertEquals(Collections.singleton("connector.name"), trino.keySet());
    }

    @Test
    public void trinoPropertiesAreUnmodifiable() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> TrinoCatalogProperties.of(minimal()).getTrinoProperties().put("x", "y"));
    }

    // ===== the deprecated dashed spelling =====

    @Test
    public void deprecatedDashedNameIsCorrected() {
        Assertions.assertEquals("delta_lake",
                TrinoCatalogProperties.of(ImmutableMap.of(
                        TrinoCatalogProperties.CONNECTOR_NAME, "delta-lake")).getConnectorName());
    }

    /**
     * Guards the fix for a real defect: the correction used to live in a local variable, so the map
     * serialized into the BE scan payload kept the dashed spelling. BE feeds that value straight into
     * Trino's {@code ConnectorName}, whose constructor rejects anything outside
     * {@code [a-z][a-z0-9_]*} — so every SELECT on such a catalog failed on BE with
     * "Invalid connector name", while FE metadata worked fine.
     */
    @Test
    public void theCorrectionAlsoReachesTheTrinoPropertiesSentToBackends() {
        Map<String, String> trino = TrinoCatalogProperties.of(ImmutableMap.of(
                TrinoCatalogProperties.CONNECTOR_NAME, "delta-lake")).getTrinoProperties();
        Assertions.assertEquals("delta_lake", trino.get("connector.name"));
    }

    @Test
    public void anUndashedNameIsLeftAlone() {
        Assertions.assertEquals("delta_lake",
                TrinoCatalogProperties.of(ImmutableMap.of(
                        TrinoCatalogProperties.CONNECTOR_NAME, "delta_lake")).getConnectorName());
    }

    // ===== the three rules of of() =====

    /**
     * Guards DESIGN D3(2): ALTER CATALOG merges properties — it can overwrite a key but never remove
     * one — and the same map carries engine keys and storage keys. A key refused here would leave a
     * catalog no statement could repair.
     */
    @Test
    public void unknownKeysAreTolerated() {
        Map<String, String> m = minimal();
        m.put("some_future_key", "x");
        m.put("s3.endpoint", "http://minio:9000");
        Assertions.assertDoesNotThrow(() -> TrinoCatalogProperties.of(m));
    }

    /** Guards DESIGN D3(1): of() runs on every connector rebuild, so it must be pure and idempotent. */
    @Test
    public void ofIsIdempotent() {
        Map<String, String> m = minimal();
        m.put("trino.hive.metastore.uri", "thrift://host:9083");
        TrinoCatalogProperties first = TrinoCatalogProperties.of(m);
        TrinoCatalogProperties second = TrinoCatalogProperties.of(m);
        Assertions.assertEquals(first.getConnectorName(), second.getConnectorName());
        Assertions.assertEquals(first.getTrinoProperties(), second.getTrinoProperties());
    }

    /**
     * The provider's door must stay wired to the holder: {@code validateProperties} is one statement,
     * so nothing else would fail if someone emptied it.
     */
    @Test
    public void providerDoorRunsTheHolder() {
        TrinoConnectorProvider provider = new TrinoConnectorProvider();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(Collections.emptyMap()));
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(minimal()));
    }
}
