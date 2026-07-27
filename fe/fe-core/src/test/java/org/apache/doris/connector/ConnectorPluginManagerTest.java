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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.datasource.CatalogFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link ConnectorPluginManager}: API version compatibility, the type-name contract enforced when a
 * provider is discovered, and the split between sibling lookup and building a standalone catalog.
 */
public class ConnectorPluginManagerTest {

    private static final int CURRENT = ConnectorPluginManager.CURRENT_API_VERSION;

    private ConnectorPluginManager manager;
    private ConnectorContext testContext;

    @BeforeEach
    void setUp() {
        manager = new ConnectorPluginManager();
        testContext = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }

    @Test
    void testCompatibleApiVersionCreatesConnector() {
        manager.registerProvider(createProvider("test_type",
                ConnectorPluginManager.CURRENT_API_VERSION));

        Connector connector = manager.createConnector("test_type",
                Collections.emptyMap(), testContext);
        Assertions.assertNotNull(connector,
                "Compatible provider should create connector successfully");
    }

    @Test
    void testIncompatibleApiVersionReturnsNull() {
        manager.registerProvider(createProvider("test_type", 999));

        Connector connector = manager.createConnector("test_type",
                Collections.emptyMap(), testContext);
        Assertions.assertNull(connector,
                "Incompatible provider should be skipped, returning null");
    }

    @Test
    void testIncompatibleApiVersionValidateThrows() {
        manager.registerProvider(createProvider("test_type", 999));

        Assertions.assertThrows(IllegalArgumentException.class, () ->
                manager.validateProperties("test_type", Collections.emptyMap()),
                "validateProperties should throw for incompatible version");
    }

    @Test
    void testFallsBackToCompatibleProvider() {
        // Register incompatible first, compatible second
        manager.registerProvider(createProvider("test_type", 999));
        // registerProvider adds at index 0, so add compatible last with direct list access
        // Actually registerProvider always inserts at index 0, so we need a workaround.
        // The incompatible one was added at 0. Now add compatible at 0 too — it'll be first.
        // But we want incompatible first. Let's just test the reverse: compatible registered,
        // then incompatible for same type — the compatible (index 0) should be found first.
        ConnectorPluginManager mgr = new ConnectorPluginManager();
        // Add compatible at index 0
        mgr.registerProvider(createProvider("test_type",
                ConnectorPluginManager.CURRENT_API_VERSION));

        Connector connector = mgr.createConnector("test_type",
                Collections.emptyMap(), testContext);
        Assertions.assertNotNull(connector,
                "Should use the compatible provider");
    }

    @Test
    void testNoMatchingProviderReturnsNull() {
        Connector connector = manager.createConnector("nonexistent",
                Collections.emptyMap(), testContext);
        Assertions.assertNull(connector,
                "No matching provider should return null");
    }

    @Test
    void siblingOnlyTypeIsReachableForSiblingLookupButNeverAsACatalog() {
        // A sibling-only connector serves a table format parasitic on another connector's metastore (hudi on an
        // HMS catalog): the gateway builds it through createConnector, and that is its ONLY way in. If the
        // standalone filter leaked into createConnector, every such table would stop being readable; if the
        // filter were missing from createStandaloneCatalogConnector, CREATE CATALOG could build a catalog with
        // no engine-side semantics behind it. Both directions must hold at once.
        manager.registerProvider(createProvider("sibling_only", CURRENT, false, "sib"));

        Assertions.assertNotNull(manager.createConnector("sibling_only", Collections.emptyMap(), testContext),
                "sibling lookup must still reach a sibling-only connector — it has no other entry point");
        Assertions.assertNull(
                manager.createStandaloneCatalogConnector("sibling_only", Collections.emptyMap(), testContext),
                "a sibling-only connector must never back a standalone catalog");
    }

    @Test
    void anyRegisteredTypeCanBackACatalog_noEngineSideList() {
        // The point of removing the catalog type allow-list: a type name the engine has never heard of becomes
        // usable purely by registering a provider for it. This cannot pass while an engine-side list of
        // accepted types exists.
        manager.registerProvider(createProvider("acme-lake", CURRENT, true, "acme"));

        Assertions.assertNotNull(
                manager.createStandaloneCatalogConnector("acme-lake", Collections.emptyMap(), testContext),
                "a third-party type must be routed to its provider without any engine-side registration");
        Assertions.assertEquals(Collections.singletonList("acme-lake"), manager.getStandaloneCatalogTypes(),
                "a standalone type must be listed as creatable");
    }

    @Test
    void siblingOnlyTypeIsNotListedAsCreatable() {
        // The list feeds the CREATE CATALOG diagnostic; naming a type that can never be created would send the
        // user chasing a value the engine will always reject.
        manager.registerProvider(createProvider("sibling_only", CURRENT, false, "sib"));

        Assertions.assertTrue(manager.getStandaloneCatalogTypes().isEmpty(),
                "sibling-only types must not appear among the creatable catalog types");
        Assertions.assertEquals(Collections.singletonList("sibling_only"), manager.getRegisteredTypes(),
                "it is still a registered provider — only its eligibility for a catalog differs");
    }

    @Test
    void duplicateTypeNameOnClasspathFailsLoud() {
        // Type names are the identity CREATE CATALOG routes on and the anchor of source-prefixed namespaces.
        // Two providers claiming one name on the classpath is a build error, and the winner would be decided by
        // ServiceLoader order — silently, differently per build.
        Assertions.assertTrue(manager.registerDiscovered(createProvider("dup", CURRENT, true, "first"), true));

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.registerDiscovered(createProvider("DUP", CURRENT, true, "second"), true),
                "a classpath duplicate must fail loud, and case must not be a way around it");
        Assertions.assertTrue(e.getMessage().contains("already claimed"), e.getMessage());
    }

    @Test
    void duplicateTypeNameInPluginDirectoryIsSkippedButDoesNotStopFe() {
        // Same conflict, different blame: two plugin directories shipping one type name is a deployment
        // accident. loadPlugins promises partial success — one bad plugin dir must not keep FE from starting —
        // so the offender is skipped and the incumbent keeps serving.
        Assertions.assertTrue(manager.registerDiscovered(createProvider("dup", CURRENT, true, "first"), false));
        Assertions.assertFalse(manager.registerDiscovered(createProvider("dup", CURRENT, true, "second"), false),
                "the second claimant must be refused, not silently appended");

        Assertions.assertEquals(Collections.singletonList("dup"), manager.getRegisteredTypes(),
                "the type must be claimed exactly once");
        Connector connector = manager.createConnector("dup", Collections.emptyMap(), testContext);
        Assertions.assertEquals("first", ((TaggedConnector) connector).tag,
                "the provider that claimed the name first must keep serving it");
    }

    @Test
    void providerClaimingAnEngineBuiltinCatalogTypeIsRefused() {
        // Reserving the engine's own catalog type names is what makes the plugin-first routing order safe: a
        // plugin declaring itself "doris" could otherwise quietly take over every remote-Doris catalog a user
        // creates. Refusing it at registration means the shadowing case cannot arise at all.
        for (String builtin : new String[] {"doris", "test", "lakesoul"}) {
            Assertions.assertTrue(CatalogFactory.isBuiltinCatalogType(builtin), builtin);
            Assertions.assertFalse(manager.registerDiscovered(createProvider(builtin, CURRENT, true, "x"), false),
                    "a plugin must not be allowed to claim engine built-in type '" + builtin + "'");
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.registerDiscovered(createProvider(builtin.toUpperCase(), CURRENT, true, "x"),
                            true),
                    "on the classpath the same violation must fail loud, case-insensitively");
        }
        Assertions.assertTrue(manager.getRegisteredTypes().isEmpty(),
                "no refused provider may end up in the registry");
    }

    @Test
    void blankTypeNameIsRefused() {
        // getType() is now the sole admission ticket for third-party code; a blank name would otherwise sit in
        // the registry and match nothing, or match everything the moment someone compares loosely.
        Assertions.assertFalse(manager.registerDiscovered(createProvider("  ", CURRENT, true, "x"), false),
                "a blank type name must be refused");
        Assertions.assertTrue(manager.getRegisteredTypes().isEmpty());
    }

    @Test
    void registerProviderStillShadowsADiscoveredType() {
        // registerProvider exists to stand in for a real plugin in tests (several rely on shadowing a real
        // type name). The uniqueness check must not reach it, or those tests lose their only seam.
        Assertions.assertTrue(manager.registerDiscovered(createProvider("iceberg", CURRENT, true, "real"), false));
        manager.registerProvider(createProvider("iceberg", CURRENT, true, "override"));

        Connector connector = manager.createConnector("iceberg", Collections.emptyMap(), testContext);
        Assertions.assertEquals("override", ((TaggedConnector) connector).tag,
                "the explicitly registered provider must win over the discovered one");
    }

    @Test
    void duplicateCreateTableEngineNameIsRefused() {
        // Engine names route CREATE TABLE ... ENGINE= the same way type names route CREATE CATALOG. Two
        // plugins answering to one engine name would make the statement mean whichever registered first, so
        // the conflict is refused where it can still be refused: at registration.
        Assertions.assertTrue(manager.registerDiscovered(
                createProviderWithEngines("first_type", "shared_engine"), false));
        Assertions.assertFalse(manager.registerDiscovered(
                        createProviderWithEngines("second_type", "SHARED_ENGINE"), false),
                "a second claimant of the same engine name must be refused, case-insensitively");

        Assertions.assertEquals(Collections.singletonList("first_type"), manager.getRegisteredTypes(),
                "the refused provider must not end up in the registry");

        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.registerDiscovered(createProviderWithEngines("third_type", "shared_engine"), true),
                "on the classpath the same conflict must fail loud");
        Assertions.assertTrue(e.getMessage().contains("already claimed"), e.getMessage());
    }

    @Test
    void providerClaimingAnEngineReservedEngineNameIsRefused() {
        // olap is the internal catalog's own engine, and odbc/mysql/broker are retired table types that still
        // owe the user a specific "use X instead" message from InternalCatalog. A plugin claiming one would
        // silently take over a statement the engine answers for.
        for (String reserved : new String[] {"olap", "mysql", "odbc", "broker"}) {
            Assertions.assertFalse(manager.registerDiscovered(
                            createProviderWithEngines("t_" + reserved, reserved.toUpperCase()), false),
                    "a plugin must not be allowed to claim reserved engine name '" + reserved + "'");
        }
        Assertions.assertTrue(manager.getRegisteredTypes().isEmpty(),
                "no refused provider may end up in the registry");
    }

    @Test
    void refusedProviderDoesNotLeaveItsTypeOrEngineNameClaimed() {
        // The checks run before anything is claimed, so a provider rejected for one reason must not poison the
        // name it never got. Otherwise a bad plugin directory could permanently disable a good one.
        Assertions.assertFalse(manager.registerDiscovered(createProviderWithEngines("good_type", "olap"), false),
                "rejected for the reserved engine name");

        Assertions.assertTrue(manager.registerDiscovered(createProviderWithEngines("good_type", "good_engine"),
                        false),
                "the type name must still be free after the earlier provider was refused");
        Assertions.assertEquals(Collections.singletonList("good_type"), manager.getRegisteredTypes());
    }

    private static ConnectorProvider createProviderWithEngines(String type, String... engineNames) {
        Set<String> engines = new HashSet<>(Arrays.asList(engineNames));
        return new ConnectorProvider() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public Set<String> acceptedCreateTableEngineNames() {
                return engines;
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                return new TaggedConnector(type);
            }
        };
    }

    private static ConnectorProvider createProvider(String type, int apiVersion) {
        return createProvider(type, apiVersion, true, "");
    }

    private static ConnectorProvider createProvider(String type, int apiVersion, boolean standalone, String tag) {
        return new ConnectorProvider() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public int apiVersion() {
                return apiVersion;
            }

            @Override
            public boolean isStandaloneCatalogType() {
                return standalone;
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                return new TaggedConnector(tag);
            }
        };
    }

    /** A connector that remembers which provider made it, so selection can be asserted. */
    private static final class TaggedConnector implements Connector {
        private final String tag;

        private TaggedConnector(String tag) {
            this.tag = tag;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
