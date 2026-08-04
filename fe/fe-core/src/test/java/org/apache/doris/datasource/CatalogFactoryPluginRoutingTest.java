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

package org.apache.doris.datasource;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.CreateCatalogCommand;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * How {@link CatalogFactory} decides which catalog type is served by what.
 *
 * <p>The engine holds no list of accepted catalog types: a type is creatable because some registered connector
 * provider claims it, or because the engine implements that type itself. These tests pin the consequences of
 * that: a type the engine has never heard of is creatable, a sibling-only connector still is not, a typo fails
 * loud, and edit-log replay of an unserved type does not take FE down with it.
 *
 * <p>Routing is asserted by counting whether the provider was actually consulted; the catalog class alone
 * cannot tell "built from its plugin" apart from "registered degraded", since both are plugin-driven catalogs.
 */
public class CatalogFactoryPluginRoutingTest {

    private static final String THIRD_PARTY_TYPE = "acme-lake";

    @BeforeEach
    void setUp() {
        // The plugin manager is a static singleton shared with every other test in the fork: start from a known
        // empty one rather than inheriting whatever the previous test class registered.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @AfterEach
    void tearDown() {
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @Test
    void typeUnknownToTheEngineIsCreatableViaItsPlugin() throws Exception {
        // The whole point of dropping the catalog type allow-list. Before it was dropped this type could not be
        // created no matter how correctly its plugin was written, installed and loaded: CREATE CATALOG never
        // reached the provider, because the type name was not one of seven strings compiled into fe-core.
        AtomicInteger consulted = registerProvider(THIRD_PARTY_TYPE, true);

        CatalogIf<?> catalog = CatalogFactory.createFromLog(catalogLog("acme_ctl", props(THIRD_PARTY_TYPE)));

        Assertions.assertEquals(1, consulted.get(),
                "the provider claiming this type must be asked to build the connector");
        Assertions.assertInstanceOf(PluginDrivenExternalCatalog.class, catalog);
    }

    @Test
    void siblingOnlyTypeStillCannotBeCreatedAsACatalog() {
        // A sibling-only connector (hudi is the real one) serves tables parasitic on another connector's
        // metastore and has no catalog class behind it in the engine. Dropping the allow-list must not turn its
        // internal lookup key into a user-facing catalog type — that would build a catalog with no semantics.
        AtomicInteger consulted = registerProvider("sibling_only", false);

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> CatalogFactory.createFromCommand(1L, command("sib_ctl", props("sibling_only"))));

        Assertions.assertEquals(0, consulted.get(), "a sibling-only provider must not be asked to back a catalog");
        Assertions.assertTrue(e.getMessage().contains("No connector plugin claimed"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("Installed connector types: []"),
                "a sibling-only type must not be advertised as a creatable catalog type: " + e.getMessage());
    }

    @Test
    void anUnclaimedTypeFailsLoudAndNamesTheInstalledTypes() {
        // Without this, dropping the allow-list would degrade into "any typo silently builds a broken catalog".
        // The installed-type list is what tells a user whether they mistyped or forgot to install the plugin.
        registerProvider("iceberg", true);

        DdlException e = Assertions.assertThrows(DdlException.class,
                () -> CatalogFactory.createFromCommand(1L, command("typo_ctl", props("icebrg"))));

        Assertions.assertTrue(e.getMessage().contains("icebrg"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("Installed connector types: [iceberg]"),
                "the installed types must be named so a typo is self-diagnosing: " + e.getMessage());
    }

    @Test
    void replayOfAnUnservedTypeDegradesInsteadOfKillingFe() throws Exception {
        // Replaying an edit log must never throw here: EditLog's fallback catch turns any exception from a
        // replayed operation into System.exit(-1) unless its op code is listed in
        // skip_operation_types_on_replay_exception (empty by default). So a plugin removed from the plugin
        // directory would keep the whole FE from starting instead of making one catalog unusable. Registering it
        // degraded defers the failure to first access, where it is a normal query error.
        AtomicInteger consulted = registerProvider("iceberg", true);

        CatalogIf<?> catalog = CatalogFactory.createFromLog(catalogLog("gone_ctl", props(THIRD_PARTY_TYPE)));

        Assertions.assertEquals(0, consulted.get(), "no registered provider claims this type");
        Assertions.assertInstanceOf(PluginDrivenExternalCatalog.class, catalog,
                "the catalog must still be registered, so that only accessing it fails");
    }

    @Test
    void everyReservedTypeNameIsStillServedByTheEngineItself() throws Exception {
        // CatalogFactory.BUILTIN_CATALOG_TYPES is what makes those names unclaimable by a plugin. If a name were
        // listed there but had no case in the switch, it would silently stop being a catalog type at all and
        // start reporting "no connector plugin claimed" — this loop is the guard against that drift.
        boolean savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        try {
            for (String builtin : CatalogFactory.BUILTIN_CATALOG_TYPES) {
                Map<String, String> props = props(builtin);
                // TestExternalCatalog reads its provider from this property; harmless for the other types.
                props.put("catalog_provider.class",
                        "org.apache.doris.datasource.RefreshCatalogTest$RefreshCatalogProvider");
                try {
                    CatalogIf<?> catalog = CatalogFactory.createFromLog(catalogLog("builtin_" + builtin, props));
                    Assertions.assertFalse(catalog instanceof PluginDrivenExternalCatalog,
                            "reserved type '" + builtin + "' must be served by the engine, not routed to plugins");
                } catch (DdlException e) {
                    // A reserved type may legitimately refuse (lakesoul is retired), but it must refuse as
                    // itself, not as an unknown type.
                    Assertions.assertFalse(e.getMessage().contains("No connector plugin claimed"),
                            "reserved type '" + builtin + "' has no case in the built-in switch: "
                                    + e.getMessage());
                }
            }
        } finally {
            FeConstants.runningUnitTest = savedRunningUnitTest;
        }
    }

    /** Registers a single fake provider and returns a counter of how often it was asked to build a connector. */
    private static AtomicInteger registerProvider(String type, boolean standalone) {
        AtomicInteger consulted = new AtomicInteger();
        ConnectorPluginManager manager = new ConnectorPluginManager();
        manager.registerProvider(new ConnectorProvider() {
            @Override
            public String getType() {
                return type;
            }

            @Override
            public boolean isStandaloneCatalogType() {
                return standalone;
            }

            @Override
            public Connector create(Map<String, String> properties, ConnectorContext context) {
                consulted.incrementAndGet();
                return new Connector() {
                    @Override
                    public ConnectorMetadata getMetadata(ConnectorSession session) {
                        return null;
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        });
        ConnectorFactory.initPluginManager(manager);
        return consulted;
    }

    private static Map<String, String> props(String type) {
        Map<String, String> props = Maps.newHashMap();
        props.put(CatalogMgr.CATALOG_TYPE_PROP, type);
        return props;
    }

    private static CatalogLog catalogLog(String name, Map<String, String> props) {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(1L);
        log.setCatalogName(name);
        log.setResource("");
        log.setComment("");
        log.setProps(props);
        return log;
    }

    private static CreateCatalogCommand command(String name, Map<String, String> props) {
        return new CreateCatalogCommand(name, false, "", "", props);
    }
}
