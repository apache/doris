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

import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Which uninitialized catalogs the metastore-event driver is allowed to force-initialize.
 *
 * <p>The driver initializes a catalog nobody has queried on this FE so it can obtain its event source and
 * seed its cursor — required on followers, which normally receive no queries at all. Two opposite mistakes
 * are possible and neither shows up as an error:</p>
 * <ul>
 *   <li>too narrow — a connector that has an event source is left out, and its catalog silently never syncs
 *       incrementally on an FE that has not queried it (this is what a hardcoded {@code "hms"} type check
 *       did to every other connector);</li>
 *   <li>too wide — idle catalogs of every other type get force-initialized on a timer, i.e. connect to
 *       remote metastores nobody asked about.</li>
 * </ul>
 *
 * <p>So both directions are asserted, by counting whether the catalog was actually touched.</p>
 */
public class MetastoreEventSyncDriverSeedingTest {

    private static final String WITH_EVENTS = "fake-with-events";
    private static final String WITHOUT_EVENTS = "fake-without-events";

    @BeforeEach
    void setUp() {
        // The plugin manager is a static singleton shared with every other test in the fork.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @AfterEach
    void tearDown() {
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @Test
    public void catalogOfAnEventSourceTypeIsForceInitialized() {
        registerProviders();
        CountingCatalog catalog = new CountingCatalog(WITH_EVENTS);

        boolean polled = new MetastoreEventSyncDriver().seedCursorOfUninitializedCatalog(catalog);

        // The declaring type must be initialized even though nothing on this FE has queried it — otherwise
        // its cursor is never seeded and the catalog only starts syncing after someone happens to query it.
        Assertions.assertEquals(1, catalog.initCount.get(),
                "a catalog whose type declares an event source must be force-initialized");
        // Our fake init throws, mirroring an unreachable metastore: the driver swallows it and retries the
        // next cycle rather than aborting the whole sweep.
        Assertions.assertFalse(polled);
    }

    @Test
    public void catalogWithoutAnEventSourceIsNeverTouched() {
        registerProviders();
        CountingCatalog catalog = new CountingCatalog(WITHOUT_EVENTS);

        boolean polled = new MetastoreEventSyncDriver().seedCursorOfUninitializedCatalog(catalog);

        // The guard that keeps idle catalogs inert: no connection, no metadata load, no init. MUTATION:
        // dropping the providesEventSource() check -> every idle plugin catalog is initialized on a timer.
        Assertions.assertEquals(0, catalog.initCount.get(),
                "an idle catalog of a type without an event source must not be touched at all");
        Assertions.assertFalse(polled);
    }

    @Test
    public void catalogOfAnUnregisteredTypeIsNeverTouched() {
        // No provider registered at all (plugin not installed / not loaded yet).
        CountingCatalog catalog = new CountingCatalog(WITH_EVENTS);

        Assertions.assertFalse(new MetastoreEventSyncDriver().seedCursorOfUninitializedCatalog(catalog));
        Assertions.assertEquals(0, catalog.initCount.get());
    }

    private static void registerProviders() {
        ConnectorPluginManager manager = new ConnectorPluginManager();
        manager.registerProvider(new FakeProvider(WITH_EVENTS, true));
        manager.registerProvider(new FakeProvider(WITHOUT_EVENTS, false));
        ConnectorFactory.initPluginManager(manager);
    }

    /** A plugin catalog that records force-initialization instead of performing one. */
    private static final class CountingCatalog extends PluginDrivenExternalCatalog {
        private final AtomicInteger initCount = new AtomicInteger();

        private CountingCatalog(String type) {
            super(1L, "ctl_" + type, null, typeProps(type), "", null);
        }

        @Override
        protected void initLocalObjectsImpl() {
            initCount.incrementAndGet();
            // Stop here: a real init would build the meta cache and connect to the remote metastore. The
            // driver treats a throwing init as "retry next cycle", which is the behaviour under test for the
            // declaring type.
            throw new IllegalStateException("test stub: not really initializing");
        }
    }

    private static Map<String, String> typeProps(String type) {
        Map<String, String> props = Maps.newHashMap();
        props.put(CatalogMgr.CATALOG_TYPE_PROP, type);
        return props;
    }

    private static final class FakeProvider implements ConnectorProvider {
        private final String type;
        private final boolean withEvents;

        private FakeProvider(String type, boolean withEvents) {
            this.type = type;
            this.withEvents = withEvents;
        }

        @Override
        public String getType() {
            return type;
        }

        @Override
        public boolean providesEventSource() {
            return withEvents;
        }

        @Override
        public Connector create(Map<String, String> properties, ConnectorContext context) {
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
    }
}
