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

package org.apache.doris.datasource.upgrade;

import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.Legacy413Fixtures;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The catalog type a migrated 4.1.3 catalog recovers must be a type a REAL shipped connector answers to.
 *
 * <p>This is the one upgrade property that no other test here can express. The recovery side lives in
 * {@code PluginDrivenExternalCatalog.legacyLogTypeToCatalogType} as string literals; the answering side
 * lives in each connector's {@code ConnectorProvider.getType()}, in a different maven module that fe-core
 * does not depend on at compile or runtime scope. Every other test in this repository asserts one side
 * against a literal copied out of the file under test, so renaming
 * {@code TrinoConnectorProvider.getType()} to {@code "trino"} leaves the whole suite green while every
 * migrated trino catalog is permanently unusable. The connector modules are on the test classpath (test
 * scope only, see fe-core/pom.xml) purely so this class can compare the two sides against each other.
 *
 * <p>Production discovers providers from the {@code plugins/connector/} directory rather than from the
 * classpath, so the set found here is "what this build ships", which is exactly the set this assertion
 * needs.
 */
public class Legacy413ProviderTypeContractTest {

    /**
     * The fixture ids whose properties carry no {@code type}: their catalog type exists only because the
     * migration recovered it from logType. These are the ones where a mismatch is fatal and invisible.
     */
    private static final long[] RESOURCE_BACKED_IDS = {10020L, 10021L, 10022L, 10023L};

    /** Ids whose type came straight off the wire; included so a provider rename is caught for them too. */
    private static final long[] TYPE_BEARING_IDS = {
            10001L, 10002L, 10003L, 10004L, 10005L, 10006L, 10007L, 10008L,
            10009L, 10010L, 10011L, 10012L, 10013L,
    };

    @BeforeEach
    public void setUp() {
        ConnectorPluginManager manager = new ConnectorPluginManager();
        // ServiceLoader over the test classpath, i.e. the providers this build actually ships.
        manager.loadBuiltins();
        ConnectorFactory.initPluginManager(manager);
    }

    @AfterEach
    public void tearDown() {
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @Test
    public void theBuildActuallyShipsProviders() {
        // Guard against the whole class passing vacuously: if the test-scope dependencies were dropped,
        // ServiceLoader finds nothing, findProvider returns empty for everything, and the assertions below
        // would have nothing to disagree with.
        Assertions.assertFalse(ConnectorFactory.getStandaloneCatalogTypes().isEmpty(),
                "no ConnectorProvider is on the test classpath: the fe-connector-* test-scope dependencies "
                        + "in fe-core/pom.xml were removed, and every assertion in this class is now vacuous");
    }

    @Test
    public void everyMigratedCatalogTypeIsClaimedByAShippedProvider() throws Exception {
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();

        List<String> unclaimed = new ArrayList<>();
        for (long id : concat(RESOURCE_BACKED_IDS, TYPE_BEARING_IDS)) {
            ExternalCatalog catalog = (ExternalCatalog) mgr.getCatalog(id);
            String type = catalog.getType();
            Map<String, String> props = catalog.getCatalogProperty().getProperties();
            Optional<ConnectorProvider> provider = ConnectorFactory.findProvider(type, props);
            if (!provider.isPresent()) {
                unclaimed.add("catalog " + id + " ('" + catalog.getName() + "') migrated to type '" + type
                        + "', which no shipped connector provider answers to; installed types: "
                        + ConnectorFactory.getStandaloneCatalogTypes());
            }
        }
        Assertions.assertTrue(unclaimed.isEmpty(), String.join("\n", unclaimed));
    }

    @Test
    public void theHyphenatedTrinoTypeSurvivesBothSidesOfTheContract() throws Exception {
        // Called out separately because it is the single most fragile pairing: the recovery side has to
        // special-case TRINO_CONNECTOR (name().toLowerCase() gives "trino_connector"), and the provider side
        // has to spell it the same way. Every other type happens to agree by accident of naming.
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();
        ExternalCatalog trino = (ExternalCatalog) mgr.getCatalog(10023L);

        Assertions.assertEquals("trino-connector", trino.getType());
        Assertions.assertTrue(
                ConnectorFactory.findProvider(trino.getType(), trino.getCatalogProperty().getProperties())
                        .isPresent(),
                "the type recovered from logType TRINO_CONNECTOR must be the exact string the shipped "
                        + "trino provider answers to");
    }

    @Test
    public void siblingOnlyConnectorNeverBecomesACatalogType() {
        // hudi ships a provider but is sibling-only: it serves tables parasitic on another connector's
        // metastore. If it ever started claiming standalone catalogs, "CREATE CATALOG ... type=hudi" would
        // build a catalog with no semantics, and a migrated hms catalog could be routed to it.
        Assertions.assertFalse(ConnectorFactory.getStandaloneCatalogTypes().contains("hudi"),
                "hudi is sibling-only and must never be advertised as a creatable catalog type");
    }

    @Test
    public void engineOwnedTypesAreNotClaimedByAnyPlugin() throws Exception {
        // The control from the image side, restated against real providers: "doris" stays with the engine.
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();
        CatalogIf<?> remoteDoris = mgr.getCatalog(10015L);

        Assertions.assertFalse(ConnectorFactory.getStandaloneCatalogTypes().contains("doris"),
                "'doris' is implemented by the engine; a plugin claiming it would silently take over "
                        + remoteDoris.getClass().getSimpleName() + " catalogs on upgrade");
    }

    private static long[] concat(long[] a, long[] b) {
        long[] out = new long[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
