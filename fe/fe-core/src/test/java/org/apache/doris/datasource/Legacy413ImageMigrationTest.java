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

import org.apache.doris.common.Pair;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.log.InitCatalogLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Loading a real Doris 4.1.3 image's "datasource" module on this branch.
 *
 * <p>The fixture is not hand-written: it was emitted by 4.1.3 bytecode itself
 * (see src/test/resources/upgrade/413/PROVENANCE.txt). That matters because 4.1.3's wire format has
 * several details a hand-authored blob gets wrong -- HTML-escaped '=' and '&amp;' inside jdbc_url, the
 * "resource" key vanishing when null, and "taap" switching between a JSON object and an array of
 * 2-tuples depending on whether the map is empty.
 *
 * <p>This class lives in {@code org.apache.doris.datasource} rather than beside the other upgrade tests
 * because {@link ExternalCatalog#logType} is protected and there is no getter: asserting that the
 * legacy logType really was rewritten to PLUGIN is the whole point of {@link #resourceBackedCatalogsRecoverTheirTypeFromLogType()},
 * and going through {@code getType()} instead would not observe it.
 */
public class Legacy413ImageMigrationTest {

    /** Every id in the fixture whose 4.1.3 class no longer exists and must therefore migrate. */
    private static final long[] MIGRATED_IDS = {
            10001L,                                                   // HMSExternalCatalog
            10002L, 10003L, 10004L, 10005L, 10006L, 10007L, 10008L,   // Iceberg x 7 flavours
            10009L,                                                   // PaimonExternalCatalog
            10010L, 10011L, 10012L, 10013L,                           // Es / Jdbc / MaxCompute / Trino
            10020L, 10021L, 10022L, 10023L,                           // G2: resource-backed, no "type" prop
            10030L, 10031L, 10032L, 10033L,                           // G3: Paimon flavour carry-forward
            10034L,                                                   // G3: LakeSoul (retired, degraded)
    };

    @Test
    public void everyLegacyExternalCatalogBecomesPluginDriven() throws Exception {
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();

        Assertions.assertEquals(24, mgr.getCatalogNum(),
                "the 4.1.3 fixture holds the internal catalog plus 23 externals; a different count means the "
                        + "fixture was regenerated without updating this test");

        for (long id : MIGRATED_IDS) {
            CatalogIf<?> catalog = mgr.getCatalog(id);
            Assertions.assertNotNull(catalog, "catalog " + id + " disappeared while loading the 4.1.3 image");
            // assertSame, not instanceof: PluginDrivenMvccExternalTable extends PluginDrivenExternalTable and the
            // same shape of silent downgrade is possible here if a future edit remaps a label to a subclass.
            Assertions.assertSame(PluginDrivenExternalCatalog.class, catalog.getClass(),
                    "catalog " + id + " (" + catalog.getName() + ") must be served by the connector framework "
                            + "after the cutover, but loaded as " + catalog.getClass().getSimpleName());
        }
    }

    @Test
    public void catalogTypesTheEngineStillImplementsAreNotRoutedToPlugins() throws Exception {
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();

        // Control group. "doris" is a BUILTIN_CATALOG_TYPE served by the engine itself, so remapping it onto
        // PluginDrivenExternalCatalog would be a silent regression that every other assertion here would miss.
        CatalogIf<?> remoteDoris = mgr.getCatalog(10015L);
        Assertions.assertSame(RemoteDorisExternalCatalog.class, remoteDoris.getClass(),
                "an engine-implemented catalog type must keep its own class across the cutover");

        Assertions.assertSame(InternalCatalog.class, mgr.getCatalog(0L).getClass());
    }

    @Test
    public void resourceBackedCatalogsRecoverTheirTypeFromLogType() throws Exception {
        // THE migration test. These four catalogs carry NO "type" property: 4.1.3 never needed one because the
        // concrete class carried the type. After the cutover the class is gone, so the type can only come from
        // PluginDrivenExternalCatalog.gsonPostProcess backfilling it out of the persisted logType.
        //
        // Deleting that backfill leaves every G1 (type-bearing) assertion in this file green -- these four are
        // the only ones that turn red. Do not "simplify" them away.
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();

        assertTypeRecovered(mgr, 10020L, "hms");
        assertTypeRecovered(mgr, 10021L, "es");
        assertTypeRecovered(mgr, 10022L, "jdbc");
        // The load-bearing case: TRINO_CONNECTOR is the only logType whose name().toLowerCase()
        // ("trino_connector") is NOT the type string the connector answers to. A backfill that just lowercases
        // the enum passes the other three and silently kills every migrated trino catalog.
        assertTypeRecovered(mgr, 10023L, "trino-connector");
    }

    private void assertTypeRecovered(CatalogMgr mgr, long id, String expectedType) {
        ExternalCatalog catalog = (ExternalCatalog) mgr.getCatalog(id);

        Assertions.assertEquals(expectedType, catalog.getType(),
                "catalog " + id + " persisted no 'type' property, so its type must be recovered from logType");
        // Not just reported -- actually written back, because that map is what CatalogFactory and the connector
        // providers dispatch on. A getType() that computes the answer on the fly would leave the catalog
        // unusable the moment anything reads the property map instead.
        Assertions.assertEquals(expectedType,
                catalog.getCatalogProperty().getOrDefault(CatalogMgr.CATALOG_TYPE_PROP, ""),
                "the recovered type must be persisted back into the property map, not just returned");
        Assertions.assertEquals(InitCatalogLog.Type.PLUGIN, catalog.logType,
                "a migrated catalog must report the PLUGIN logType, otherwise buildDbForInit takes the legacy path");
    }

    @Test
    public void catalogIdentityAndUserVisibleStateSurviveTheMigration() throws Exception {
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();

        ExternalCatalog hms = (ExternalCatalog) mgr.getCatalog(10001L);
        Assertions.assertEquals("g1_hms", hms.getName());
        Assertions.assertEquals("my hive", hms.getComment(), "the user's COMMENT must survive the upgrade");
        // Asserted BEFORE anything initialises the catalog: makeSureInitialized() overwrites lastUpdateTime
        // with System.currentTimeMillis(), so this can only be checked on a freshly deserialised object.
        Assertions.assertEquals(Legacy413Fixtures.FIXED_UPDATE_TIME, hms.getLastUpdateTime());
        Assertions.assertEquals("thrift://hms-host:9083",
                hms.getCatalogProperty().getOrDefault("hive.metastore.uris", ""));

        // taap uses a Pair<String,String> key, which GSON writes as an array of 2-tuples via
        // enableComplexMapKeySerialization. If that encoding ever changes, auto-analyze policies silently
        // vanish on upgrade and nothing else in the suite would notice.
        Map<Pair<String, String>, String> policy = hms.tableAutoAnalyzePolicy;
        Assertions.assertEquals(2, policy.size(), "auto-analyze policies must survive the upgrade");
        Assertions.assertEquals("enable", policy.get(Pair.of("db1", "tbl1")));
        Assertions.assertEquals("disable", policy.get(Pair.of("db2", "tbl2")));
    }

    @Test
    public void jdbcUrlSurvivesHtmlEscapingRoundTrip() throws Exception {
        // 4.1.3 persists jdbc_url with '=' and '&' HTML-escaped (= / &). This asserts the value is
        // read back unescaped -- a fixture-fidelity canary as much as a product assertion: if someone replaces
        // the golden bytes with a hand-typed blob, this is the first thing that breaks.
        CatalogMgr mgr = Legacy413Fixtures.loadCatalogMgr();

        String jdbcUrl = ((ExternalCatalog) mgr.getCatalog(10011L))
                .getCatalogProperty().getOrDefault("jdbc_url", "");
        Assertions.assertTrue(jdbcUrl.startsWith("jdbc:mysql://mysql:3306/db?"), jdbcUrl);
        Assertions.assertTrue(jdbcUrl.contains("useSSL=false"), "'=' must round-trip unescaped: " + jdbcUrl);
        Assertions.assertTrue(jdbcUrl.contains("&serverTimezone=UTC"), "'&' must round-trip unescaped: " + jdbcUrl);
    }
}
