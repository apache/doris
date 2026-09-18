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

import org.apache.doris.common.io.Text;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.datasource.CatalogFactory;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.Legacy413Fixtures;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Replaying a 4.1.3 FE's edit log on this branch.
 *
 * <p>This is the half of the upgrade that the image tests cannot reach. The two paths behave differently
 * on purpose: loading an image only deserialises, and the connector is built lazily on first access, so a
 * catalog whose plugin is missing or whose properties the connector rejects merely degrades. Replaying
 * {@code OP_CREATE_CATALOG} instead builds the connector synchronously, on the thread that is starting the
 * FE -- and {@code EditLog}'s fallback turns anything thrown there into {@code System.exit(-1)}. So the
 * question "does an upgraded FE start" is decided here, not by the image tests.
 *
 * <p>The fixtures are the real bytes a 4.1.3 FE would have journalled: op code short followed by the
 * Text-framed JSON of {@link CatalogLog}. Nothing about the payload class is on the wire, which is why a
 * 4.1.3 entry and a current entry are byte-identical and only the replay behaviour differs.
 */
public class Legacy413JournalReplayTest {

    /** Catalog types the fixtures contain, i.e. the providers an upgraded FE must have installed. */
    private static final List<String> FIXTURE_TYPES = com.google.common.collect.ImmutableList.of(
            "hms", "iceberg", "paimon", "es", "jdbc", "max_compute", "trino-connector");

    private RecordingConnectorProvider.Registry registry;

    @BeforeEach
    public void setUp() {
        // The plugin manager is a process-wide static shared with every other test class in this fork:
        // start from a known-empty one rather than inheriting whatever ran before.
        registry = RecordingConnectorProvider.installFor(FIXTURE_TYPES);
    }

    @AfterEach
    public void tearDown() {
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @Test
    public void everyCreateCatalogEntryFrom413Replays() throws Exception {
        // The whole point: a 4.1.3 master's journal, replayed by a follower running this branch, must not
        // take the FE down. Asserting the returned object rather than "did not throw" matters -- a fix that
        // swallowed the exception and returned null would satisfy assertDoesNotThrow and then NPE one frame
        // up in CatalogMgr.replayCreateCatalog, landing back in System.exit(-1).
        for (String fixture : Legacy413Fixtures.journalEntryNames(320)) {
            CatalogLog log = readCatalogLog(fixture);
            String type = log.getProps().get(CatalogMgr.CATALOG_TYPE_PROP);

            CatalogIf<?> catalog = CatalogFactory.createFromLog(log);

            Assertions.assertNotNull(catalog, "replay of " + fixture + " produced no catalog");
            Assertions.assertTrue(catalog instanceof ExternalCatalog,
                    "replay of " + fixture + " produced " + catalog.getClass().getSimpleName());
            Assertions.assertEquals(log.getCatalogName(), catalog.getName());
            if (type != null && !"doris".equals(type) && !"lakesoul".equals(type)) {
                Assertions.assertEquals(type, ((ExternalCatalog) catalog).getType(),
                        "replay of " + fixture + " must preserve the catalog type");
            }
        }
    }

    @Test
    public void replayHandsTheConnectorExactlyThePropertiesThatWerePersisted() throws Exception {
        // The closest a fe-core unit test can get to "the migrated catalog still works": we cannot reach a
        // real metastore, but we can pin that the correct provider is chosen and that every property the
        // 4.1.3 FE persisted arrives at it unchanged. A migration that silently drops or renames a property
        // produces a catalog that loads fine and then cannot connect to anything.
        CatalogLog log = readCatalogLog("op320-create-g1_hms.bin");

        CatalogIf<?> catalog = CatalogFactory.createFromLog(log);
        ((ExternalCatalog) catalog).makeSureInitialized();

        RecordingConnectorProvider hms = registry.get("hms");
        // Two constructions are expected, not one: CatalogFactory builds a connector with a lightweight
        // context while the catalog does not exist yet, and initLocalObjectsImpl deliberately rebuilds it
        // with the real engine context (execution authenticator for Kerberos/secured HMS) and closes the
        // first. The one that matters is the last -- that is the connector the catalog actually runs on.
        Assertions.assertFalse(hms.calls.isEmpty(),
                "the provider claiming 'hms' must be asked to build the connector");
        Map<String, String> delivered = hms.lastProperties();
        for (Map.Entry<String, String> persisted : log.getProps().entrySet()) {
            Assertions.assertEquals(persisted.getValue(), delivered.get(persisted.getKey()),
                    "property '" + persisted.getKey() + "' did not survive the journey from the 4.1.3 "
                            + "journal to the connector");
        }
        Assertions.assertEquals("g1_hms", hms.calls.get(hms.calls.size() - 1).catalogName);

        // The superseded connector must be closed, otherwise every replayed catalog leaks a connection pool
        // and a plugin classloader reference at FE startup -- invisible until an FE with many catalogs runs
        // out of metaspace.
        for (int i = 0; i < hms.created.size() - 1; i++) {
            Assertions.assertTrue(hms.created.get(i).isClosed(),
                    "connector #" + i + " was replaced during initialisation but never closed");
        }
    }

    @Test
    public void connectorConstructorThatThrowsMustNotTakeTheFeDown() throws Exception {
        // An upgraded FE replays properties that an older FE stored without the new connector's validation
        // ever running. If the connector constructor rejects one of them, the exception surfaces on the
        // replay thread, where EditLog turns it into System.exit(-1) -- the FE never starts, and the only
        // way out is to hand-edit metadata. The image path has no such failure mode because it is lazy.
        //
        // We assert one frame below EditLog (asserting System.exit is not possible without killing the fork).
        registry = RecordingConnectorProvider.installThrowingFor("hms");
        CatalogLog log = readCatalogLog("op320-create-g1_hms.bin");

        CatalogIf<?> catalog = CatalogFactory.createFromLog(log);

        Assertions.assertInstanceOf(PluginDrivenExternalCatalog.class, catalog,
                "a connector that fails to construct during replay must leave a registered, degraded catalog "
                        + "behind, not propagate out of CatalogFactory.createFromLog");
        Assertions.assertEquals("hms", ((ExternalCatalog) catalog).getType());
    }

    @Test
    public void replayOfATypeWithNoInstalledPluginDegradesInsteadOfFailing() throws Exception {
        // The realistic upgrade accident: the new fe/lib is deployed but plugins/connector is not, so no
        // provider claims any type. Every catalog must still register, so that only using one fails.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());

        for (String fixture : Legacy413Fixtures.journalEntryNames(320)) {
            CatalogLog log = readCatalogLog(fixture);
            CatalogIf<?> catalog = CatalogFactory.createFromLog(log);
            Assertions.assertNotNull(catalog, "replay of " + fixture + " must not fail when no plugin is installed");
        }
    }

    private static CatalogLog readCatalogLog(String fixture) throws IOException {
        byte[] bytes = Legacy413Fixtures.journalEntry(fixture);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            in.readShort(); // op code; the payload class is not on the wire
            return GsonUtils.GSON.fromJson(Text.readString(in), CatalogLog.class);
        }
    }
}
