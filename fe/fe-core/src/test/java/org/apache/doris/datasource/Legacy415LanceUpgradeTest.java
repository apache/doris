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
import org.apache.doris.common.io.Text;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.ConnectorPluginManager;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.log.InitCatalogLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.upgrade.RecordingConnectorProvider;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.persist.meta.PersistMetaModules;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Upgrading a 4.1.4+ cluster that has Lance catalogs to this branch.
 *
 * <p>4.1.4 shipped Lance as fe-core classes ({@code LanceExternalCatalog} and friends); this branch has no such
 * classes and, until a Lance connector plugin lands, no provider for the "lance" type either. The fixtures were
 * emitted by real 4.1.5 bytecode (src/test/resources/upgrade/415/PROVENANCE.txt). What must hold: the image
 * loads and the FE starts, the catalog keeps everything the user configured, it fails only when used while the
 * plugin is missing, and a plugin that claims "lance" later is handed exactly the persisted properties.
 *
 * <p>Also here: the two pieces of metadata that only unreleased branch-4.x builds wrote (the durable Lance
 * index job framework's edit-log record and image module), which must stay readable as tombstones.
 *
 * <p>This class lives in {@code org.apache.doris.datasource} because {@link ExternalCatalog#logType} and
 * {@link ExternalCatalog#tableAutoAnalyzePolicy} are protected.
 */
public class Legacy415LanceUpgradeTest {

    private static final long[] LANCE_IDS = {10101L, 10102L, 10103L};

    @AfterEach
    public void tearDown() {
        // The plugin manager is a process-wide static shared with every other test class in this fork.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
    }

    @Test
    public void everyLanceCatalogInThe415ImageLoadsAsPluginDriven() throws Exception {
        // MUTATION: without the LanceExternalCatalog compatible subtype the whole module fails to parse
        // ("cannot deserialize ... subtype named LanceExternalCatalog"), i.e. the upgraded FE cannot start.
        CatalogMgr mgr = Legacy415Fixtures.loadCatalogMgr();

        Assertions.assertEquals(4, mgr.getCatalogNum(),
                "the 4.1.5 fixture holds the internal catalog plus 3 Lance catalogs; a different count means the "
                        + "fixture was regenerated without updating this test");
        Assertions.assertSame(InternalCatalog.class, mgr.getCatalog(0L).getClass());
        for (long id : LANCE_IDS) {
            ExternalCatalog catalog = (ExternalCatalog) mgr.getCatalog(id);
            Assertions.assertSame(PluginDrivenExternalCatalog.class, catalog.getClass(),
                    "catalog " + id + " must be served by the connector framework, but loaded as "
                            + catalog.getClass().getSimpleName());
            Assertions.assertEquals("lance", catalog.getType());
            Assertions.assertEquals(InitCatalogLog.Type.PLUGIN, catalog.logType,
                    "a migrated catalog must report the PLUGIN logType, otherwise buildDbForInit takes the "
                            + "legacy path");
        }
    }

    @Test
    public void everyPropertyAndUserVisibleStateSurvivesTheMigration() throws Exception {
        CatalogMgr mgr = Legacy415Fixtures.loadCatalogMgr();

        for (long id : LANCE_IDS) {
            ExternalCatalog catalog = (ExternalCatalog) mgr.getCatalog(id);
            Map<String, String> loaded = catalog.getCatalogProperty().getProperties();
            persistedProperties(id).forEach((key, value) -> Assertions.assertEquals(value, loaded.get(key),
                    "property '" + key + "' of catalog " + id + " did not survive loading the 4.1.5 image"));
        }

        ExternalCatalog s3 = (ExternalCatalog) mgr.getCatalog(10101L);
        Assertions.assertEquals("g1_lance_s3", s3.getName());
        Assertions.assertEquals("lance on s3", s3.getComment(), "the user's COMMENT must survive the upgrade");
        // Before anything initialises the catalog: makeSureInitialized() overwrites lastUpdateTime.
        Assertions.assertEquals(Legacy415Fixtures.FIXED_UPDATE_TIME, s3.getLastUpdateTime());
        Map<Pair<String, String>, String> policy = s3.tableAutoAnalyzePolicy;
        Assertions.assertEquals(1, policy.size(), "auto-analyze policies must survive the upgrade");
        Assertions.assertEquals("enable", policy.get(Pair.of("db1", "tbl1")));
    }

    @Test
    public void everyLogTypeNameThe415ImageCarriesStillDeserializesToItself() throws Exception {
        // Gson reads an enum name it does not know as null instead of failing, so losing LANCE would not break
        // loading: a Lance catalog always persists its "type" property and nothing is backfilled from the
        // logType. This pins the rule InitCatalogLog.Type documents -- every name a released image carries
        // stays -- for the name 4.1.4 added.
        Set<String> names = new HashSet<>();
        for (Map.Entry<String, JsonElement> entry : idToCatalogJson().entrySet()) {
            JsonElement logType = entry.getValue().getAsJsonObject().get("logType");
            if (logType != null) {
                names.add(logType.getAsString());
            }
        }
        Assertions.assertEquals(java.util.Collections.singleton("LANCE"), names,
                "the fixture's premise changed; regenerate the expectations with the fixture");
        for (String name : names) {
            InitCatalogLog.Type type = GsonUtils.GSON.fromJson("\"" + name + "\"", InitCatalogLog.Type.class);
            Assertions.assertNotNull(type, "logType '" + name + "' written by 4.1.5 is no longer a known name");
            Assertions.assertEquals(name, type.name());
        }
    }

    @Test
    public void lanceCatalogWithoutItsPluginFailsOnlyWhenUsed() throws Exception {
        // The state right after upgrading to a build that ships no Lance connector: the image loaded (above),
        // and touching the catalog reports what is missing instead of something obscure.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());
        ExternalCatalog catalog = (ExternalCatalog) Legacy415Fixtures.loadCatalogMgr().getCatalog(10102L);

        RuntimeException e = Assertions.assertThrows(RuntimeException.class, catalog::makeSureInitialized);
        Assertions.assertTrue(Throwables.getStackTraceAsString(e).contains(
                        "No ConnectorProvider found for plugin-driven catalog: g1_lance_local, type: lance"),
                Throwables.getStackTraceAsString(e));
    }

    @Test
    public void lanceConnectorIsHandedExactlyThePersistedProperties() throws Exception {
        // Once a plugin claims "lance", the migrated catalog must reach it with nothing dropped or renamed --
        // credentials and namespace options included -- or it loads fine and then cannot connect to anything.
        RecordingConnectorProvider.Registry registry =
                RecordingConnectorProvider.installFor(ImmutableList.of("lance"));
        CatalogMgr mgr = Legacy415Fixtures.loadCatalogMgr();

        for (long id : LANCE_IDS) {
            ExternalCatalog catalog = (ExternalCatalog) mgr.getCatalog(id);
            catalog.makeSureInitialized();

            RecordingConnectorProvider lance = registry.get("lance");
            Assertions.assertEquals(catalog.getName(), lance.calls.get(lance.calls.size() - 1).catalogName);
            Map<String, String> delivered = lance.lastProperties();
            persistedProperties(id).forEach((key, value) -> Assertions.assertEquals(value, delivered.get(key),
                    "property '" + key + "' of catalog " + id + " did not reach the connector unchanged"));
        }
    }

    @Test
    public void everyCreateLanceCatalogEntryFrom415ReplaysWithoutThePlugin() throws Exception {
        // OP_CREATE_CATALOG journals only the property map, so this path never depended on the class label; it
        // is pinned because replay builds the connector synchronously on the FE's startup thread, where
        // EditLog turns any exception into System.exit(-1). Without the plugin the catalog registers degraded.
        ConnectorFactory.initPluginManager(new ConnectorPluginManager());

        for (String fixture : Legacy415Fixtures.journalEntryNames(OperationType.OP_CREATE_CATALOG)) {
            CatalogLog log = readCatalogLog(fixture);
            CatalogIf<?> catalog = CatalogFactory.createFromLog(log);

            Assertions.assertSame(PluginDrivenExternalCatalog.class, catalog.getClass(),
                    "replay of " + fixture + " produced " + catalog.getClass().getSimpleName());
            Assertions.assertEquals(log.getCatalogName(), catalog.getName());
            Assertions.assertEquals("lance", ((ExternalCatalog) catalog).getType());
        }
    }

    @Test
    public void everyCreateLanceCatalogEntryFrom415ReplaysThroughALancePlugin() throws Exception {
        RecordingConnectorProvider.Registry registry =
                RecordingConnectorProvider.installFor(ImmutableList.of("lance"));

        for (String fixture : Legacy415Fixtures.journalEntryNames(OperationType.OP_CREATE_CATALOG)) {
            CatalogLog log = readCatalogLog(fixture);
            CatalogIf<?> catalog = CatalogFactory.createFromLog(log);

            Assertions.assertSame(PluginDrivenExternalCatalog.class, catalog.getClass());
            RecordingConnectorProvider lance = registry.get("lance");
            Assertions.assertEquals(log.getCatalogName(), lance.calls.get(lance.calls.size() - 1).catalogName,
                    "the provider claiming 'lance' must be asked to build the connector for " + fixture);
            Map<String, String> delivered = lance.lastProperties();
            log.getProps().forEach((key, value) -> Assertions.assertEquals(value, delivered.get(key),
                    "property '" + key + "' did not survive the journey from the 4.1.5 journal to the connector"));
        }
    }

    @Test
    public void retiredLanceIndexJobRecordIsConsumedAndIgnored() throws Exception {
        // Only the branch-4.x builds that carried the durable Lance index job framework wrote op 500, so no
        // release can produce this frame; it is built in the old writer's wire form instead (branch-4.2
        // cabcf45c1af: LanceIndexJob.write = Text.writeString(out, GSON.toJson(job)), keyed by short names).
        // A real 4.1.5 entry follows it in the same stream: the reader must consume exactly the retired payload,
        // or every later entry of the journal is misread.
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeShort(OperationType.OP_LANCE_INDEX_JOB_UPSERT);
            Text.writeString(out, "{\"jid\":7,\"rev\":2,\"cid\":10101,\"dbn\":\"db1\",\"tbn\":\"tbl1\","
                    + "\"iid\":\"idx_v\",\"it\":\"IVF_PQ\",\"cn\":[\"v\"],\"rs\":\"RUNNING\"}");
            out.write(Legacy415Fixtures.journalEntry("op320-create-g1_lance_s3.bin"));
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            JournalEntity retired = new JournalEntity();
            // MUTATION: without the JournalEntity case this throws "UNKNOWN Operation Type 500".
            retired.readFields(in);
            Assertions.assertEquals(OperationType.OP_LANCE_INDEX_JOB_UPSERT, retired.getOpCode());
            // There is nothing to replay, so it must not even need an Env. Without the EditLog case the
            // IOException is turned into System.exit(-1) -- the same way it keeps a real FE from starting.
            EditLog.loadJournal(null, 1L, retired);

            JournalEntity next = new JournalEntity();
            next.readFields(in);
            Assertions.assertEquals(OperationType.OP_CREATE_CATALOG, next.getOpCode());
            Assertions.assertEquals("g1_lance_s3", ((CatalogLog) next.getData()).getCatalogName());
            Assertions.assertEquals(-1, in.read(), "both entries must be consumed exactly");
        }
    }

    @Test
    public void lanceIndexJobManagerImageModuleStaysDeprecated() {
        // The same builds wrote an image module named "lanceIndexJobManager". MetaReader skips a module listed
        // here by its byte range; without the entry an image from those builds fails with
        // "Unknown meta module: lanceIndexJobManager" unless ignore_unknown_metadata_module is set.
        Assertions.assertTrue(PersistMetaModules.DEPRECATED_MODULE_NAMES.contains("lanceIndexJobManager"));
        Assertions.assertFalse(PersistMetaModules.MODULE_NAMES.contains("lanceIndexJobManager"),
                "a deprecated module must not be written again");
    }

    /** The property map catalog {@code id} has on the wire, read straight out of the 4.1.5 module's JSON. */
    private static Map<String, String> persistedProperties(long id) throws IOException {
        JsonObject properties = idToCatalogJson().getAsJsonObject(String.valueOf(id))
                .getAsJsonObject("catalogProperty").getAsJsonObject("properties");
        Map<String, String> result = new TreeMap<>();
        properties.entrySet().forEach(e -> result.put(e.getKey(), e.getValue().getAsString()));
        Assertions.assertTrue(result.size() > 2, "catalog " + id + " has an implausibly small property map");
        return result;
    }

    private static JsonObject idToCatalogJson() throws IOException {
        return JsonParser.parseString(Legacy415Fixtures.catalogMgrJson()).getAsJsonObject()
                .getAsJsonObject("idToCatalog");
    }

    private static CatalogLog readCatalogLog(String fixture) throws IOException {
        byte[] bytes = Legacy415Fixtures.journalEntry(fixture);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            in.readShort(); // op code; the payload class is not on the wire
            return GsonUtils.GSON.fromJson(Text.readString(in), CatalogLog.class);
        }
    }
}
