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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.DorisConnectorException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.Tables;
import com.aliyun.odps.table.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * P2-5 FIX-DROP-DB-FORCE (clean-room re-review DG-3 / F22, F27) — guards that
 * {@code DROP DATABASE ... FORCE} cascades the table drops in the connector.
 *
 * <p><b>WHY this matters:</b> after the SPI cutover the FE
 * {@code PluginDrivenExternalCatalog.dropDb} discarded the user's {@code force} flag,
 * and the connector's {@code dropDatabase} just called {@code schemas().delete()}.
 * ODPS {@code schemas().delete()} does NOT auto-cascade (the legacy
 * {@code MaxComputeMetadataOps.dropDbImpl} force-branch enumerate-loop is itself proof),
 * so on a non-empty schema {@code DROP DB FORCE} degraded to a non-FORCE drop —
 * failing outright or leaving residue, while silently ignoring FORCE (Rule 12). These
 * tests pin the restored cascade: every table is dropped BEFORE the schema, only when
 * FORCE is set, and a failing remote drop aborts loudly before the schema is deleted.</p>
 *
 * <p>The maxcompute connector test module has no Mockito, so a hand-written recording
 * {@link McStructureHelper} captures the call order. {@code dropDatabase} never
 * dereferences {@code odps} (it only passes it to the helper), so a {@code null} odps
 * keeps the test offline — the same pattern as {@link MaxComputeBuildTableDescriptorTest}.</p>
 */
public class MaxComputeConnectorMetadataDropDbTest {

    private MaxComputeConnectorMetadata metadataWith(RecordingStructureHelper helper) {
        return new MaxComputeConnectorMetadata(
                null /* odps */, helper, "proj", "ep", "quota", Collections.emptyMap(),
                null); // null: partition cache unused by this test
    }

    @Test
    public void forceTrueCascadesAllTablesBeforeDroppingSchema() {
        RecordingStructureHelper helper = new RecordingStructureHelper(Arrays.asList("t1", "t2"));
        MaxComputeConnectorMetadata metadata = metadataWith(helper);

        metadata.dropDatabase(null, "db1", false, true);

        // WHY: legacy parity requires every table dropped first (ODPS won't auto-cascade),
        // each with ifExists=true so a raced already-gone table does not abort the cascade.
        // MUTATION: removing the `if (force) {...}` block -> log is just ["dropDb:db1"] (red);
        // flipping the hardcoded dropTable(...,true) to false -> ":false" markers (red).
        Assertions.assertEquals(
                Arrays.asList("dropTable:t1:true", "dropTable:t2:true", "dropDb:db1"),
                helper.log,
                "FORCE must drop every table (in order, ifExists=true) before deleting the schema");
    }

    @Test
    public void forceFalseDoesNotEnumerateOrDropTables() {
        RecordingStructureHelper helper = new RecordingStructureHelper(Arrays.asList("t1", "t2"));
        MaxComputeConnectorMetadata metadata = metadataWith(helper);

        metadata.dropDatabase(null, "db1", false, false);

        // WHY: a plain (non-FORCE) DROP DB must never delete tables; over-correcting into
        // always-cascading would silently drop user data. MUTATION: making the gate
        // unconditional records dropTable calls -> red.
        Assertions.assertEquals(
                Collections.singletonList("dropDb:db1"),
                helper.log,
                "non-FORCE must drop only the schema, never the tables");
    }

    @Test
    public void forceTrueOnEmptySchemaJustDropsDb() {
        RecordingStructureHelper helper = new RecordingStructureHelper(Collections.emptyList());
        MaxComputeConnectorMetadata metadata = metadataWith(helper);

        metadata.dropDatabase(null, "db1", false, true);

        // WHY: FORCE on an empty schema must behave like a plain drop (loop is a no-op).
        Assertions.assertEquals(
                Collections.singletonList("dropDb:db1"),
                helper.log,
                "FORCE on an empty schema must just drop the schema");
    }

    @Test
    public void forceTrueSurfacesRemoteDropFailureAsConnectorException() {
        RecordingStructureHelper helper = new RecordingStructureHelper(Arrays.asList("t1", "t2"));
        helper.failOnTable = "t2";
        MaxComputeConnectorMetadata metadata = metadataWith(helper);

        // WHY (Rule 12 fail-loud): a failing remote table drop must abort the cascade BEFORE
        // the schema is deleted and surface as DorisConnectorException, not be swallowed.
        // MUTATION: catch+continue (swallow OdpsException) would let dropDb run -> red.
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.dropDatabase(null, "db1", false, true));
        Assertions.assertTrue(ex.getMessage().contains("t2"),
                "the failure must name the table that could not be dropped");
        Assertions.assertFalse(helper.log.contains("dropDb:db1"),
                "the schema must NOT be deleted after a failed table cascade");
    }

    /**
     * Recording fake: returns a fixed table list and appends an ordered marker for each
     * cascade call. Only the three methods the cascade touches are meaningful; the rest
     * return harmless defaults (they are never invoked by {@code dropDatabase}).
     */
    private static final class RecordingStructureHelper implements McStructureHelper {
        private final List<String> tables;
        private final List<String> log = new ArrayList<>();
        private String failOnTable;

        RecordingStructureHelper(List<String> tables) {
            this.tables = tables;
        }

        @Override
        public List<String> listTableNames(Odps mcClient, String dbName) {
            return tables;
        }

        @Override
        public void dropTable(Odps mcClient, String dbName, String tableName, boolean ifExists)
                throws OdpsException {
            // Record ifExists too: the cascade must pass ifExists=true (legacy
            // dropTableImpl(tbl, true)) so a duplicate/raced already-gone table does not
            // abort the cascade. Pinning it makes a true->false mutation go red.
            log.add("dropTable:" + tableName + ":" + ifExists);
            if (tableName.equals(failOnTable)) {
                throw new OdpsException("simulated remote drop failure for " + tableName);
            }
        }

        @Override
        public void dropDb(Odps mcClient, String dbName, boolean ifExists) {
            log.add("dropDb:" + dbName);
        }

        // ---- unused by dropDatabase: harmless defaults ----

        @Override
        public List<String> listDatabaseNames(Odps mcClient, String defaultProject) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(Odps mcClient, String dbName, String tableName) {
            return false;
        }

        @Override
        public boolean databaseExist(Odps mcClient, String dbName) {
            return false;
        }

        @Override
        public TableIdentifier getTableIdentifier(String dbName, String tableName) {
            return null;
        }

        @Override
        public List<Partition> getPartitions(Odps mcClient, String dbName, String tableName) {
            return Collections.emptyList();
        }

        @Override
        public Iterator<Partition> getPartitionIterator(Odps mcClient, String dbName, String tableName) {
            return Collections.emptyIterator();
        }

        @Override
        public Table getOdpsTable(Odps mcClient, String dbName, String tableName) {
            return null;
        }

        @Override
        public Tables.TableCreator createTableCreator(Odps mcClient, String dbName,
                String tableName, TableSchema schema) {
            return null;
        }

        @Override
        public void createDb(Odps mcClient, String dbName, boolean ifNotExists) {
        }
    }
}
