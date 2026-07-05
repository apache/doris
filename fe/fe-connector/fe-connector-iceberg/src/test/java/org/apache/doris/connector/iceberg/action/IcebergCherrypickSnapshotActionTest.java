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

package org.apache.doris.connector.iceberg.action;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Pins {@code cherrypick_snapshot}: cherry-picking a staged (WAP-style) snapshot into the current state.
 *
 * <p><b>WHY this matters:</b> cherry-pick produces a NEW current snapshot from an existing (staged) one. The
 * result reports (source id, new current id). Bug-for-bug: the not-found message is the generic
 * "Snapshot not found in table" (no id) and is re-wrapped under "Failed to cherry-pick snapshot ..."; the
 * post-commit current snapshot is read without a null guard.</p>
 */
public class IcebergCherrypickSnapshotActionTest {

    private static IcebergCherrypickSnapshotAction action(String snapshotId) {
        return new IcebergCherrypickSnapshotAction(
                ImmutableMap.of("snapshot_id", snapshotId), Collections.emptyList(), null);
    }

    /** Stages an append (stageOnly) and returns the staged snapshot id (the one not set as current). */
    private static long stageSnapshot(InMemoryCatalog catalog, TableIdentifier id, String file, long current) {
        Table t = catalog.loadTable(id);
        t.newAppend().stageOnly().appendFile(ActionTestTables.dataFile(file, 5L)).commit();
        Table reloaded = catalog.loadTable(id);
        long staged = -1;
        for (Snapshot s : reloaded.snapshots()) {
            if (s.snapshotId() != current) {
                staged = s.snapshotId();
            }
        }
        return staged;
    }

    @Test
    public void cherrypicksStagedSnapshotIntoCurrentState() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        long snap1 = ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        long staged = stageSnapshot(catalog, id, "staged.parquet", snap1);
        Assertions.assertNotEquals(-1L, staged);

        IcebergCherrypickSnapshotAction action = action(String.valueOf(staged));
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        long newCurrent = catalog.loadTable(id).currentSnapshot().snapshotId();
        Assertions.assertEquals(String.valueOf(staged), result.getRows().get(0).get(0),
                "source_snapshot_id is the cherry-picked snapshot");
        Assertions.assertEquals(String.valueOf(newCurrent), result.getRows().get(0).get(1),
                "current_snapshot_id is the post-commit current snapshot");
        Assertions.assertNotEquals(snap1, newCurrent, "cherry-pick advanced the current snapshot");
    }

    @Test
    public void unknownSnapshotIsReWrappedWithGenericNotFound() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);

        IcebergCherrypickSnapshotAction action = action("999999");
        action.validate();
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(catalog.loadTable(id), ActionTestTables.session("UTC")));
        Assertions.assertEquals(
                "Failed to cherry-pick snapshot 999999: Snapshot not found in table", e.getMessage());
    }

    @Test
    public void resultSchemaIsTwoBigints() {
        List<ConnectorColumn> schema = action("1").getResultSchema();
        Assertions.assertEquals(2, schema.size());
        Assertions.assertEquals("source_snapshot_id", schema.get(0).getName());
        Assertions.assertEquals("BIGINT", schema.get(0).getType().getTypeName());
        Assertions.assertFalse(schema.get(0).isNullable());
        Assertions.assertEquals("current_snapshot_id", schema.get(1).getName());
        Assertions.assertEquals("BIGINT", schema.get(1).getType().getTypeName());
        Assertions.assertFalse(schema.get(1).isNullable());
    }
}
