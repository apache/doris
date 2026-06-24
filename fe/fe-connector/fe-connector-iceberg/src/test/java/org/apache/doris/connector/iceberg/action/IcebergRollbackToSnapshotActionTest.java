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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Pins {@code rollback_to_snapshot} against a real {@link InMemoryCatalog}.
 *
 * <p><b>WHY this matters:</b> this is a destructive metadata mutation. The body must roll the table back to
 * the requested snapshot ({@code manageSnapshots().rollbackTo(id).commit()}), return the (previous, target)
 * ids, short-circuit when already on the target (no commit), and reject an unknown id with the legacy message
 * <em>before</em> the try (so it is NOT re-wrapped). The result schema is two NOT-NULL BIGINT columns. Any
 * drift here changes user-visible {@code .out} output (T08 byte-parity).</p>
 */
public class IcebergRollbackToSnapshotActionTest {

    private static IcebergRollbackToSnapshotAction action(String snapshotId) {
        return new IcebergRollbackToSnapshotAction(
                ImmutableMap.of("snapshot_id", snapshotId), Collections.emptyList(), null);
    }

    @Test
    public void rollsBackToEarlierSnapshotAndReturnsPreviousAndTarget() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        long snap1 = ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        long snap2 = ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        Assertions.assertNotEquals(snap1, snap2);

        IcebergRollbackToSnapshotAction action = action(String.valueOf(snap1));
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(
                ImmutableList.of(String.valueOf(snap2), String.valueOf(snap1)),
                result.getRows().get(0),
                "result is (previous=snap2, target=snap1)");
        Assertions.assertEquals(snap1, catalog.loadTable(id).currentSnapshot().snapshotId(),
                "the table must be rolled back to snap1");
    }

    @Test
    public void shortCircuitsWhenAlreadyOnTargetWithoutCommitting() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        long snap2 = ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        long historyBefore = catalog.loadTable(id).history().size();

        IcebergRollbackToSnapshotAction action = action(String.valueOf(snap2));
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(ImmutableList.of(String.valueOf(snap2), String.valueOf(snap2)),
                result.getRows().get(0));
        Assertions.assertEquals(historyBefore, catalog.loadTable(id).history().size(),
                "rolling back to the current snapshot must not append history (short-circuit, no commit)");
    }

    @Test
    public void rejectsUnknownSnapshotIdWithLegacyMessageBeforeWrapping() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);

        IcebergRollbackToSnapshotAction action = action("999999");
        action.validate();
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(catalog.loadTable(id), ActionTestTables.session("UTC")));
        // Legacy throws this BEFORE the try -> it is not wrapped by "Failed to rollback to snapshot ...".
        Assertions.assertTrue(e.getMessage().startsWith("Snapshot 999999 not found in table "),
                "unknown id is rejected verbatim, not wrapped: " + e.getMessage());
    }

    @Test
    public void requiresSnapshotIdArgument() {
        IcebergRollbackToSnapshotAction action = new IcebergRollbackToSnapshotAction(
                Collections.emptyMap(), Collections.emptyList(), null);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, action::validate);
        Assertions.assertEquals("Missing required argument: snapshot_id", e.getMessage());
    }

    @Test
    public void resultSchemaIsTwoNotNullBigints() {
        List<ConnectorColumn> schema = action("1").getResultSchema();
        Assertions.assertEquals(2, schema.size());
        Assertions.assertEquals("previous_snapshot_id", schema.get(0).getName());
        Assertions.assertEquals("BIGINT", schema.get(0).getType().getTypeName());
        Assertions.assertFalse(schema.get(0).isNullable());
        Assertions.assertEquals("current_snapshot_id", schema.get(1).getName());
        Assertions.assertFalse(schema.get(1).isNullable());
    }
}
