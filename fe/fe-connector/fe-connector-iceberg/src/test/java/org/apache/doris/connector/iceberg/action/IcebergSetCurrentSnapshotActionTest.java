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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

/**
 * Pins {@code set_current_snapshot}: the {@code snapshot_id} XOR {@code ref} validation, both resolution
 * branches, and the not-found errors.
 *
 * <p><b>WHY this matters:</b> the mutual-exclusion validation gates a destructive {@code setCurrentSnapshot}
 * commit; the ref branch resolves a branch/tag to its snapshot id (the result always reports the resolved
 * id). The not-found checks live INSIDE the try, so legacy re-wraps them under "Failed to set current
 * snapshot to ..." — a parity detail that differs from {@code rollback_to_snapshot} (which checks before the
 * try). All four validation/lookup messages are byte-checked (T08).</p>
 */
public class IcebergSetCurrentSnapshotActionTest {

    private static IcebergSetCurrentSnapshotAction action(Map<String, String> props) {
        return new IcebergSetCurrentSnapshotAction(props, Collections.emptyList(), null);
    }

    @Test
    public void rejectsWhenNeitherSnapshotIdNorRefProvided() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(Collections.emptyMap()).validate());
        Assertions.assertEquals("Either snapshot_id or ref must be provided", e.getMessage());
    }

    @Test
    public void rejectsWhenBothSnapshotIdAndRefProvided() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(ImmutableMap.of("snapshot_id", "1", "ref", "main")).validate());
        Assertions.assertEquals("snapshot_id and ref are mutually exclusive, only one can be provided",
                e.getMessage());
    }

    @Test
    public void setsCurrentBySnapshotId() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        long snap1 = ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        long snap2 = ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);

        IcebergSetCurrentSnapshotAction action = action(ImmutableMap.of("snapshot_id", String.valueOf(snap1)));
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(ImmutableList.of(String.valueOf(snap2), String.valueOf(snap1)),
                result.getRows().get(0));
        Assertions.assertEquals(snap1, catalog.loadTable(id).currentSnapshot().snapshotId());
    }

    @Test
    public void setsCurrentByRefResolvingToSnapshotId() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        long snap1 = ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        long snap2 = ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        Table tagged = catalog.loadTable(id);
        tagged.manageSnapshots().createTag("v1", snap1).commit();

        IcebergSetCurrentSnapshotAction action = action(ImmutableMap.of("ref", "v1"));
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(ImmutableList.of(String.valueOf(snap2), String.valueOf(snap1)),
                result.getRows().get(0),
                "the ref resolves to snap1, which becomes current");
        Assertions.assertEquals(snap1, catalog.loadTable(id).currentSnapshot().snapshotId());
    }

    @Test
    public void unknownSnapshotIdIsReWrappedUnderFailedToSetCurrent() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);

        IcebergSetCurrentSnapshotAction action = action(ImmutableMap.of("snapshot_id", "999999"));
        action.validate();
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(catalog.loadTable(id), ActionTestTables.session("UTC")));
        // Inside the try -> wrapped (contrast rollback_to_snapshot, which throws before the try).
        Assertions.assertTrue(e.getMessage().startsWith("Failed to set current snapshot to snapshot 999999: "),
                e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("Snapshot 999999 not found in table "), e.getMessage());
    }

    @Test
    public void unknownRefIsReWrappedWithReferenceWording() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);

        IcebergSetCurrentSnapshotAction action = action(ImmutableMap.of("ref", "nope"));
        action.validate();
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(catalog.loadTable(id), ActionTestTables.session("UTC")));
        Assertions.assertTrue(e.getMessage().startsWith("Failed to set current snapshot to reference 'nope': "),
                e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("Reference 'nope' not found in table "), e.getMessage());
    }
}
