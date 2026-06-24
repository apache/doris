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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Pins {@code publish_changes}: the WAP (write-audit-publish) pattern that finds a snapshot tagged with a
 * given {@code wap.id} and cherry-picks it.
 *
 * <p><b>WHY this matters:</b> two bug-for-bug quirks drive user-visible output. The result columns are
 * {@code STRING} (not {@code BIGINT}), and a null snapshot id renders as the literal {@code "null"} (not SQL
 * NULL). The WAP snapshot is located by a linear scan over {@code snapshots()} comparing
 * {@code summary().get("wap.id")}. A missing wap.id is rejected with the exact legacy message.</p>
 */
public class IcebergPublishChangesActionTest {

    private static IcebergPublishChangesAction action(String wapId) {
        return new IcebergPublishChangesAction(
                ImmutableMap.of("wap_id", wapId), Collections.emptyList(), null);
    }

    /** Stages a WAP append carrying {@code wap.id} in its snapshot summary. */
    private static void stageWap(InMemoryCatalog catalog, TableIdentifier id, String wapId) {
        Table t = catalog.loadTable(id);
        t.newAppend().stageOnly().set("wap.id", wapId)
                .appendFile(ActionTestTables.dataFile("wap-" + wapId + ".parquet", 5L)).commit();
    }

    @Test
    public void publishesWapSnapshotReturningStringIds() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        long snap1 = ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        stageWap(catalog, id, "wap-123");

        IcebergPublishChangesAction action = action("wap-123");
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        long newCurrent = catalog.loadTable(id).currentSnapshot().snapshotId();
        Assertions.assertEquals(String.valueOf(snap1), result.getRows().get(0).get(0),
                "previous_snapshot_id is the pre-publish current snapshot");
        Assertions.assertEquals(String.valueOf(newCurrent), result.getRows().get(0).get(1));
        Assertions.assertNotEquals(snap1, newCurrent, "publish advanced the current snapshot");
    }

    @Test
    public void rendersNullPreviousSnapshotAsLiteralNull() {
        // Empty table -> currentSnapshot() is null when the body reads the previous id -> the literal "null".
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        stageWap(catalog, id, "wap-empty");

        IcebergPublishChangesAction action = action("wap-empty");
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals("null", result.getRows().get(0).get(0),
                "a null previous snapshot id is the literal string \"null\", not SQL NULL");
    }

    @Test
    public void rejectsMissingWapIdWithLegacyMessage() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);

        IcebergPublishChangesAction action = action("missing");
        action.validate();
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action.execute(catalog.loadTable(id), ActionTestTables.session("UTC")));
        Assertions.assertEquals("Cannot find snapshot with wap.id = missing", e.getMessage());
    }

    @Test
    public void resultSchemaIsTwoStrings() {
        List<ConnectorColumn> schema = action("x").getResultSchema();
        Assertions.assertEquals("STRING", schema.get(0).getType().getTypeName());
        Assertions.assertEquals("STRING", schema.get(1).getType().getTypeName());
    }
}
