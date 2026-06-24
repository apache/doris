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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Pins {@code expire_snapshots}: the validation pass and the six-counter result.
 *
 * <p><b>WHY this matters:</b> this is the destructive GC procedure with the widest argument surface and a
 * fixed six-column ({@code BIGINT}) result built from {@code deleteWith}-callback counters. The validation
 * messages (at-least-one-required, the older_than/snapshot_ids format errors) are byte-checked. The happy
 * path proves {@code retain_last} actually expires snapshots through the SDK and that the result row matches
 * the schema width (the single-row contract over six counters).</p>
 */
public class IcebergExpireSnapshotsActionTest {

    private static IcebergExpireSnapshotsAction action(Map<String, String> props) {
        return new IcebergExpireSnapshotsAction(props, Collections.emptyList(), null);
    }

    @Test
    public void requiresAtLeastOneOfOlderThanRetainLastSnapshotIds() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(Collections.emptyMap()).validate());
        Assertions.assertEquals("At least one of 'older_than', 'retain_last', or "
                + "'snapshot_ids' must be specified", e.getMessage());
    }

    @Test
    public void rejectsBadOlderThanFormat() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(ImmutableMap.of("older_than", "yesterday")).validate());
        Assertions.assertEquals("Invalid older_than format. Expected ISO datetime "
                + "(yyyy-MM-ddTHH:mm:ss) or timestamp in milliseconds: yesterday", e.getMessage());
    }

    @Test
    public void rejectsBadSnapshotIdsFormat() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(ImmutableMap.of("snapshot_ids", "1,abc,3")).validate());
        Assertions.assertEquals("Invalid snapshot_id format: abc", e.getMessage());
    }

    @Test
    public void expiresOldSnapshotsKeepingRetainLastAndReturnsSixCounters() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 3L);
        Assertions.assertEquals(3, Iterables.size(catalog.loadTable(id).snapshots()));

        IcebergExpireSnapshotsAction action = action(ImmutableMap.of("retain_last", "1"));
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        List<String> row = result.getRows().get(0);
        Assertions.assertEquals(6, row.size(), "expire_snapshots returns the six-counter Spark schema");
        for (String count : row) {
            Assertions.assertDoesNotThrow(() -> Long.parseLong(count), "every counter is a number: " + count);
        }
        Assertions.assertEquals(1, Iterables.size(catalog.loadTable(id).snapshots()),
                "retain_last=1 must leave exactly one snapshot");
    }

    @Test
    public void resultSchemaIsSixBigintCounters() {
        Assertions.assertEquals(6, action(ImmutableMap.of("retain_last", "1")).getResultSchema().size());
        Assertions.assertEquals("deleted_data_files_count",
                action(ImmutableMap.of("retain_last", "1")).getResultSchema().get(0).getName());
        Assertions.assertEquals("BIGINT",
                action(ImmutableMap.of("retain_last", "1")).getResultSchema().get(0).getType().getTypeName());
    }
}
