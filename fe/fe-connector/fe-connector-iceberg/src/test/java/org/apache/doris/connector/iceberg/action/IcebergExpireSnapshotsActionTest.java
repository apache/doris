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
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;

import com.google.common.collect.ImmutableList;
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
        // Pin the deleteWith path-classification deterministically. Expiring 2 of 3 append-only snapshots
        // (retain_last=1) reclaims the 2 expired snapshots' manifest-LIST files (snap-*.avro, index 4 == 2)
        // but NOT the data manifests (still referenced by the retained snapshot, index 3 == 0) nor the data
        // files themselves (index 0 == 0); there are no delete/statistics files. A mutation swapping the
        // manifest vs manifest-list branch in the deleteWith callback would flip indices 3/4 and go RED.
        Assertions.assertEquals(ImmutableList.of("0", "0", "0", "0", "2", "0"), row,
                "deleteWith classification: 2 manifest-lists reclaimed, everything else 0");
        Assertions.assertEquals(1, Iterables.size(catalog.loadTable(id).snapshots()),
                "retain_last=1 must leave exactly one snapshot");
    }

    @Test
    public void resultSchemaIsSixBigintCounters() {
        List<ConnectorColumn> schema = action(ImmutableMap.of("retain_last", "1")).getResultSchema();
        String[] names = {"deleted_data_files_count", "deleted_position_delete_files_count",
                "deleted_equality_delete_files_count", "deleted_manifest_files_count",
                "deleted_manifest_lists_count", "deleted_statistics_files_count"};
        Assertions.assertEquals(6, schema.size());
        for (int i = 0; i < 6; i++) {
            Assertions.assertEquals(names[i], schema.get(i).getName(), "column " + i + " name");
            Assertions.assertEquals("BIGINT", schema.get(i).getType().getTypeName(), "column " + i + " type");
        }
    }

    @Test
    public void rejectsNegativeOlderThanTimestamp() {
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(ImmutableMap.of("older_than", "-1")).validate());
        Assertions.assertEquals("older_than timestamp must be non-negative", e.getMessage());
    }

    @Test
    public void rejectsPartitionSpecification() {
        IcebergExpireSnapshotsAction a = new IcebergExpireSnapshotsAction(
                ImmutableMap.of("retain_last", "1"), Collections.singletonList("p1"), null);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'expire_snapshots' does not support partition specification",
                e.getMessage());
    }

    @Test
    public void rejectsWhereCondition() {
        ConnectorPredicate where =
                new ConnectorPredicate(new ConnectorLiteral(ConnectorType.of("BOOLEAN"), Boolean.TRUE));
        IcebergExpireSnapshotsAction a = new IcebergExpireSnapshotsAction(
                ImmutableMap.of("retain_last", "1"), Collections.emptyList(), where);
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class, a::validate);
        Assertions.assertEquals("Action 'expire_snapshots' does not support WHERE condition", e.getMessage());
    }
}
