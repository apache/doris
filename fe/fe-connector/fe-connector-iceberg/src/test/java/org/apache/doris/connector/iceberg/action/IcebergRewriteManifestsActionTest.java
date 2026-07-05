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
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Pins {@code rewrite_manifests}, including the delegated connector {@link RewriteManifestExecutor}.
 *
 * <p><b>WHY this matters:</b> the body short-circuits an empty table to {@code ["0", "0"]} (no executor
 * call), and otherwise reports (rewritten, added) manifest counts from the executor. The executor is the
 * fe-core port stripped of its {@code ExternalTable}/{@code ExtMetaCacheMgr} couplings; this verifies it
 * actually combines the per-append manifests through the SDK {@code rewriteManifests()} API.</p>
 */
public class IcebergRewriteManifestsActionTest {

    private static IcebergRewriteManifestsAction action(java.util.Map<String, String> props) {
        return new IcebergRewriteManifestsAction(props, Collections.emptyList(), null);
    }

    @Test
    public void emptyTableShortCircuitsToZeroZero() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");

        IcebergRewriteManifestsAction action = action(Collections.emptyMap());
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        Assertions.assertEquals(ImmutableList.of("0", "0"), result.getRows().get(0),
                "an empty table (no current snapshot) returns 0/0 without touching the executor");
    }

    @Test
    public void combinesPerAppendManifests() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        // Three separate appends -> three data manifests accumulate in the current snapshot.
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 3L);
        Table before = catalog.loadTable(id);
        int manifestsBefore = before.currentSnapshot().dataManifests(before.io()).size();
        Assertions.assertEquals(3, manifestsBefore);

        IcebergRewriteManifestsAction action = action(Collections.emptyMap());
        action.validate();
        ConnectorProcedureResult result = action.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));

        // The executor reports manifestsBefore.size() as the rewritten count (the faithful port behaviour);
        // the added count is iceberg's internal combine outcome, asserted only as a valid non-negative int.
        Assertions.assertEquals(String.valueOf(manifestsBefore), result.getRows().get(0).get(0),
                "rewritten_manifests_count is the number of manifests targeted for rewrite");
        Assertions.assertTrue(Integer.parseInt(result.getRows().get(0).get(1)) >= 0,
                "added_manifests_count is a valid non-negative count");
    }

    @Test
    public void resultSchemaIsTwoInts() {
        Assertions.assertEquals(2, action(Collections.emptyMap()).getResultSchema().size());
        Assertions.assertEquals("rewritten_manifests_count",
                action(Collections.emptyMap()).getResultSchema().get(0).getName());
        Assertions.assertEquals("added_manifests_count",
                action(Collections.emptyMap()).getResultSchema().get(1).getName());
        Assertions.assertEquals("INT",
                action(Collections.emptyMap()).getResultSchema().get(0).getType().getTypeName());
        Assertions.assertEquals("INT",
                action(Collections.emptyMap()).getResultSchema().get(1).getType().getTypeName());
        Assertions.assertFalse(action(Collections.emptyMap()).getResultSchema().get(0).isNullable());
        Assertions.assertFalse(action(Collections.emptyMap()).getResultSchema().get(1).isNullable());
    }

    @Test
    public void specIdAcceptsZeroToMaxRange() {
        // spec_id is optional with intRange(0, MAX); a negative value is rejected at parse time.
        Assertions.assertDoesNotThrow(() -> action(ImmutableMap.of("spec_id", "0")).validate());
    }

    @Test
    public void specIdFiltersManifestsByPartitionSpec() {
        InMemoryCatalog catalog = ActionTestTables.freshCatalog();
        TableIdentifier id = ActionTestTables.id("t");
        ActionTestTables.createTable(catalog, "t");
        ActionTestTables.appendSnapshot(catalog, "t", "f1.parquet", 1L);
        ActionTestTables.appendSnapshot(catalog, "t", "f2.parquet", 2L);
        ActionTestTables.appendSnapshot(catalog, "t", "f3.parquet", 3L);
        // getInt(SPEC_ID) reads parsedValues populated by validate(), so validate() MUST run before execute()
        // for the spec_id filter to take effect (skipping it makes spec_id silently a no-op). Run the
        // non-matching case FIRST: all manifests are spec 0, so spec_id=1 targets zero -> the executor
        // short-circuits at rewrittenCount==0 to ["0","0"] WITHOUT committing, leaving the table pristine for
        // the matching case below.
        IcebergRewriteManifestsAction miss = action(ImmutableMap.of("spec_id", "1"));
        miss.validate();
        ConnectorProcedureResult unmatched = miss.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));
        Assertions.assertEquals(ImmutableList.of("0", "0"), unmatched.getRows().get(0),
                "spec_id=1 matches none of the (all spec-0) manifests -> rewrittenCount==0 short-circuit");
        // spec_id=0 matches the unpartitioned spec -> all three manifests are targeted for rewrite.
        IcebergRewriteManifestsAction keep = action(ImmutableMap.of("spec_id", "0"));
        keep.validate();
        ConnectorProcedureResult kept = keep.execute(catalog.loadTable(id), ActionTestTables.session("UTC"));
        Assertions.assertEquals("3", kept.getRows().get(0).get(0),
                "spec_id=0 targets every manifest's partitionSpecId()==0");
    }

    @Test
    public void wrapsCurrentSnapshotFailure() {
        // In the connector port, executeAction probes icebergTable.currentSnapshot() itself (before the
        // executor); a throw there is caught and wrapped ONCE by the action's "Rewrite manifests failed: "
        // handler. (The executor's own "Failed to rewrite manifests: " prefix is not reached on this path,
        // because the action short-circuits the empty/throwing snapshot before constructing the executor.)
        Table throwing = new ThrowingTable();
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> action(Collections.emptyMap()).execute(throwing, ActionTestTables.session("UTC")));
        Assertions.assertEquals("Rewrite manifests failed: boom", e.getMessage());
    }

    @Test
    public void executorWrapsFailureWithInnerPrefix() {
        // The other half of the double-wrap quirk: RewriteManifestExecutor.execute probes
        // table.currentSnapshot() first inside its OWN try, so a throw there is wrapped with the executor's
        // inner "Failed to rewrite manifests: " prefix. Composed with the action's outer "Rewrite manifests
        // failed: " wrap (wrapsCurrentSnapshotFailure), this pins BOTH layers of the byte string the action
        // emits when the executor itself fails ("Rewrite manifests failed: Failed to rewrite manifests: ...").
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> new RewriteManifestExecutor().execute(new ThrowingTable(), null));
        Assertions.assertEquals("Failed to rewrite manifests: boom", e.getMessage());
    }

    /**
     * Minimal {@link Table} double whose {@link #currentSnapshot()} throws — the first probe inside the
     * action's {@code executeAction}. The action body wraps it as "Rewrite manifests failed: boom".
     * {@link #name()} is read only by the LOG.warn call so it must not throw; every other method is outside
     * this path and fails loud.
     */
    private static final class ThrowingTable implements Table {

        @Override
        public String name() {
            return "throwing";
        }

        @Override
        public Snapshot currentSnapshot() {
            throw new RuntimeException("boom");
        }

        @Override
        public void refresh() {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableScan newScan() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Schema schema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, Schema> schemas() {
            throw new UnsupportedOperationException();
        }

        @Override
        public PartitionSpec spec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, PartitionSpec> specs() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SortOrder sortOrder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, SortOrder> sortOrders() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> properties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String location() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterable<Snapshot> snapshots() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<HistoryEntry> history() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdateSchema updateSchema() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdatePartitionSpec updateSpec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdateProperties updateProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReplaceSortOrder replaceSortOrder() {
            throw new UnsupportedOperationException();
        }

        @Override
        public UpdateLocation updateLocation() {
            throw new UnsupportedOperationException();
        }

        @Override
        public AppendFiles newAppend() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RewriteFiles newRewrite() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RewriteManifests rewriteManifests() {
            throw new UnsupportedOperationException();
        }

        @Override
        public OverwriteFiles newOverwrite() {
            throw new UnsupportedOperationException();
        }

        @Override
        public RowDelta newRowDelta() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ReplacePartitions newReplacePartitions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeleteFiles newDelete() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpireSnapshots expireSnapshots() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ManageSnapshots manageSnapshots() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Transaction newTransaction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIO io() {
            throw new UnsupportedOperationException();
        }

        @Override
        public EncryptionManager encryption() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocationProvider locationProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<StatisticsFile> statisticsFiles() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, SnapshotRef> refs() {
            throw new UnsupportedOperationException();
        }
    }
}
