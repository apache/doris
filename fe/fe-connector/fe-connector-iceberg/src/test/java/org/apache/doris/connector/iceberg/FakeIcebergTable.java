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

package org.apache.doris.connector.iceberg;

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
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Minimal offline {@link Table} double for unit tests, mirroring the paimon connector's
 * {@code FakePaimonTable}. Only the metadata read accessors that
 * {@link IcebergConnectorMetadata#getTableSchema} actually exercises — {@link #schema()},
 * {@link #spec()}, {@link #location()}, {@link #properties()} — return controlled values from the
 * constructor; every other method throws {@link UnsupportedOperationException}.
 *
 * <p>Throwing on the rest is deliberate: it documents that the metadata read path must touch
 * nothing else, and a future change that starts depending on (say) {@code newScan()} in the
 * read-only metadata path would blow up loudly in the test instead of silently passing.
 */
final class FakeIcebergTable implements Table {

    private final String name;
    private final Schema schema;
    private final PartitionSpec spec;
    private final String location;
    private final Map<String, String> properties;
    // Optional FileIO for the T09 vended-credential extraction test (extractVendedToken reads table.io()).
    // Null by default -> io() keeps its fail-loud contract; only the vended test injects one.
    private FileIO io;
    // Optional sort order for the SHOW CREATE TABLE sort-clause read path (buildShowSortClause reads
    // table.sortOrder()). Null by default -> the render path treats it as unsorted (legacy getSortOrderSql
    // guards `sortOrder == null`), so unsorted tables emit no ORDER BY; only the sort-clause test injects one.
    private SortOrder sortOrder;

    FakeIcebergTable(String name, Schema schema, PartitionSpec spec,
            String location, Map<String, String> properties) {
        this.name = name;
        this.schema = schema;
        this.spec = spec;
        this.location = location;
        this.properties = properties;
    }

    /** Inject a FileIO so {@link #io()} returns it (T09 vended-credential extraction); otherwise io() throws. */
    void setIo(FileIO io) {
        this.io = io;
    }

    /** Inject a sort order so {@link #sortOrder()} returns it (SHOW CREATE TABLE sort-clause test). */
    void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public PartitionSpec spec() {
        return spec;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public Map<String, String> properties() {
        return properties;
    }

    // ---- everything below is outside the metadata read path: fail loud if ever called ----

    @Override
    public void refresh() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableScan newScan() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, Schema> schemas() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        // The single spec keyed by its id — getScanNodeProperties' getIdentityPartitionColumns iterates this
        // (T09 location tests run getScanNodeProperties against a FakeIcebergTable).
        return Collections.singletonMap(spec.specId(), spec);
    }

    @Override
    public SortOrder sortOrder() {
        return sortOrder;
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Snapshot currentSnapshot() {
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
        if (io != null) {
            return io;
        }
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
