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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;

public class IcebergSnapshotCacheValue {

    private final IcebergPartitionInfo partitionInfo;
    private final IcebergSnapshot snapshot;
    private final Optional<Map<Integer, List<String>>> nameMapping;
    private final Optional<Table> icebergTable;
    private final ThreadPoolExecutor planningExecutor;

    public IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot) {
        this(partitionInfo, snapshot, Optional.empty(), Optional.empty(), null);
    }

    public IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot,
            Optional<Map<Integer, List<String>>> nameMapping) {
        this(partitionInfo, snapshot, nameMapping, Optional.empty(), null);
    }

    public IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot,
            Optional<Map<Integer, List<String>>> nameMapping, Table icebergTable) {
        this(partitionInfo, snapshot, nameMapping, Optional.of(icebergTable), null);
    }

    public IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot,
            Optional<Map<Integer, List<String>>> nameMapping, Table icebergTable,
            ThreadPoolExecutor planningExecutor) {
        this(partitionInfo, snapshot, nameMapping, Optional.of(icebergTable), planningExecutor);
    }

    private IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot,
            Optional<Map<Integer, List<String>>> nameMapping, Optional<Table> icebergTable,
            ThreadPoolExecutor planningExecutor) {
        this.partitionInfo = partitionInfo;
        this.snapshot = snapshot;
        // A cached BaseTable shares live TableOperations; retain a metadata-only generation so a
        // later commit through that same Table cannot move an already bound statement forward.
        this.icebergTable = icebergTable.map(IcebergSnapshotCacheValue::retainTableGeneration);
        this.planningExecutor = planningExecutor;
        this.nameMapping = nameMapping.map(mapping -> {
            Map<Integer, List<String>> copy = new HashMap<>();
            // Preserve the immutable snapshot contract while remaining compatible with branch-4.1's Java target.
            mapping.forEach((id, names) -> copy.put(id,
                    Collections.unmodifiableList(new ArrayList<>(names))));
            return Collections.unmodifiableMap(copy);
        });
    }

    public IcebergPartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public IcebergSnapshot getSnapshot() {
        return snapshot;
    }

    public Optional<Map<Integer, List<String>>> getNameMapping() {
        return nameMapping;
    }

    public Optional<Table> getIcebergTable() {
        return icebergTable;
    }

    public ThreadPoolExecutor getPlanningExecutor() {
        return planningExecutor;
    }

    static Table retainTableGeneration(Table table) {
        if (!(table instanceof HasTableOperations) || isFrozenGeneration(table)) {
            return table;
        }
        TableOperations operations = ((HasTableOperations) table).operations();
        // Capture current() exactly once so every projection derived from the returned table sees
        // one metadata generation even when the shared catalog handle refreshes concurrently.
        TableOperations frozenOperations = new FrozenTableOperations(operations, operations.current());
        return tableWithOperations(table, frozenOperations);
    }

    static boolean isFrozenGeneration(Table table) {
        return table instanceof HasTableOperations
                && ((HasTableOperations) table).operations() instanceof FrozenTableOperations;
    }

    static Table createWritableTable(Table retainedTable, Table liveTable) {
        if (!isFrozenGeneration(retainedTable)) {
            return retainedTable;
        }
        if (!(liveTable instanceof HasTableOperations)
                || isFrozenGeneration(liveTable)) {
            throw new IllegalArgumentException(
                    "Iceberg commit table must provide writable table operations");
        }
        TableMetadata retainedMetadata = ((HasTableOperations) retainedTable).operations().current();
        TableOperations liveOperations = ((HasTableOperations) liveTable).operations();
        return tableWithOperations(retainedTable,
                new WritableTableOperations(liveOperations, retainedMetadata));
    }

    private static Table tableWithOperations(Table table, TableOperations operations) {
        if (table instanceof BaseTable) {
            return new BaseTable(operations, table.name(), ((BaseTable) table).reporter());
        }
        return new BaseTable(operations, table.name());
    }

    private abstract static class RetainedTableOperations implements TableOperations {
        protected final TableOperations delegate;
        private final TableMetadata metadata;

        private RetainedTableOperations(TableOperations delegate, TableMetadata metadata) {
            this.delegate = delegate;
            this.metadata = metadata;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public FileIO io() {
            return delegate.io();
        }

        @Override
        public EncryptionManager encryption() {
            return delegate.encryption();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return delegate.metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider() {
            return delegate.locationProvider();
        }
    }

    private static class FrozenTableOperations extends RetainedTableOperations {
        private FrozenTableOperations(TableOperations delegate, TableMetadata metadata) {
            super(delegate, metadata);
        }

        @Override
        public void commit(TableMetadata base, TableMetadata newMetadata) {
            throw new UnsupportedOperationException("Frozen Iceberg table generation is read-only");
        }
    }

    private static class WritableTableOperations extends RetainedTableOperations {
        private final TableMetadata retainedMetadata;
        private TableMetadata currentMetadata;

        private WritableTableOperations(TableOperations delegate, TableMetadata retainedMetadata) {
            super(delegate, retainedMetadata);
            this.retainedMetadata = retainedMetadata;
            this.currentMetadata = retainedMetadata;
        }

        @Override
        public TableMetadata current() {
            return currentMetadata;
        }

        @Override
        public TableMetadata refresh() {
            TableMetadata refreshedMetadata = delegate.refresh();
            // Data-only snapshot advances are safe to replay, but a changed writer contract must
            // fail instead of silently committing files produced for another metadata generation.
            if (!isWriterCompatible(refreshedMetadata)) {
                throw new CommitFailedException(
                        "Cannot retry Iceberg commit after schema, spec, sort order, location, "
                                + "format version, or table properties changed");
            }
            currentMetadata = refreshedMetadata;
            return refreshedMetadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata newMetadata) {
            delegate.commit(base, newMetadata);
            currentMetadata = newMetadata;
        }

        private boolean isWriterCompatible(TableMetadata refreshedMetadata) {
            return retainedMetadata.formatVersion() == refreshedMetadata.formatVersion()
                    && retainedMetadata.currentSchemaId() == refreshedMetadata.currentSchemaId()
                    && retainedMetadata.defaultSpecId() == refreshedMetadata.defaultSpecId()
                    && retainedMetadata.defaultSortOrderId() == refreshedMetadata.defaultSortOrderId()
                    && Objects.equals(retainedMetadata.location(), refreshedMetadata.location())
                    && Objects.equals(retainedMetadata.properties(), refreshedMetadata.properties());
        }
    }
}
