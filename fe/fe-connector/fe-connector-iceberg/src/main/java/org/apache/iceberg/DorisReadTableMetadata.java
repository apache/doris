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

package org.apache.iceberg;

import java.util.List;
import java.util.stream.Collectors;

/** Uses the pinned SDK's package-private constructor to isolate mutable snapshot caches for read-only operations. */
public final class DorisReadTableMetadata {
    private DorisReadTableMetadata() {
    }

    /** Copies trusted catalog metadata without sharing decrypted snapshot state or eagerly fetching history. */
    public static TableMetadata copy(TableMetadata metadata) {
        List<Snapshot> references = metadata.refs().values().stream().map(SnapshotRef::snapshotId).distinct()
                .map(metadata::snapshot).map(DorisReadTableMetadata::copySnapshot)
                .collect(Collectors.toUnmodifiableList());
        Snapshot current = metadata.currentSnapshot();
        // buildFrom/toJson eagerly fetch all snapshots; suppressHistoricalSnapshots also drops their statistics.
        // Copy only referenced snapshot records now and preserve the catalog's lazy history supplier and metadata.
        return new TableMetadata(metadata.metadataFileLocation(), metadata.formatVersion(), metadata.uuid(),
                metadata.location(), metadata.lastSequenceNumber(), metadata.lastUpdatedMillis(),
                metadata.lastColumnId(),
                metadata.currentSchemaId(), metadata.schemas(), metadata.defaultSpecId(), metadata.specs(),
                metadata.lastAssignedPartitionId(), metadata.defaultSortOrderId(), metadata.sortOrders(),
                metadata.properties(), current == null ? -1 : current.snapshotId(), references,
                () -> metadata.snapshots().stream().map(DorisReadTableMetadata::copySnapshot)
                        .collect(Collectors.toUnmodifiableList()),
                metadata.snapshotLog(), metadata.previousFiles(), metadata.refs(), metadata.statisticsFiles(),
                metadata.partitionStatisticsFiles(), metadata.nextRowId(), metadata.encryptionKeys(), List.of());
    }

    private static Snapshot copySnapshot(Snapshot snapshot) {
        // Snapshot JSON stores manifest-list references, never the SDK's memoized decrypted manifest descriptors.
        return SnapshotParser.fromJson(SnapshotParser.toJson(snapshot));
    }
}
