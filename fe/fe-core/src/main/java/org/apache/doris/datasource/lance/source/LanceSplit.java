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

package org.apache.doris.datasource.lance.source;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.FileSplit;
import org.apache.doris.datasource.TableFormatType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * A Lance scan split. Ordinary catalog scans use one or more fixed-version fragments per split.
 * Indexed scans can use one physical index segment and its covered fragments per split.
 * Backend-local TVFs use one whole-dataset latest-version split.
 */
public class LanceSplit extends FileSplit {
    public enum Kind {
        /** A nonempty fragment range pinned to a dataset version. */
        FRAGMENTS,
        /** One physical index segment and its nonempty visible fragment range. */
        INDEX_SEGMENT,
        /** A fixed-version row-count carrier; an empty dataset may have no fragments. */
        METADATA_COUNT,
        /** A backend-local TVF resolves latest at execution, using version zero on the wire. */
        WHOLE_DATASET_LATEST;

        boolean requiresFixedVersion() {
            return this != WHOLE_DATASET_LATEST;
        }

        boolean requiresFragments() {
            return this == FRAGMENTS || this == INDEX_SEGMENT;
        }
    }

    private final Kind kind;
    private final boolean scalarIndexDisabled;
    private final String datasetUri;
    private final long version;
    private final List<Long> fragmentIds;
    private final UUID indexSegmentUuid;
    // Set to a nonnegative value only when this split carries a metadata COUNT(*) result so BE can
    // synthesize that many rows instead of scanning fragments. -1 means ordinary scan.
    private final long tableLevelRowCount;

    /** Scans fixed-version fragments, allowing Lance to select scalar indexes. */
    public static LanceSplit scanFragments(
            String datasetUri, long version, List<Long> fragmentIds, long physicalRows) {
        return new LanceSplit(datasetUri, version, fragmentIds, null, physicalRows,
                Kind.FRAGMENTS, -1, false);
    }

    /** Scans uncovered fragments without reusing scalar indexes selected for other splits. */
    static LanceSplit scanFragmentsWithoutScalarIndex(String datasetUri, long version, List<Long> fragmentIds,
            long physicalRows) {
        return new LanceSplit(datasetUri, version, fragmentIds, null, physicalRows,
                Kind.FRAGMENTS, -1, true);
    }

    /** Defers snapshot resolution to the BE for a backend-local TVF. */
    public static LanceSplit scanLatestDataset(String datasetUri) {
        return new LanceSplit(datasetUri, 0, Collections.emptyList(), null, 1,
                Kind.WHOLE_DATASET_LATEST, -1, false);
    }

    // A metadata COUNT(*) carrier pinned to the planned snapshot. Its fragment range remains valid
    // input if BE falls back to scanning, while rowCount lets the metadata path skip that scan.
    public static LanceSplit metadataCount(String datasetUri, long version, List<Long> fragmentIds,
            long rowCount, long physicalRows) {
        return new LanceSplit(datasetUri, version, fragmentIds, null, physicalRows,
                Kind.METADATA_COUNT, rowCount, false);
    }

    /** Pins one physical index segment and its visible fragments to the planned snapshot. */
    public static LanceSplit scanIndexSegment(String datasetUri, long version, UUID indexSegmentUuid,
            List<Long> fragmentIds, long physicalRows) {
        return new LanceSplit(datasetUri, version, fragmentIds,
                indexSegmentUuid, physicalRows, Kind.INDEX_SEGMENT, -1, false);
    }

    private LanceSplit(String datasetUri, long version, List<Long> fragmentIds,
            UUID indexSegmentUuid, long physicalRows, Kind kind, long rowCount, boolean disableScalarIndex) {
        super(LocationPath.of(requireDatasetUri(datasetUri)), 0, 0, 0, 0, null,
                Collections.emptyList());
        if (kind.requiresFixedVersion() && version <= 0) {
            throw new IllegalArgumentException("Lance snapshot version must be positive");
        }
        if (fragmentIds == null) {
            throw new IllegalArgumentException("Lance fragment IDs must not be null");
        }
        if (kind.requiresFragments() && fragmentIds.isEmpty()) {
            throw new IllegalArgumentException("Lance scan split must contain fragments");
        }
        if (kind == Kind.METADATA_COUNT && rowCount < 0) {
            throw new IllegalArgumentException("Lance metadata count must be non-negative");
        }
        this.kind = kind;
        this.tableLevelRowCount = rowCount;
        this.scalarIndexDisabled = disableScalarIndex;
        for (Long fragmentId : fragmentIds) {
            if (fragmentId == null || fragmentId < 0) {
                throw new IllegalArgumentException("Lance fragment id must be non-negative");
            }
        }
        if (kind == Kind.INDEX_SEGMENT && indexSegmentUuid == null) {
            throw new IllegalArgumentException("Lance index segment UUID must not be null");
        }
        this.datasetUri = datasetUri;
        this.version = version;
        this.fragmentIds = Collections.unmodifiableList(new ArrayList<>(fragmentIds));
        this.indexSegmentUuid = indexSegmentUuid;
        this.tableFormatType = TableFormatType.LANCE;
        this.selfSplitWeight = Math.max(physicalRows, 1);
    }

    private static String requireDatasetUri(String datasetUri) {
        if (datasetUri == null || datasetUri.trim().isEmpty()) {
            throw new IllegalArgumentException("Lance dataset URI must not be empty");
        }
        return datasetUri;
    }

    public Kind getKind() {
        return kind;
    }

    public boolean isMetadataCount() {
        return kind == Kind.METADATA_COUNT;
    }

    public boolean isScalarIndexDisabled() {
        return scalarIndexDisabled;
    }

    public String getDatasetUri() {
        return datasetUri;
    }

    public long getVersion() {
        return version;
    }

    public List<Long> getFragmentIds() {
        return fragmentIds;
    }

    public boolean hasFragmentIds() {
        return !fragmentIds.isEmpty();
    }

    public Optional<UUID> getIndexSegmentUuid() {
        return Optional.ofNullable(indexSegmentUuid);
    }

    public long getTableLevelRowCount() {
        return tableLevelRowCount;
    }

    @Override
    public String getConsistentHashString() {
        return hasFragmentIds()
                ? datasetUri + "#" + version + "#" + fragmentIds + "#" + indexSegmentUuid
                : datasetUri + "#" + (version == 0 ? "latest" : version) + "#all";
    }
}
