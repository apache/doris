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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * One BE scan task: fragments of one dataset version.
 *
 * <p>BE opens the dataset itself through lance-c, from {@code TLanceFileDesc.dataset_uri} and
 * {@code version}, and reads only the listed fragments; an empty fragment list would make it read the whole
 * dataset, so a range always carries at least one, except a metadata COUNT(*) carrier of a version without
 * fragments. The range has no byte extent of its own: its path is the dataset URI, which decides the file
 * type the engine reports to BE, and offsets and sizes stay 0.
 *
 * <p>Besides plain fragments a range can be one physical scalar-index segment with the fragments it covers,
 * the uncovered fragments next to such segments (read without the scalar index, whose segments other ranges
 * already search), or a carrier of a metadata COUNT(*): BE emits the carried row count without opening a
 * scanner, and a BE that cannot do that scans the carrier's fragments of the same version instead.
 */
final class LanceScanRange implements ConnectorScanRange {
    private static final long serialVersionUID = 1L;
    /** The reader key BE dispatches on ({@code FileScannerV2} selects its Lance reader by it). */
    static final String TABLE_FORMAT = "lance";
    private static final long NONE = -1;

    private final String datasetUri;
    private final long version;
    private final ArrayList<Long> fragmentIds;
    private final long physicalRows;
    private final long targetSplitSize;
    /** The scalar-index segment this range searches; null for a fragment range. */
    private final UUID indexSegmentUuid;
    private final boolean scalarIndexDisabled;
    private final long limit;
    /** The rows BE emits for a metadata COUNT(*) carrier; -1 for a range BE reads. */
    private final long tableLevelRowCount;
    /** The count of the whole plan that a COUNT(*) carrier belongs to; -1 for a range BE reads. */
    private final long pushDownRowCount;

    /**
     * @param physicalRows the rows the fragments store before deletions, which is what reading them costs
     * @param targetSplitSize the largest {@code physicalRows} of the plan; split weights are relative to it
     */
    LanceScanRange(String datasetUri, long version, List<Long> fragmentIds, long physicalRows,
            long targetSplitSize) {
        this(datasetUri, version, fragmentIds, physicalRows, targetSplitSize, null, false, NONE, NONE, NONE);
    }

    private LanceScanRange(String datasetUri, long version, List<Long> fragmentIds, long physicalRows,
            long targetSplitSize, UUID indexSegmentUuid, boolean scalarIndexDisabled, long limit,
            long tableLevelRowCount, long pushDownRowCount) {
        if (version <= 0) {
            throw new IllegalArgumentException("Lance snapshot version must be positive");
        }
        if (fragmentIds.isEmpty() && tableLevelRowCount < 0) {
            throw new IllegalArgumentException("Lance scan range must contain fragments");
        }
        for (Long fragmentId : fragmentIds) {
            if (fragmentId == null || fragmentId < 0) {
                throw new IllegalArgumentException("Lance fragment id must be non-negative");
            }
        }
        this.datasetUri = datasetUri;
        this.version = version;
        this.fragmentIds = new ArrayList<>(fragmentIds);
        this.physicalRows = physicalRows;
        this.targetSplitSize = targetSplitSize;
        this.indexSegmentUuid = indexSegmentUuid;
        this.scalarIndexDisabled = scalarIndexDisabled;
        this.limit = limit;
        this.tableLevelRowCount = tableLevelRowCount;
        this.pushDownRowCount = pushDownRowCount;
    }

    /**
     * Fragments read with BE's own index selection, or without any scalar index when
     * {@code scalarIndexDisabled}; {@code limit} is -1 or a bound safe to apply to these fragments alone.
     */
    static LanceScanRange fragments(String datasetUri, long version, List<Long> fragmentIds, long physicalRows,
            long targetSplitSize, boolean scalarIndexDisabled, long limit) {
        return new LanceScanRange(datasetUri, version, fragmentIds, physicalRows, targetSplitSize, null,
                scalarIndexDisabled, limit, NONE, NONE);
    }

    /** One physical scalar-index segment and the visible fragments it covers. */
    static LanceScanRange indexSegment(String datasetUri, long version, UUID indexSegmentUuid,
            List<Long> fragmentIds, long physicalRows, long targetSplitSize, long limit) {
        return new LanceScanRange(datasetUri, version, fragmentIds, physicalRows, targetSplitSize,
                indexSegmentUuid, false, limit, NONE, NONE);
    }

    /**
     * A metadata COUNT(*) carrier: BE emits {@code rowCount} rows, the logical rows of these fragments, and
     * {@code planRowCount} is the count of the whole plan, which EXPLAIN shows.
     */
    static LanceScanRange metadataCount(String datasetUri, long version, List<Long> fragmentIds, long rowCount,
            long planRowCount, long physicalRows, long targetSplitSize) {
        if (rowCount < 0) {
            throw new IllegalArgumentException("Lance metadata count must be non-negative");
        }
        return new LanceScanRange(datasetUri, version, fragmentIds, physicalRows, targetSplitSize, null, false,
                NONE, rowCount, planRowCount);
    }

    String getDatasetUri() {
        return datasetUri;
    }

    long getVersion() {
        return version;
    }

    List<Long> getFragmentIds() {
        return Collections.unmodifiableList(fragmentIds);
    }

    Optional<UUID> getIndexSegmentUuid() {
        return Optional.ofNullable(indexSegmentUuid);
    }

    boolean isScalarIndexDisabled() {
        return scalarIndexDisabled;
    }

    long getLimit() {
        return limit;
    }

    long getTableLevelRowCount() {
        return tableLevelRowCount;
    }

    @Override
    public Optional<String> getPath() {
        return Optional.of(datasetUri);
    }

    /** What consistent hashing assigns by; ranges over different fragments must not collapse onto one BE. */
    @Override
    public String getSplitIdentity() {
        return datasetUri + "#" + version + "#" + fragmentIds + "#" + indexSegmentUuid;
    }

    @Override
    public long getStart() {
        return 0;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public long getFileSize() {
        return 0;
    }

    @Override
    public String getFileFormat() {
        return TABLE_FORMAT;
    }

    /** Never below 1: an empty fragment still costs a scanner. */
    @Override
    public long getSelfSplitWeight() {
        return Math.max(physicalRows, 1);
    }

    @Override
    public long getTargetSplitSize() {
        return targetSplitSize;
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public String getTableFormatType() {
        return TABLE_FORMAT;
    }

    @Override
    public long getPushDownRowCount() {
        return pushDownRowCount;
    }

    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        TLanceFileDesc lanceParams = new TLanceFileDesc();
        lanceParams.setDatasetUri(datasetUri);
        lanceParams.setVersion(version);
        if (!fragmentIds.isEmpty()) {
            lanceParams.setFragmentIds(new ArrayList<>(fragmentIds));
        }
        if (indexSegmentUuid != null) {
            ByteBuffer uuidBytes = ByteBuffer.allocate(16);
            uuidBytes.putLong(indexSegmentUuid.getMostSignificantBits());
            uuidBytes.putLong(indexSegmentUuid.getLeastSignificantBits());
            uuidBytes.flip();
            lanceParams.setIndexSegmentUuids(Collections.singletonList(uuidBytes));
        }
        if (scalarIndexDisabled) {
            // These fragments' segments are searched by other ranges; searching the whole index again for each
            // of these ranges would repeat that work. The complete filter still applies to their rows.
            lanceParams.setUseScalarIndex(false);
        }
        if (limit >= 0) {
            lanceParams.setLimit(limit);
        }
        formatDesc.setLanceParams(lanceParams);
        // Always set, -1 for a range BE reads, so BE never mistakes a stale value for a metadata count.
        formatDesc.setTableLevelRowCount(tableLevelRowCount);
    }
}
