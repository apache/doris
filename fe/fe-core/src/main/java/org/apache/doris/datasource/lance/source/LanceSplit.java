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
import java.util.OptionalLong;
import java.util.UUID;

/**
 * A Lance scan split. Catalog and S3 scans normally use one fixed-version fragment per split.
 * Indexed vector search uses one physical index segment and its covered fragments per split.
 * Backend-local TVFs use one whole-dataset latest-version split.
 */
public class LanceSplit extends FileSplit {
    private final String datasetUri;
    private final long version;
    private final List<Long> fragmentIds;
    private final List<UUID> indexSegmentUuids;
    private final long rowCount;

    public static LanceSplit forFragment(
            String datasetUri, long version, long fragmentId, long physicalRows) {
        return forFragment(datasetUri, version, fragmentId, -1, physicalRows);
    }

    public static LanceSplit forFragment(
            String datasetUri, long version, long fragmentId, long rowCount, long physicalRows) {
        return new LanceSplit(datasetUri, version, Collections.singletonList(fragmentId),
                Collections.emptyList(), rowCount, physicalRows);
    }

    public static LanceSplit wholeDatasetAtLatest(String datasetUri) {
        return new LanceSplit(datasetUri, 0, Collections.emptyList(), Collections.emptyList(), -1, 1);
    }

    public static LanceSplit forIndexSegment(String datasetUri, long version, UUID indexSegmentUuid,
            List<Long> fragmentIds, long physicalRows) {
        if (fragmentIds == null || fragmentIds.isEmpty()) {
            throw new IllegalArgumentException("Lance index segment split must contain fragments");
        }
        return new LanceSplit(datasetUri, version, fragmentIds,
                Collections.singletonList(indexSegmentUuid), -1, physicalRows);
    }

    private LanceSplit(String datasetUri, long version, List<Long> fragmentIds,
            List<UUID> indexSegmentUuids, long rowCount, long physicalRows) {
        super(LocationPath.of(requireDatasetUri(datasetUri)), 0, 0, 0, 0, null,
                Collections.emptyList());
        if (version < 0) {
            throw new IllegalArgumentException("Lance dataset version must be non-negative");
        }
        for (Long fragmentId : fragmentIds) {
            if (fragmentId == null || fragmentId < 0) {
                throw new IllegalArgumentException("Lance fragment id must be non-negative");
            }
        }
        for (UUID indexSegmentUuid : indexSegmentUuids) {
            if (indexSegmentUuid == null) {
                throw new IllegalArgumentException("Lance index segment UUID must not be null");
            }
        }
        if (rowCount < -1) {
            throw new IllegalArgumentException("Lance fragment row count must be non-negative or unknown");
        }
        this.datasetUri = datasetUri;
        this.version = version;
        this.fragmentIds = Collections.unmodifiableList(new ArrayList<>(fragmentIds));
        this.indexSegmentUuids = Collections.unmodifiableList(new ArrayList<>(indexSegmentUuids));
        this.rowCount = rowCount;
        this.tableFormatType = TableFormatType.LANCE;
        this.selfSplitWeight = Math.max(physicalRows, 1);
    }

    private static String requireDatasetUri(String datasetUri) {
        if (datasetUri == null || datasetUri.trim().isEmpty()) {
            throw new IllegalArgumentException("Lance dataset URI must not be empty");
        }
        return datasetUri;
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

    public List<UUID> getIndexSegmentUuids() {
        return indexSegmentUuids;
    }

    public boolean hasIndexSegmentUuids() {
        return !indexSegmentUuids.isEmpty();
    }

    /** Returns the fixed-snapshot logical row count after deletion vectors, when FE knows it. */
    public OptionalLong getRowCount() {
        return rowCount >= 0 ? OptionalLong.of(rowCount) : OptionalLong.empty();
    }

    @Override
    public String getConsistentHashString() {
        return hasFragmentIds()
                ? datasetUri + "#" + version + "#" + fragmentIds + "#" + indexSegmentUuids
                : datasetUri + "#" + (version == 0 ? "latest" : version) + "#all";
    }
}
