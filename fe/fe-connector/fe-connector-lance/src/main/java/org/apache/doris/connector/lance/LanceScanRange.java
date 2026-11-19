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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * One BE scan task: fragments of one dataset version.
 *
 * <p>BE opens the dataset itself through lance-c, from {@code TLanceFileDesc.dataset_uri} and
 * {@code version}, and reads only the listed fragments; an empty fragment list would make it read the whole
 * dataset, so a range always carries at least one. The range has no byte extent of its own: its path is the
 * dataset URI, which decides the file type the engine reports to BE, and offsets and sizes stay 0.
 */
final class LanceScanRange implements ConnectorScanRange {
    private static final long serialVersionUID = 1L;
    /** The reader key BE dispatches on ({@code FileScannerV2} selects its Lance reader by it). */
    static final String TABLE_FORMAT = "lance";

    private final String datasetUri;
    private final long version;
    private final ArrayList<Long> fragmentIds;
    private final long physicalRows;
    private final long targetSplitSize;

    /**
     * @param physicalRows the rows the fragments store before deletions, which is what reading them costs
     * @param targetSplitSize the largest {@code physicalRows} of the plan; split weights are relative to it
     */
    LanceScanRange(String datasetUri, long version, List<Long> fragmentIds, long physicalRows,
            long targetSplitSize) {
        if (version <= 0) {
            throw new IllegalArgumentException("Lance snapshot version must be positive");
        }
        if (fragmentIds.isEmpty()) {
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

    @Override
    public Optional<String> getPath() {
        return Optional.of(datasetUri);
    }

    /** What consistent hashing assigns by; ranges over different fragments must not collapse onto one BE. */
    @Override
    public String getSplitIdentity() {
        return datasetUri + "#" + version + "#" + fragmentIds;
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
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        TLanceFileDesc lanceParams = new TLanceFileDesc();
        lanceParams.setDatasetUri(datasetUri);
        lanceParams.setVersion(version);
        lanceParams.setFragmentIds(new ArrayList<>(fragmentIds));
        formatDesc.setLanceParams(lanceParams);
        // Always set, -1 for an ordinary scan, so BE never mistakes a stale value for a metadata count.
        formatDesc.setTableLevelRowCount(-1);
    }
}
