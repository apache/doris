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

import org.apache.doris.connector.api.handle.WriteOperation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Immutable op context for a single iceberg write, threaded into
 * {@link IcebergConnectorTransaction#beginWrite}. The connector-internal equivalent of the fe-core
 * {@code IcebergInsertCommandContext} (which the connector cannot import): it carries the write operation,
 * the overwrite mode, the static partition spec (for {@code INSERT OVERWRITE ... PARTITION}), and the target
 * branch. The transaction reads these at begin time (to pick begin-guards) and at commit time (to pick the
 * SDK operation: AppendFiles / ReplacePartitions / OverwriteFiles / RowDelta).
 *
 * <p>P6.3-T06 populates this from the {@code ConnectorWriteHandle}
 * ({@code getWriteOperation}/{@code isOverwrite}/{@code getWriteContext}) in {@code planWrite}.</p>
 */
final class IcebergWriteContext {

    private final WriteOperation writeOperation;
    private final boolean overwrite;
    private final Map<String, String> staticPartitionValues;
    private final Optional<String> branchName;
    private final long readSnapshotId;

    IcebergWriteContext(WriteOperation writeOperation, boolean overwrite,
            Map<String, String> staticPartitionValues, Optional<String> branchName) {
        this(writeOperation, overwrite, staticPartitionValues, branchName, -1L);
    }

    IcebergWriteContext(WriteOperation writeOperation, boolean overwrite,
            Map<String, String> staticPartitionValues, Optional<String> branchName, long readSnapshotId) {
        this.writeOperation = writeOperation;
        this.overwrite = overwrite;
        this.staticPartitionValues = staticPartitionValues == null
                ? Collections.emptyMap() : new HashMap<>(staticPartitionValues);
        this.branchName = branchName == null ? Optional.empty() : branchName;
        this.readSnapshotId = readSnapshotId;
    }

    WriteOperation getWriteOperation() {
        return writeOperation;
    }

    boolean isOverwrite() {
        return overwrite;
    }

    Map<String, String> getStaticPartitionValues() {
        return staticPartitionValues;
    }

    /** An {@code INSERT OVERWRITE ... PARTITION(col=val, ...)} (a non-empty static partition spec). */
    boolean isStaticPartitionOverwrite() {
        return overwrite && !staticPartitionValues.isEmpty();
    }

    Optional<String> getBranchName() {
        return branchName;
    }

    /**
     * The statement's READ snapshot id (the MVCC pin the scan used, S_read), threaded from the write
     * handle in {@code planWrite}; {@code -1} = no pin (the legacy fresh-current behavior). The
     * RowDelta path anchors {@code baseSnapshotId} at this snapshot so the commit-time removeDeletes
     * (option D) and the scan-time deletes BE unions into the new DV share one snapshot — see
     * {@link IcebergConnectorTransaction} [SHOULD-2] / Fix B.
     */
    long getReadSnapshotId() {
        return readSnapshotId;
    }
}
