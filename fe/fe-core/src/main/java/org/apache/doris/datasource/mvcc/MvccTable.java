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

package org.apache.doris.datasource.mvcc;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.TableIf;

import java.util.Optional;

/**
 * The table that needs to query data based on the version needs to implement this interface.
 */
public interface MvccTable extends TableIf {
    /**
     * Retrieve the current snapshot information of the table,
     * and the returned result will be used for the entire process of this query
     *
     * @return MvccSnapshot
     */
    MvccSnapshot loadSnapshot(Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams);

    /**
     * Load only the latest version identity used to fence relation-specific projections.
     * Implementations may avoid materializing partitions; the default preserves compatibility.
     */
    default MvccSnapshot loadLatestSnapshotFence() {
        return loadSnapshot(Optional.empty(), Optional.empty());
    }

    /**
     * Whether this relation projection needs the table's statement-scoped latest snapshot as its
     * version fence. Implementations keep projection identity separate from version identity.
     */
    default boolean requiresLatestSnapshotFence(
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        return false;
    }

    /** Load a relation projection against an already materialized statement latest snapshot. */
    default MvccSnapshot loadSnapshot(
            Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams,
            Optional<MvccSnapshot> latestSnapshotFence) {
        return loadSnapshot(tableSnapshot, scanParams);
    }
}
