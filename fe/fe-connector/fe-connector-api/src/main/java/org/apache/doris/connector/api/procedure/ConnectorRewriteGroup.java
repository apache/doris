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

package org.apache.doris.connector.api.procedure;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * One bin-packed group of data files a connector's {@code rewrite_data_files} planning produced, in an
 * engine-neutral form. The engine rewrite driver runs one {@code INSERT-SELECT} per group, scoping the scan
 * to {@link #getDataFilePaths()} (the raw file paths, fed to the connector scan's per-group file scope), and
 * sums the per-group stats into the procedure's result row.
 *
 * <p>This is the planning analogue of {@link ConnectorProcedureResult}: the connector owns the
 * file-selection / bin-pack / partition-grouping decision (iceberg's {@code rewrite_data_files} criteria —
 * file size range, delete ratio, min-input-files, max group size, partition grouping); the engine only
 * orchestrates the distributed reads/writes. No live SDK object crosses the seam — only neutral
 * {@code String} paths and primitive counts.</p>
 */
public class ConnectorRewriteGroup {

    private final Set<String> dataFilePaths;
    private final int dataFileCount;
    private final long totalSizeBytes;
    private final int deleteFileCount;

    /**
     * @param dataFilePaths  the RAW file paths of this group's data files (what the connector scan matches a
     *                       per-group file scope against — see the iceberg scan provider); never {@code null}
     * @param dataFileCount  the number of data files rewritten by this group (feeds {@code
     *                       rewritten_data_files_count}); kept distinct from {@code dataFilePaths.size()} so it
     *                       carries the connector's own count verbatim
     * @param totalSizeBytes the total byte size of this group's data files (feeds {@code rewritten_bytes_count})
     * @param deleteFileCount the number of delete files attached to this group (feeds {@code
     *                        removed_delete_files_count})
     */
    public ConnectorRewriteGroup(Set<String> dataFilePaths, int dataFileCount, long totalSizeBytes,
            int deleteFileCount) {
        this.dataFilePaths = Collections.unmodifiableSet(
                Objects.requireNonNull(dataFilePaths, "dataFilePaths is null"));
        this.dataFileCount = dataFileCount;
        this.totalSizeBytes = totalSizeBytes;
        this.deleteFileCount = deleteFileCount;
    }

    /** The raw data-file paths in this group, used to scope the per-group rewrite scan. */
    public Set<String> getDataFilePaths() {
        return dataFilePaths;
    }

    /** The number of data files this group rewrites ({@code rewritten_data_files_count} contribution). */
    public int getDataFileCount() {
        return dataFileCount;
    }

    /** The total byte size of this group's data files ({@code rewritten_bytes_count} contribution). */
    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    /** The number of delete files attached to this group ({@code removed_delete_files_count} contribution). */
    public int getDeleteFileCount() {
        return deleteFileCount;
    }

    @Override
    public String toString() {
        return "ConnectorRewriteGroup{dataFileCount=" + dataFileCount
                + ", totalSizeBytes=" + totalSizeBytes
                + ", deleteFileCount=" + deleteFileCount
                + ", dataFilePaths=" + dataFilePaths.size() + " files}";
    }
}
