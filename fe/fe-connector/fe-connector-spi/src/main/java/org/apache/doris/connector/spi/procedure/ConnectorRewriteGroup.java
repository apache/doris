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

package org.apache.doris.connector.spi.procedure;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * One group of data files a connector's rewrite planning produced, in an engine-neutral form. The engine
 * rewrite driver runs one {@code INSERT-SELECT} per group, scoping the scan to {@link #getDataFilePaths()}
 * (the raw file paths, fed to the connector scan's per-group file scope), and sums the per-group stats into
 * the {@link ConnectorRewriteStatistics} it hands back to the connector.
 *
 * <p>The model is atomic replacement by file path: each group's files are read once and replaced by what the
 * rewrite writes. This is the planning analogue of {@link ConnectorProcedureResult}: the connector owns which
 * files to touch and how to group them (size ranges, delete ratios, minimum inputs, per-partition grouping —
 * all of it connector-defined); the engine only orchestrates the distributed reads/writes. No live SDK object
 * crosses the seam — only neutral {@code String} paths and primitive counts.</p>
 */
public class ConnectorRewriteGroup {

    private final Set<String> dataFilePaths;
    private final int dataFileCount;
    private final long totalSizeBytes;
    private final int deleteFileCount;

    /**
     * @param dataFilePaths  the RAW file paths of this group's data files (what the connector scan matches a
     *                       per-group file scope against); never {@code null}
     * @param dataFileCount  the number of data files rewritten by this group; kept distinct from
     *                       {@code dataFilePaths.size()} so it carries the connector's own count verbatim
     * @param totalSizeBytes the total byte size of this group's data files
     * @param deleteFileCount the number of delete files attached to this group
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

    /** The number of data files this group rewrites. */
    public int getDataFileCount() {
        return dataFileCount;
    }

    /** The total byte size of this group's data files. */
    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    /** The number of delete files attached to this group. */
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
