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

package org.apache.doris.connector.api.handle;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.thrift.TSortInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A bound write request passed to
 * {@link org.apache.doris.connector.api.write.ConnectorWritePlanProvider#planWrite}.
 *
 * <p>Carries the engine-resolved facts about a single DML write: the target
 * table handle, the column list, whether it is an OVERWRITE, and a free-form
 * write context (static partition spec, write path, etc.). The connector reads
 * these to build its Thrift data sink.</p>
 */
public interface ConnectorWriteHandle {

    /** The target table handle (the connector's own opaque table handle). */
    ConnectorTableHandle getTableHandle();

    /** The columns being written, ordered to match the INSERT column list. */
    List<ConnectorColumn> getColumns();

    /** Whether this is an INSERT OVERWRITE. */
    boolean isOverwrite();

    /**
     * Free-form write context: static partition spec, write path, and other
     * connector-defined keys carried from the bound sink to {@code planWrite}.
     */
    Map<String, String> getWriteContext();

    /**
     * The kind of DML write (INSERT / OVERWRITE / DELETE / UPDATE / MERGE). A single
     * {@code planWrite} dispatches on this to pick the connector's Thrift sink dialect, and a
     * file-transactional connector (iceberg) dispatches on it to pick the SDK operation.
     *
     * <p>Defaults to {@link WriteOperation#INSERT} so connectors that only do plain appends
     * (jdbc / maxcompute) — which never set it — keep append semantics and stay byte-compatible.</p>
     */
    default WriteOperation getWriteOperation() {
        return WriteOperation.INSERT;
    }

    /**
     * The engine-built BE sort instruction for this write, or {@code null} if the target needs no
     * write-side sort. A connector declares its write-sort columns via
     * {@link org.apache.doris.connector.api.write.ConnectorWritePlanProvider#getWriteSortColumns}
     * (e.g. an iceberg table with a {@code WRITE ORDERED BY} sort order); the engine resolves those
     * column indices against the bound sink output and builds the {@link TSortInfo}, which the
     * connector then stamps onto its opaque Thrift sink in {@code planWrite}.
     *
     * <p>The split is necessary because the bound output expressions live only in the engine
     * (translation time), not in this source-agnostic handle. Defaults to {@code null} so connectors
     * that declare no write sort (jdbc / maxcompute) keep their byte-identical unsorted sink output.</p>
     */
    default TSortInfo getSortInfo() {
        return null;
    }

    /**
     * The named table branch this write targets ({@code INSERT INTO t@branch(name)}), or
     * {@link Optional#empty()} when the write goes to the table's default ref. Threaded from the
     * generic insert command context onto this handle; a versioned-table connector (iceberg / paimon)
     * reads it in {@code planWrite} to point the commit at the branch. Defaults to empty so connectors
     * with no branch concept (jdbc / maxcompute) keep their byte-identical default-ref write.
     */
    default Optional<String> getBranchName() {
        return Optional.empty();
    }
}
