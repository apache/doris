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

package org.apache.doris.connector.spi.handle;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.thrift.TSortInfo;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A bound write request passed to
 * {@link org.apache.doris.connector.spi.write.ConnectorWritePlanProvider#planWrite}.
 *
 * <p>Carries the engine-resolved facts about a single DML write: the target
 * table handle, the column list, whether it is an OVERWRITE, and the static
 * partition spec ({@link #getStaticPartitionSpec}). The connector reads these to build
 * its Thrift data sink.</p>
 */
public interface ConnectorWriteHandle {

    /** The target table handle (the connector's own opaque table handle). */
    ConnectorTableHandle getTableHandle();

    /** The columns being written, ordered to match the INSERT column list. */
    List<ConnectorColumn> getColumns();

    /**
     * The complete target schema captured when this write was bound, in target-schema order.
     *
     * <p>This is deliberately separate from {@link #getColumns()}: an INSERT column list and static
     * partitions can make the write list a subset even though schema-drift validation must compare the
     * complete bound schema. The default preserves compatibility for handles that already carry a full
     * write list.</p>
     */
    default List<ConnectorColumn> getBoundTargetColumns() {
        return getColumns();
    }

    /** Whether this is an INSERT OVERWRITE. */
    boolean isOverwrite();

    /**
     * The static partition spec (partition column name -&gt; value) for a statically partitioned write,
     * carried from the bound sink to {@code planWrite}; an EMPTY map when the write is not statically
     * partitioned. Both sides now spell it the same way: the sole producer is
     * {@code PluginDrivenTableSink.bindDataSink} -&gt; {@code PluginDrivenInsertCommandContext
     * .getStaticPartitionSpec}, and the write providers (hive/iceberg/maxcompute) consume it as such
     * (iceberg ships it verbatim as {@code TDataSink.static_partition_values}). It was once called
     * {@code getWriteContext} and envisioned as a free-form bag; nothing ever put anything else in it, so the
     * name was corrected rather than the contract widened. A future free-form channel would be a new method.
     */
    Map<String, String> getStaticPartitionSpec();

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
     * {@link org.apache.doris.connector.spi.write.ConnectorWritePlanProvider#getWriteSortColumns}
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

    /** Metadata identity captured when the engine bound the physical write plan, or {@code null}. */
    default String getBoundWriteMetadataIdentity() {
        return null;
    }

    /**
     * Whether the statement behind this write is a SQL {@code MERGE INTO} whose cardinality rule the sink
     * must enforce: a target row matched by more than one source row is an error, and the connector's BE
     * sink is the only place that can see the duplicates. {@code false} for {@code UPDATE} — which shares
     * the same {@link WriteOperation#MERGE} sink dialect but has no cardinality rule — and for every
     * non-row-level write.
     *
     * <p>Kept separate from {@link #getWriteOperation()} because it is a statement-level SQL requirement,
     * not a sink dialect: the engine also uses it to keep the merge distribution when the optional
     * {@code enable_strict_consistency_dml} session variable is off. Defaults to {@code false} so every
     * connector that never sees a MERGE keeps its byte-identical sink.</p>
     */
    default boolean isRequireMergeCardinalityCheck() {
        return false;
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
