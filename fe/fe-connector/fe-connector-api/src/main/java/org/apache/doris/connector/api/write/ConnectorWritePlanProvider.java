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

package org.apache.doris.connector.api.write;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;

import java.util.List;

/**
 * Plans the write (sink) for a connector table: produces the opaque
 * {@link org.apache.doris.thrift.TDataSink} that BE uses to write data.
 *
 * <p>This is the write-side analogue of
 * {@link org.apache.doris.connector.api.scan.ConnectorScanPlanProvider}. A
 * connector with write capability returns an implementation from
 * {@link org.apache.doris.connector.api.Connector#getWritePlanProvider()}; the
 * engine calls {@link #planWrite} when translating a physical table sink, then
 * dispatches the resulting Thrift data sink to BE unchanged.</p>
 */
public interface ConnectorWritePlanProvider {

    /**
     * Builds the data sink for the given bound write request.
     *
     * @param session the current session
     * @param handle  the bound write request (target table, columns, overwrite, context)
     * @return a {@link ConnectorSinkPlan} wrapping the Thrift data sink
     */
    ConnectorSinkPlan planWrite(ConnectorSession session, ConnectorWriteHandle handle);

    /**
     * Appends connector-specific EXPLAIN detail for the write (e.g. the generated INSERT SQL,
     * sink dialect, target format). Write-side analogue of the scan provider's
     * {@code appendExplainInfo}: the engine emits the generic plugin-driven sink line, then calls
     * this so the connector can surface its own write details without the engine knowing the
     * sink dialect.
     *
     * <p>This runs when the plan's EXPLAIN string is generated, which is <i>before</i> the write
     * plan is bound (the sink's {@code planWrite} has not run yet for an EXPLAIN). The connector
     * therefore derives the detail from the {@code handle} and may consult the source for metadata
     * (e.g. remote column names). Default: no extra EXPLAIN info.</p>
     *
     * @param output  the EXPLAIN string being built
     * @param prefix  the current indentation prefix
     * @param session the current session (may be consulted for session-scoped write options)
     * @param handle  the write request (target table handle and write columns)
     */
    default void appendExplainInfo(StringBuilder output, String prefix,
            ConnectorSession session, ConnectorWriteHandle handle) {
        // Default: no extra EXPLAIN info
    }

    /**
     * Declares whether the target has a write-side sort order and, if so, its sort columns, in an
     * engine-neutral form. The engine calls this when translating the table sink: for a non-{@code null}
     * result it builds the Thrift {@code TSortInfo} from the columns (resolving each
     * {@link ConnectorWriteSortColumn#getColumnIndex()} against the bound sink output) and threads it
     * back to {@link #planWrite} via {@link ConnectorWriteHandle#getSortInfo()} so the connector can stamp
     * it onto its opaque sink.
     *
     * <p>The {@code null}-vs-list distinction mirrors a source's {@code isSorted()} gate: {@code null}
     * means the target has <b>no</b> write sort order (no {@code TSortInfo}); a non-{@code null} list
     * means it <b>has</b> one — even an empty list, which yields an empty {@code TSortInfo} (a target
     * sorted only by non-resolvable transforms still requests sorted-write semantics). Depends only on
     * the target table (not the bound write), so it takes the {@link ConnectorTableHandle}: at
     * translation time the full write handle is not yet formed. Default: {@code null} — jdbc / maxcompute
     * keep their byte-identical unsorted sink output.</p>
     *
     * @param session     the current session
     * @param tableHandle the target table handle
     * @return the ordered write-sort columns (possibly empty) if the target has a sort order, or
     *         {@code null} if it has none
     */
    default List<ConnectorWriteSortColumn> getWriteSortColumns(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return null;
    }

    /**
     * Declares the target's write-time partitioning, in an engine-neutral form, so the engine can reproduce
     * the connector's write distribution (the iceberg merge-write {@code DistributionSpecMerge}) without
     * importing the connector's native partition-spec types. The engine resolves each
     * {@link ConnectorWritePartitionField#getSourceColumnName()} to a bound output expr id locally and builds
     * the distribution from the field tuple + {@link ConnectorWritePartitionSpec#getSpecId()}.
     *
     * <p>{@code null} (not an empty spec) means the target is unpartitioned, mirroring the legacy
     * {@code spec().isPartitioned()} gate — the engine then falls back to its non-partitioned merge
     * distribution. Depends only on the target table (not the bound write), so it takes the
     * {@link ConnectorTableHandle}. Default: {@code null} — jdbc / maxcompute / paimon keep their
     * byte-identical non-partitioned write distribution.</p>
     *
     * @param session     the current session
     * @param tableHandle the target table handle
     * @return the current spec id + ordered partition fields if the target is partitioned, or {@code null}
     *         if it is unpartitioned
     */
    default ConnectorWritePartitionSpec getWritePartitioning(ConnectorSession session,
            ConnectorTableHandle tableHandle) {
        return null;
    }
}
