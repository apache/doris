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

package org.apache.doris.connector.api.scan;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Plans the set of scan ranges (splits) needed to read a connector table.
 *
 * <p>This is a core SPI interface that every connector with scan capability
 * must implement. The engine calls {@link #planScan} to obtain scan ranges,
 * which are then converted to Thrift structures and dispatched to BE.</p>
 */
public interface ConnectorScanPlanProvider {

    /**
     * Returns the scan range type this provider produces.
     *
     * <p>The engine uses this to determine which Thrift scan range structure
     * to generate. For example, {@link ConnectorScanRangeType#FILE_SCAN}
     * produces TFileScanRange.</p>
     *
     * @return the scan range type (default: FILE_SCAN)
     */
    default ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    /**
     * Whether this connector is PREDICATE-DRIVEN and therefore opts out of the FE prune-to-zero
     * short-circuit.
     *
     * <p>A connector whose {@link #planScan} re-plans through its own SDK from the pushed predicate and
     * does NOT consume {@code requiredPartitions} (e.g. paimon) must return {@code true}. The engine then
     * maps a GENUINE prune-to-zero (FE pruning emptied the partition set over a non-empty universe) to
     * scan-all instead of short-circuiting to zero rows. This is required for master parity once a
     * genuine-null partition is rendered as a NON-null sentinel ({@code isNull=false}): {@code col IS NULL}
     * prunes every partition away, yet the genuine-null rows must still be returned via the pushed
     * predicate (the legacy {@code PaimonScanNode} never consults the FE partition selection).</p>
     *
     * <p>Default {@code false}: connectors that genuinely restrict the read to the pruned partitions
     * (e.g. MaxCompute, whose read session spans only {@code requiredPartitions}) keep the short-circuit.</p>
     *
     * @return {@code true} to disable the prune-to-zero short-circuit for this connector
     */
    default boolean ignorePartitionPruneShortCircuit() {
        return false;
    }

    /**
     * Whether this connector's SYSTEM tables honor a pinned read — {@code FOR TIME/VERSION AS OF}
     * (snapshot) and {@code @branch}/{@code @tag} scan-params. Consulted by the generic
     * {@code PluginDrivenScanNode} sys-table guard: a connector returning {@code true} (e.g. iceberg,
     * whose metadata tables legally time-travel) lets those pinned sys reads through to {@link #planScan};
     * a connector returning {@code false} (the default — e.g. paimon, whose binlog/audit_log sys tables
     * have no point-in-time semantics) keeps the fail-loud rejection. {@code @incr} (incremental read) is
     * rejected for EVERY connector regardless of this flag — it is undefined on a synthetic metadata table.
     *
     * @return {@code true} if this connector's system tables honor a time-travel / branch-tag pin
     *         (default: {@code false})
     */
    default boolean supportsSystemTableTimeTravel() {
        return false;
    }

    /**
     * Classifies a SPECIAL (synthesized / generated) column that this connector owns, by name. Consulted by
     * the generic {@code PluginDrivenScanNode.classifyColumn} so the engine can tag a connector's hidden /
     * metadata columns (e.g. iceberg's {@code __DORIS_ICEBERG_ROWID_COL__} row-id and v3 row-lineage columns)
     * with the right BE column category WITHOUT the generic node importing any connector-specific code.
     *
     * <p>Returning {@link ConnectorColumnCategory#DEFAULT} (the default — for every regular data column and
     * for connectors with no special columns, e.g. paimon/jdbc/es) means "not mine": the generic node falls
     * through to its own partition-key / regular classification. The engine-wide lazy-materialization row-id
     * column ({@code __DORIS_GLOBAL_ROWID_COL__*}) is NOT classified here — it is a generic Doris mechanism
     * handled by the generic node itself.</p>
     *
     * @param columnName the (already identifier-mapped) Doris column name of a query slot
     * @return the special-column category, or {@link ConnectorColumnCategory#DEFAULT} if not a special column
     */
    default ConnectorColumnCategory classifyColumn(String columnName) {
        return ConnectorColumnCategory.DEFAULT;
    }

    /**
     * Plans the scan for the given table, returning a list of scan ranges.
     *
     * @param session the current session
     * @param handle  the table handle to scan (may have been updated by applyFilter/applyProjection)
     * @param columns the columns to read
     * @param filter  an optional filter expression (remaining after pushdown)
     * @return a list of scan ranges that cover the requested data
     */
    List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter);

    /**
     * Plans the scan with an optional row limit.
     *
     * <p>Some connectors (e.g., JDBC) can push the limit into the remote query
     * to reduce data transfer. The default delegates to the 4-arg planScan,
     * ignoring the limit.</p>
     *
     * @param session the current session
     * @param handle  the table handle
     * @param columns the columns to read
     * @param filter  an optional remaining filter expression
     * @param limit   the maximum number of rows to return, or -1 for no limit
     * @return a list of scan ranges
     */
    default List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit) {
        return planScan(session, handle, columns, filter);
    }

    /**
     * Plans the scan restricted to a pruned set of partitions.
     *
     * <p>The engine computes partition pruning (Nereids {@code SelectedPartitions}) and
     * threads the surviving partitions here so partition-aware connectors can build a read
     * session over only those partitions instead of the whole table. The default ignores
     * {@code requiredPartitions} and delegates to the 5-arg variant, so connectors that do
     * not support partition pushdown are unaffected.</p>
     *
     * <p>Contract for {@code requiredPartitions}:</p>
     * <ul>
     *   <li>{@code null} or empty &rarr; not pruned; scan ALL partitions (default behavior).</li>
     *   <li>non-empty &rarr; scan ONLY these partitions. Each entry is a partition spec string
     *       (e.g. {@code "pt=1,region=cn"}), i.e. the keys of the pruned partition map.</li>
     * </ul>
     *
     * <p>The "pruned to zero partitions" case (a partition predicate that matches nothing) is
     * short-circuited by the engine before this method is called, so an empty list here always
     * means "not pruned / scan all", never "scan nothing".</p>
     *
     * @param session           the current session
     * @param handle            the table handle
     * @param columns           the columns to read
     * @param filter            an optional remaining filter expression
     * @param limit             the maximum number of rows to return, or -1 for no limit
     * @param requiredPartitions the pruned partition spec strings, or null/empty for all
     * @return a list of scan ranges
     */
    default List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit,
            List<String> requiredPartitions) {
        return planScan(session, handle, columns, filter, limit);
    }

    /**
     * Plans the scan, signalling whether a no-grouping {@code COUNT(*)} is being pushed down here.
     *
     * <p>When {@code countPushdown} is true, the engine has determined the query is a no-grouping
     * {@code COUNT(*)} (Nereids {@code getPushDownAggNoGroupingOp()==COUNT}) and BE is already in
     * count mode. A connector that can produce a precomputed row count for (some of) its splits
     * should emit it so BE serves the count from metadata instead of materializing rows
     * (e.g. Paimon's {@code DataSplit.mergedRowCount()}). The default ignores the flag and delegates
     * to the 6-arg variant, so connectors without a metadata row count are unaffected and keep the
     * normal scan.</p>
     *
     * @param session            the current session
     * @param handle             the table handle
     * @param columns            the columns to read
     * @param filter             an optional remaining filter expression
     * @param limit              the maximum number of rows to return, or -1 for no limit
     * @param requiredPartitions the pruned partition spec strings, or null/empty for all
     * @param countPushdown      whether a no-grouping {@code COUNT(*)} is being pushed down to this scan
     * @return a list of scan ranges
     */
    default List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit,
            List<String> requiredPartitions,
            boolean countPushdown) {
        return planScan(session, handle, columns, filter, limit, requiredPartitions);
    }

    /**
     * Whether this connector supports batched / streaming split generation for a partitioned scan.
     *
     * <p>When {@code true}, a partition-aware ScanNode (e.g. {@code PluginDrivenScanNode}) may
     * enter batch mode: instead of enumerating all splits synchronously via {@link #planScan},
     * it slices the pruned partitions into batches and calls {@link #planScanForPartitionBatch}
     * per batch on a background executor, streaming splits as they are produced (mirrors legacy
     * {@code MaxComputeScanNode.startSplit}). The default is {@code false}, so connectors stay on
     * the synchronous {@code planScan} path unless they opt in.</p>
     *
     * @param session the current session
     * @param handle  the table handle
     * @return whether batched split generation is supported for this table (default: false)
     */
    default boolean supportsBatchScan(ConnectorSession session, ConnectorTableHandle handle) {
        return false;
    }

    /**
     * Whether this connector's scan ranges carry meaningful byte lengths
     * ({@link ConnectorScanRange#getLength()}) so the engine can apply {@code TABLESAMPLE} by
     * size-weighted split selection ({@code PluginDrivenScanNode.sampleSplits}). Returning
     * {@code false} (the default) makes {@code TABLESAMPLE} a no-op — the full table is scanned (with a
     * warning) — matching these connectors' behavior before the SPI migration (only the legacy Hive scan
     * node ever sampled). A connector must NOT return {@code true} unless EVERY range it plans exposes a
     * positive, byte-proportional length: MaxCompute's default byte-size ranges and Paimon's JNI-read
     * ranges report {@code -1}, and MaxCompute row_offset ranges report a ROW count (not bytes), so they
     * must stay {@code false}. Mirrors {@link #supportsBatchScan}'s opt-in shape and Trino's
     * {@code ConnectorMetadata.applySample}.
     *
     * @return whether split-size TABLESAMPLE is valid for this connector (default: false)
     */
    default boolean supportsTableSample() {
        return false;
    }

    /**
     * Plans the scan for a single batch of partitions (used by batch-mode scans).
     *
     * <p>Called once per partition batch when the engine drives batch-mode split generation
     * (see {@link #supportsBatchScan}). Each call should build a read session over exactly the
     * given {@code partitionBatch} and return that batch's scan ranges. The default delegates to
     * the 6-arg {@link #planScan} with {@code partitionBatch} as the required partitions, which is
     * correct for connectors whose {@code planScan} builds one read session per partition set
     * (e.g. MaxCompute). A connector whose {@code planScan} is not partition-set-scoped must
     * override this method (and {@link #supportsBatchScan}) before enabling batch mode.</p>
     *
     * @param session        the current session
     * @param handle         the table handle
     * @param columns        the columns to read
     * @param filter         an optional remaining filter expression
     * @param limit          the maximum number of rows to return, or -1 for no limit
     * @param partitionBatch the partition spec strings for this batch (non-empty)
     * @return the scan ranges for this partition batch
     */
    default List<ConnectorScanRange> planScanForPartitionBatch(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit,
            List<String> partitionBatch) {
        return planScan(session, handle, columns, filter, limit, partitionBatch);
    }

    /**
     * Decides whether this scan should use streaming (lazy) split generation and, if so, estimates
     * the number of splits it will produce. This is the file-count counterpart of the partition-count
     * batch gate ({@link #supportsBatchScan}), and echoes Trino's lazy {@code ConnectorSplitSource}
     * model: the connector owns the whole decision (e.g. Iceberg: matched-manifest file count &ge;
     * {@code num_files_in_batch_mode} with {@code enable_external_table_batch_mode} on, a snapshot
     * present, not a system table, count pushdown not servable). The engine additionally requires the
     * scan to have output slots before entering streaming mode.
     *
     * <p>When this returns a non-negative value, the engine drives streaming split generation via
     * {@link #streamSplits} (pulling ranges with backpressure) instead of the synchronous
     * {@link #planScan}; the returned estimate doubles as the BE concurrency hint. A negative value
     * keeps the scan on the synchronous {@code planScan} path (the default).</p>
     *
     * <p>The decision MUST be cheap (e.g. manifest metadata counts), NOT a full split enumeration —
     * the heavy enumeration is deferred to {@link #streamSplits}.</p>
     *
     * @param session       the current session
     * @param handle        the table handle (carries any pushed-down filter)
     * @param filter        an optional remaining filter expression
     * @param countPushdown whether a no-grouping {@code COUNT(*)} is pushed down for this scan
     * @return the approximate streamed split count if streaming should be used, else a negative value
     *         (default: -1)
     */
    default long streamingSplitEstimate(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter,
            boolean countPushdown) {
        return -1;
    }

    /**
     * Builds a lazy {@link ConnectorSplitSource} for streaming split generation. Called once, on a
     * background task, only when {@link #streamingSplitEstimate} returned a non-negative value. The
     * engine pulls ranges from the source with backpressure and pumps them into the split queue
     * (mirrors legacy {@code IcebergScanNode.doStartSplit}).
     *
     * <p>The source MUST defer the heavy planning (e.g. {@code TableScan.planFiles()}) until ranges
     * are consumed, so FE heap usage stays bounded for very large scans. The default throws, so a
     * connector that returns a non-negative {@link #streamingSplitEstimate} must override this.</p>
     *
     * @param session the current session
     * @param handle  the table handle (snapshot/time-travel already pinned by the engine)
     * @param columns the columns to read
     * @param filter  an optional remaining filter expression
     * @param limit   the maximum number of rows to return, or -1 for no limit
     * @return a lazy, closeable source of scan ranges
     */
    default ConnectorSplitSource streamSplits(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit) {
        throw new UnsupportedOperationException(
                "streamSplits requires streamingSplitEstimate(...) >= 0; connector did not implement it");
    }

    /**
     * Returns scan-node-level properties shared across all scan ranges.
     *
     * <p>Unlike per-range properties in {@link ConnectorScanRange#getProperties()},
     * these properties apply to the entire scan node. For example, ES connectors
     * return the query DSL, authentication info, and field context mappings here,
     * since they are shared across all shard scan ranges.</p>
     *
     * @param session the current session
     * @param handle  the table handle (may have been updated by applyFilter)
     * @param columns the columns to read
     * @param filter  an optional remaining filter expression
     * @return node-level properties (default: empty map)
     */
    default Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        return Collections.emptyMap();
    }

    /**
     * Estimates the number of scan ranges for parallelism planning.
     * Returns -1 if the estimate is unknown.
     *
     * <p>The engine may use this to pre-allocate resources or decide
     * scan parallelism before calling {@link #planScan}.</p>
     */
    default long estimateScanRangeCount(ConnectorSession session,
            ConnectorTableHandle handle) {
        return -1;
    }

    /**
     * Returns scan-node-level properties along with filter pushdown results.
     *
     * <p>Override this when the connector performs fine-grained conjunct pushdown
     * (e.g., ES query DSL building) and needs to report which conjuncts
     * were NOT pushed. The indices in {@link ScanNodePropertiesResult#getNotPushedConjunctIndices()}
     * refer to the AND children of the filter expression, in the same order as
     * the conjuncts list.</p>
     *
     * <p>The default wraps {@link #getScanNodeProperties} with an empty not-pushed set,
     * meaning all conjuncts are assumed to have been pushed.</p>
     *
     * @param session the current session
     * @param handle  the table handle (may have been updated by applyFilter)
     * @param columns the columns to read
     * @param filter  an optional remaining filter expression
     * @return properties with filter pushdown metadata
     */
    default ScanNodePropertiesResult getScanNodePropertiesResult(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        return new ScanNodePropertiesResult(
                getScanNodeProperties(session, handle, columns, filter));
    }

    /**
     * Populates scan-level Thrift params that apply to all scan ranges.
     * Called once after all ranges are distributed.
     *
     * <p>Connectors that need to set fields on TFileScanRangeParams
     * (e.g., Paimon predicate, ES docvalue context) override this method.</p>
     *
     * @param params         the TFileScanRangeParams to populate
     * @param nodeProperties the scan node properties from getScanNodeProperties()
     */
    default void populateScanLevelParams(TFileScanRangeParams params,
            Map<String, String> nodeProperties) {
        // Default: no scan-level params needed
    }

    /**
     * Appends connector-specific EXPLAIN output.
     * Called after the generic TABLE/QUERY/PREDICATES lines.
     *
     * <p>Each connector decides its own EXPLAIN format. For example, ES
     * appends "ES index/type" and "REMOTE_PREDICATES" lines.</p>
     *
     * @param output         the StringBuilder to append to
     * @param prefix         the indentation prefix for this explain level
     * @param nodeProperties the scan node properties
     */
    default void appendExplainInfo(StringBuilder output,
            String prefix, Map<String, String> nodeProperties) {
        // Default: no extra EXPLAIN info
    }

    /**
     * Returns the delete-file paths carried by one scan range's table-format descriptor, for the
     * VERBOSE per-backend EXPLAIN block ({@code deleteFileNum}/{@code deleteSplitNum}).
     *
     * <p>The default returns an empty list, so connectors without merge-on-read deletes contribute
     * nothing. A connector that threads delete files onto its per-range thrift (e.g. Paimon's
     * deletion vectors) overrides this to read them back from {@code tableFormatParams}.</p>
     *
     * @param tableFormatParams the per-range table-format descriptor (may be {@code null})
     * @return the delete-file paths for this range (default: empty)
     */
    default List<String> getDeleteFiles(TTableFormatFileDesc tableFormatParams) {
        return Collections.emptyList();
    }

    /**
     * Returns the serialized table representation for this connector,
     * or {@code null} if not applicable.
     *
     * <p>Currently used by Paimon to pass the serialized Paimon Table
     * object to BE for JNI-based reading.</p>
     *
     * @param nodeProperties the scan node properties
     * @return serialized table string, or null
     */
    default String getSerializedTable(Map<String, String> nodeProperties) {
        return null;
    }

    /**
     * Releases any per-query read transaction this provider opened, called by the engine when the query
     * finishes (via the generic query-finish callback registry). The default is a no-op: connectors that do
     * not open a per-query read transaction (every connector except transactional/ACID hive) need not override
     * it.
     *
     * <p>A connector that opens a metastore read transaction + shared read lock during {@link #planScan} (hive
     * full-ACID / insert-only reads) MUST override this to commit that transaction and release the lock, else
     * the shared read lock leaks for the metastore's lifetime. Best-effort: an implementation should log and
     * swallow a commit failure rather than propagate (the callback registry isolates exceptions anyway).
     * {@code queryId} is the engine query id string ({@link ConnectorSession#getQueryId()}), the same key the
     * provider registered the transaction under.</p>
     *
     * @param queryId the finishing query's id (== {@link ConnectorSession#getQueryId()})
     */
    default void releaseReadTransaction(String queryId) {
        // default: no per-query read transaction to release
    }
}
