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

package org.apache.doris.connector.spi.scan;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Plans the set of scan ranges (splits) needed to read a connector table.
 *
 * <p>This is a core SPI interface that every connector with scan capability
 * must implement. The engine calls {@link #planScan} to obtain scan ranges,
 * which are then converted to Thrift structures and dispatched to BE.</p>
 */
public interface ConnectorScanPlanProvider {

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
     * have no point-in-time semantics) keeps the fail-loud rejection.
     *
     * <p>This flag governs the SNAPSHOT-shaped selectors only. {@code @incr} and {@code @options} are
     * answered PER SYSTEM TABLE by {@link #supportsSystemTableIncrementalRead} /
     * {@link #supportsSystemTableOptions}, because whether a metadata view can honor them depends on
     * that view's own reader, not on the connector as a whole.</p>
     *
     * @return {@code true} if this connector's system tables honor a time-travel / branch-tag pin
     *         (default: {@code false})
     */
    default boolean supportsSystemTableTimeTravel() {
        return false;
    }

    /**
     * Whether the named SYSTEM table honors {@code @incr(...)} (an incremental range read). Answered per
     * system table because the capability belongs to the view's reader: a view that enumerates the LATEST
     * partitions before applying a per-partition range read cannot serve a range whose partitions have since
     * been dropped, while a commit-log view (paimon {@code $audit_log} / {@code $binlog}) can.
     *
     * <p>Default {@code false} — a synthetic metadata view has no incremental semantics unless its connector
     * says otherwise. Consulted by the {@code PluginDrivenScanNode} sys-table guard, which otherwise rejects
     * {@code @incr} fail-loud.</p>
     *
     * @param sysTableName the bare system-table name (no {@code "$"} prefix), lower-cased by the caller
     * @return {@code true} if that system table honors {@code @incr} (default: {@code false})
     */
    default boolean supportsSystemTableIncrementalRead(String sysTableName) {
        return false;
    }

    /**
     * Whether the named SYSTEM table honors {@code @options(...)} (relation-scoped, source-native scan
     * options). Answered per system table for the same reason as {@link #supportsSystemTableIncrementalRead}:
     * a view may advertise this only when EVERY row-producing stage of its reader observes the selected
     * snapshot. A view that internally consults latest metadata (paimon {@code $files} / {@code $buckets})
     * must decline, or it would silently answer a historical question with current metadata.
     *
     * @param sysTableName the bare system-table name (no {@code "$"} prefix), lower-cased by the caller
     * @return {@code true} if that system table honors {@code @options} (default: {@code false})
     */
    default boolean supportsSystemTableOptions(String sysTableName) {
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
     * Lets a connector adjust the file compression type the generic node inferred from the file path/extension
     * (via {@code Util.inferFileCompressTypeByPath}) before it is shipped to BE on the scan range. The default
     * is identity — the inferred type is used as-is.
     *
     * <p>Hive overrides this to remap {@code LZ4FRAME -> LZ4BLOCK}: hadoop/hive write {@code .lz4} files with
     * the LZ4 <em>block</em> codec, not the LZ4 frame format the {@code .lz4} extension implies, so the frame
     * inferred from the path would make BE's frame decoder fail ({@code LZ4F_getFrameInfo ERROR_frameType_unknown}).
     * This keeps that hadoop-specific fact inside the connector; the generic node stays connector-agnostic.</p>
     *
     * @param inferred the compression type the generic node inferred from the file path
     * @return the compression type to actually send to BE (identity by default)
     */
    default TFileCompressType adjustFileCompressType(TFileCompressType inferred) {
        return inferred;
    }

    /**
     * The columns BE must READ for this scan even when the query references none of them, by Doris-side
     * column name. The engine keeps their slots in the scan's tuple instead of pruning them away; the
     * projection above the scan still removes them from the query's output, so the answer changes what is
     * read, never what is returned.
     *
     * <p>This exists for a connector whose BE-side reader needs a column to produce CORRECT ROWS rather than
     * to answer the query — a merge key, a suppression key, a row identity. Doris does the same thing for its
     * own aggregate / merge-on-read unique-key tables ({@code PhysicalPlanTranslator.preserveExtraStorageKeySlots}):
     * BE merges by key whether or not the user selected the key. Trino has no counterpart because its
     * connectors own the page source and can add such columns privately; here the reader is BE, so the columns
     * have to reach it through the plan.</p>
     *
     * <p>Answer per SCAN, not per table: a connector that only sometimes needs the column (e.g. only when it
     * decides to combine two sources) must return it only for those scans, and must reach the SAME decision
     * when it later plans the splits — the engine asks this during plan translation, strictly before
     * {@link #planScan}. Memoize that decision on the provider instance (the engine keeps one per scan node)
     * rather than deciding twice: two independent decisions can disagree, and then BE is asked to read a
     * column the tuple does not carry.</p>
     *
     * <p>Every name returned must be a column of the scanned table, spelled as Doris knows it (the same
     * identifier-mapped name {@link #classifyColumn} receives). A name that matches no slot in the scan's
     * tuple fails the query loud: it means the connector and the engine disagree about the table, and reading
     * on would silently produce whatever the connector's reader does without that column.</p>
     *
     * <p>The default returns an empty set — every connector whose reader needs nothing beyond the projection
     * is untouched, and its scans prune exactly as before.</p>
     *
     * @param session the current session
     * @param handle  the table handle being scanned
     * @return Doris-side names of the columns to read regardless of the projection (default: empty)
     */
    default Set<String> getMustReadColumns(ConnectorSession session, ConnectorTableHandle handle) {
        return Collections.emptySet();
    }

    /**
     * Plans the scan described by {@code request}, returning the ranges that cover the requested data.
     *
     * <p>This is the one method a scanning connector must implement. Everything the engine can tell it about
     * the scan — columns, remaining filter, row limit, pruned partitions, {@code COUNT(*)} pushdown — arrives
     * on {@link ConnectorScanRequest}, and a connector consumes what it can serve and ignores the rest. It
     * replaced a chain of four overloads in which only the shortest was abstract, so implementing the obvious
     * one silently discarded the limit, the partition pruning and the count signal.</p>
     *
     * @param session the current session
     * @param request what to scan and what the engine has already pushed down
     * @return the scan ranges covering the requested data
     */
    List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request);

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
     * Whether the ranges this connector plans are read by BE's NATIVE file readers, so BE's file cache
     * applies to them and the engine should run its file-cache admission governance for these tables.
     *
     * <p>{@code false} (the default) keeps a connector out of that governance, which is right for anything
     * read through JNI or over a remote protocol: it never populates the BE file cache, so evaluating an
     * admission rule for it only spends the lookup. A connector whose ranges BE reads natively (the lake
     * formats reading parquet/orc off object storage) returns {@code true}, and does so regardless of which
     * catalog type its tables live under — that is the point of asking the connector rather than matching a
     * catalog type name. Mirrors {@link #supportsTableSample}'s opt-in shape.</p>
     *
     * @return whether BE file-cache admission governance applies to this connector's scans (default: false)
     */
    default boolean supportsFileCache() {
        return false;
    }

    /**
     * The number of DISTINCT native partitions among the just-planned scan ranges — the count the
     * connector's SDK actually resolved after ITS full manifest/residual/transform/bucket pruning.
     * Feeds the scan node's {@code selectedPartitionNum} (EXPLAIN {@code partition=N/M} and the
     * {@code sql_block_rule} {@code partition_num} guard), so the reported count reflects what is
     * really scanned rather than the engine's declared-partition-column (Nereids) prune count.
     *
     * <p>The default returns {@link OptionalLong#empty()}, so the generic node keeps its Nereids-pruned
     * count — correct for directory-partitioned / requiredPartition-driven connectors (hive, MaxCompute),
     * where the two coincide. A predicate-driven connector whose SDK prunes beyond the engine (Paimon
     * manifest pruning, Iceberg hidden/transform partitioning) overrides this. Mirrors the
     * {@link #supportsTableSample} opt-in shape; the connector downcasts its OWN range type (it produced
     * these very ranges) to read partition identity, so the generic node stays connector-agnostic. It
     * must never over-count (each native partition must map to exactly one identity within a scan), so it
     * can only tighten, never loosen, the {@code partition_num} guard relative to the Nereids count.</p>
     *
     * @param scanRanges the ranges this provider just returned from {@code planScan}
     * @return the distinct scanned-partition count, or empty to keep the engine's Nereids count (default)
     */
    default OptionalLong scannedPartitionCount(List<ConnectorScanRange> scanRanges) {
        return OptionalLong.empty();
    }

    /**
     * Connector SDK scan diagnostics harvested during the just-finished {@link #planScan} — manifest cache
     * hit/miss, scan/planning durations, files and manifests scanned vs skipped — as connector-neutral
     * {@link ConnectorScanProfile} groups the engine writes into the query's profile execution summary.
     *
     * <p>The default returns an empty list (connector reports nothing). A connector that wants scan
     * diagnostics harvests them from its SDK during {@code planScan} (the paimon SDK exposes a metric registry,
     * the iceberg SDK a metrics reporter), stashes them keyed by {@link ConnectorSession#getQueryId()}, and drains
     * them here — mirroring the per-query queryId stashes this SPI already uses (read-transaction release,
     * rewritable-delete supply). The engine calls this immediately after {@code planScan} on the same thread, so
     * the harvest is complete; the connector must also drop its stash on {@link #releaseReadTransaction} to
     * reclaim an entry left by a failed planning path.</p>
     *
     * @param session the current session (its queryId keys the connector's per-query stash)
     * @return this scan's diagnostics, or an empty list (the default) to contribute nothing to the profile
     */
    default List<ConnectorScanProfile> collectScanProfiles(ConnectorSession session) {
        return Collections.emptyList();
    }

    /**
     * Connector SDK scan diagnostics harvested while the engine consumes the source returned by
     * {@link #streamSplits}. The engine calls this on the streaming producer thread only after closing that
     * source, and publishes the returned profiles before signaling split completion or failure.
     *
     * <p>This is deliberately separate from {@link #collectScanProfiles}: an existing connector may validly
     * implement that eager hook using state established by {@link #planScan} or thread-local planning context.
     * Streaming never calls {@code planScan} and runs on a background thread, so reusing the eager hook would
     * violate its lifecycle contract. The empty default is an explicit opt-in boundary.</p>
     *
     * @param session the current session
     * @return this streaming scan's diagnostics, or an empty list (the default) to contribute nothing
     */
    default List<ConnectorScanProfile> collectStreamingScanProfiles(ConnectorSession session) {
        return Collections.emptyList();
    }

    /**
     * Plans the scan for a single batch of partitions (used by batch-mode scans).
     *
     * <p>Called once per partition batch when the engine drives batch-mode split generation
     * (see {@link #supportsBatchScan}). Each call should build a read session over exactly the
     * given {@code partitionBatch} and return that batch's scan ranges. The default re-scopes the
     * request to {@code partitionBatch} and calls {@link #planScan}, which is correct for connectors
     * whose {@code planScan} builds one read session per partition set (e.g. MaxCompute). A connector
     * whose {@code planScan} is not partition-set-scoped must override this method (and
     * {@link #supportsBatchScan}) before enabling batch mode — inheriting the default would re-plan the
     * WHOLE pruned set once per batch and emit every partition's files repeatedly.</p>
     *
     * @param session        the current session
     * @param request        the scan request; its partition set is replaced by this batch
     * @param partitionBatch the partition spec strings for this batch (non-empty)
     * @return the scan ranges for this partition batch
     */
    default List<ConnectorScanRange> planScanForPartitionBatch(
            ConnectorSession session,
            ConnectorScanRequest request,
            List<String> partitionBatch) {
        return planScan(session, request.withRequiredPartitions(partitionBatch));
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
     * <p>The keys the engine reads are declared in {@link ScanNodePropertyKeys}; use those constants rather
     * than string literals. Any other key is connector-private (the engine passes the map back to
     * {@link #populateScanLevelParams} and {@link #appendExplainInfo}, so a connector can carry its own
     * information through it) and should be namespaced with the connector's own name.</p>
     *
     * <p><b>Override this face OR {@link #getScanNodePropertiesResult}, not both.</b> The engine only ever
     * calls the result face, whose default implementation delegates here; a connector that overrides both
     * with different content has the map returned by this method silently discarded.</p>
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
     * Returns scan-node-level properties along with filter pushdown results.
     *
     * <p>Override this when the connector performs fine-grained conjunct pushdown
     * (e.g., ES query DSL building) and needs to report which conjuncts
     * were NOT pushed. The indices in {@link ScanNodePropertiesResult#getNotPushedConjunctIndices()}
     * refer to the AND children of the filter expression, in the same order as
     * the conjuncts list.</p>
     *
     * <p>The indices are into the conjunct list AS RECEIVED, i.e. after
     * {@code PluginDrivenScanNode.buildRemainingFilter} has dropped CAST-wrapped conjuncts when
     * {@link org.apache.doris.connector.spi.ConnectorPushdownOps#supportsCastPredicatePushdown} is false; the
     * engine maps them back to the original positions itself and always keeps the conjuncts it never sent.
     * When {@code filter} is a single non-AND node (the engine does not wrap a lone conjunct in an AND),
     * index 0 refers to that whole expression.</p>
     *
     * <p>The default wraps {@link #getScanNodeProperties} in a result WITHOUT conjunct tracking, which makes
     * {@code PluginDrivenScanNode.pruneConjunctsFromNodeProperties} remove nothing: every conjunct is still
     * evaluated on BE. That is the safe default — it is NOT "all conjuncts are assumed pushed". Reporting an
     * empty not-pushed set through {@link ScanNodePropertiesResult#withPushdownTracking} is the opposite
     * claim (everything was pushed, prune them all), so use it only when the pushdown was exact.</p>
     *
     * <p><b>This is the face the engine calls.</b> A connector that needs conjunct tracking overrides this
     * one and leaves {@link #getScanNodeProperties} alone — overriding both is how the {@code Map} face's
     * result gets silently discarded.</p>
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
        return ScanNodePropertiesResult.of(
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
     * Whether unannotated Parquet INT96 values use the Hive writer timezone configured on the catalog.
     * The default is false because table formats such as Iceberg and Paimon define their own timestamp
     * semantics even when they are discovered through an HMS-backed catalog.
     */
    default boolean usesHiveParquetInt96TimeZone() {
        return false;
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
     * <p><b>Where that transaction may be kept.</b> This method releases state opened by a DIFFERENT call, yet
     * a provider instance cannot carry it: providers are built fresh per acquisition ({@code
     * Connector.getScanPlanProvider}), so the instance that opened the transaction is generally not the one
     * asked to release it. The state therefore belongs to the CONNECTOR — a connector-scoped, query-id-keyed
     * registry that both the opening {@code planScan} and this callback reach through the connector instance
     * they were built from (this is what hive does). A provider that stashes the transaction in its own field
     * leaks it, and no test will say so. The same rule applies to any other cross-call state a provider
     * appears to need: put it on the connector, keyed by query id, and clean it up here.</p>
     *
     * @param queryId the finishing query's id (== {@link ConnectorSession#getQueryId()})
     */
    default void releaseReadTransaction(String queryId) {
        // default: no per-query read transaction to release
    }
}
