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

package org.apache.doris.datasource;

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.PassthroughQueryTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.api.pushdown.LimitApplicationResult;
import org.apache.doris.connector.api.pushdown.ProjectionApplicationResult;
import org.apache.doris.connector.api.scan.ConnectorColumnCategory;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorSplitSource;
import org.apache.doris.connector.api.scan.ScanNodePropertiesResult;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TColumnCategory;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Generic scan node that delegates scan planning to the connector SPI.
 *
 * <p>Replaces connector-specific ScanNode subclasses for plugin-driven catalogs.
 * Extends {@link FileQueryScanNode} to reuse the existing split-to-Thrift
 * conversion pipeline. Uses {@code FORMAT_JNI} by default, which routes
 * to BE's JNI scanner framework.</p>
 *
 * <p>Scan flow:</p>
 * <ol>
 *   <li>{@link #getSplits} calls {@link ConnectorScanPlanProvider#planScan}
 *       to obtain {@link ConnectorScanRange}s from the connector plugin</li>
 *   <li>Each range is wrapped in a {@link PluginDrivenSplit}</li>
 *   <li>{@link FileQueryScanNode#createScanRangeLocations} distributes splits
 *       to backends</li>
 *   <li>{@link #setScanParams} populates {@link TTableFormatFileDesc} with
 *       connector-specific properties for each split</li>
 * </ol>
 */
public class PluginDrivenScanNode extends FileQueryScanNode {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenScanNode.class);

    private static final String TABLE_FORMAT_TYPE = "plugin_driven";

    /** Scan node property keys (shared with connector plugins). */
    private static final String PROP_FILE_FORMAT_TYPE = "file_format_type";
    private static final String PROP_PATH_PARTITION_KEYS = "path_partition_keys";
    private static final String PROP_LOCATION_PREFIX = "location.";
    private static final String PROP_HIVE_TEXT_PREFIX = "hive.text.";

    // FIX-E (explain gap): synthetic node-property keys threaded into the props map passed to the
    // connector's appendExplainInfo, carrying the native/total split counts this node accumulated from
    // ConnectorScanRange.isNativeReadRange() in getSplits(). They are NOT real connector properties
    // (never reach BE) — only a connector that surfaces a native/JNI split distinction (paimon) reads
    // them to emit its "paimonNativeReadSplits=<raw>/<total>" line. Byte-identical to the keys
    // PaimonScanPlanProvider consumes, so the inject/consume sides stay in lockstep. Connector-agnostic:
    // injected for every plugin connector but consumed only by the one that opts in.
    private static final String NATIVE_READ_SPLITS_KEY = "__native_read_splits";
    private static final String TOTAL_READ_SPLITS_KEY = "__total_read_splits";
    // FIX-E (explain gap): injected (="true") into the connector's appendExplainInfo props ONLY when this
    // node renders a VERBOSE EXPLAIN, so a connector can gate VERBOSE-only output (paimon's per-split
    // "PaimonSplitStats:" block) without an SPI signature change. Connector-agnostic: injected for every
    // plugin connector but consumed only by the one that opts in. Byte-identical to the key
    // PaimonScanPlanProvider consumes.
    private static final String VERBOSE_EXPLAIN_KEY = "__explain_verbose";

    private final Connector connector;
    private final ConnectorSession connectorSession;

    // Set during filter pushdown; may be updated from the original table handle.
    private ConnectorTableHandle currentHandle;

    // Nereids partition-pruning result, injected by the translator. Defaults to NOT_PRUNED
    // so that connectors / non-partitioned tables read all partitions unless pruning applies.
    private SelectedPartitions selectedPartitions = SelectedPartitions.NOT_PRUNED;

    // Cached isBatchMode() result. isBatchMode is read on both the dispatch (FileQueryScanNode)
    // and explain (FileScanNode) paths and num_partitions_in_batch_mode is fuzzy, so cache it to
    // keep the decision stable across reads (mirrors IcebergScanNode).
    private Boolean isBatchModeCache;

    // FIX-M3 (streaming splits): when a connector opts into file-count streaming split generation
    // (ConnectorScanPlanProvider.streamingSplitEstimate >= 0), this caches that estimate and flags the
    // streaming flavor of batch mode — distinct from the partition-count flavor in shouldUseBatchMode.
    // -1 / false until computeBatchMode runs.
    private long streamingSplitEstimate = -1;
    private boolean streamingBatch;

    // FIX-E (explain gap): native (ORC/Parquet) vs total scan-range counts accumulated in getSplits()
    // from ConnectorScanRange.isNativeReadRange(), surfaced to the connector's appendExplainInfo for the
    // "paimonNativeReadSplits=<raw>/<total>" line. Default 0/0 (no native splits) before getSplits runs.
    private int nativeReadSplitNum;
    private int totalReadSplitNum;

    // Populated from ConnectorScanPlanProvider.getScanNodePropertiesResult()
    private ScanNodePropertiesResult cachedPropertiesResult;
    private Map<String, String> scanNodeProperties;
    // Maps filtered conjunct indices (after CAST removal) back to original conjunct indices
    private List<Integer> filteredToOriginalIndex;

    public PluginDrivenScanNode(PlanNodeId id, TupleDescriptor desc,
            boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext, Connector connector,
            ConnectorSession connectorSession, ConnectorTableHandle tableHandle) {
        super(id, desc, "PluginDrivenScanNode", scanContext, needCheckColumnPriv, sv);
        this.connector = connector;
        this.connectorSession = connectorSession;
        this.currentHandle = tableHandle;
    }

    /**
     * Creates a PluginDrivenScanNode by resolving the connector, session, and table handle
     * from the plugin-driven catalog and table.
     */
    public static PluginDrivenScanNode create(PlanNodeId id, TupleDescriptor desc,
            boolean needCheckColumnPriv, SessionVariable sv,
            ScanContext scanContext, PluginDrivenExternalCatalog catalog,
            PluginDrivenExternalTable table) {
        Connector connector = catalog.getConnector();
        ConnectorSession session = catalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        String dbName = table.getDb() != null ? table.getDb().getRemoteName() : "";
        // Resolve through the table's sys-aware seam (NOT raw metadata.getTableHandle): for a normal
        // table this is identical to getTableHandle(session, dbName, remoteName), but for a
        // PluginDrivenSysExternalTable the override returns the connector's SYSTEM handle (carrying
        // sysTableName + forceJni), so the scan path threads force-JNI correctly for binlog/audit_log.
        ConnectorTableHandle handle = table.resolveConnectorTableHandle(session, metadata)
                .orElseThrow(() -> new RuntimeException(
                        "Table handle not found for plugin-driven table: " + dbName + "."
                                + table.getRemoteName()));
        return new PluginDrivenScanNode(id, desc, needCheckColumnPriv, sv,
                scanContext, connector, session, handle);
    }

    /**
     * Injects the Nereids partition-pruning result. Called by the translator so the pruned
     * partition set can be pushed down to the connector's scan plan (see {@link #getSplits}).
     */
    public void setSelectedPartitions(SelectedPartitions selectedPartitions) {
        this.selectedPartitions = selectedPartitions;
    }

    /**
     * Resolves the pruned partition spec strings to push to the connector SPI.
     *
     * <p>Mirrors legacy {@code MaxComputeScanNode.getSplits()} three-state handling:</p>
     * <ul>
     *   <li>not pruned (NOT_PRUNED / non-partitioned) &rarr; {@code null}: scan all partitions;</li>
     *   <li>pruned to a non-empty set &rarr; that set's partition names;</li>
     *   <li>pruned to zero partitions &rarr; empty list: caller short-circuits with no splits.</li>
     * </ul>
     */
    static List<String> resolveRequiredPartitions(SelectedPartitions selectedPartitions) {
        return resolveRequiredPartitions(selectedPartitions, false);
    }

    /**
     * Overload that lets a PREDICATE-DRIVEN connector opt out of the genuine prune-to-zero short-circuit.
     *
     * <p>A connector whose {@link ConnectorScanPlanProvider#ignorePartitionPruneShortCircuit()} is {@code
     * true} (e.g. paimon, whose {@code planScan} ignores {@code requiredPartitions} and re-plans through the
     * SDK with the pushed predicate) must NOT be short-circuited to zero rows when FE pruning empties the
     * partition set: with master-parity {@code isNull=false} a genuine-null partition renders as a non-null
     * sentinel, so {@code col IS NULL} prunes every partition away, yet the genuine-null rows must still be
     * returned via the pushed predicate (regression {@code qt_null_partition_4}). For such a connector a
     * prune-to-zero maps to {@code null} (scan-all) instead of the empty list, exactly as the legacy
     * {@code PaimonScanNode} (which never consults {@code selectedPartitions}). A non-empty pruned set is
     * still forwarded unchanged. For every other connector ({@code ignorePartitionPruneShortCircuit=false})
     * the behavior is identical to {@link #resolveRequiredPartitions(SelectedPartitions)}.</p>
     */
    static List<String> resolveRequiredPartitions(SelectedPartitions selectedPartitions,
            boolean ignorePartitionPruneShortCircuit) {
        if (selectedPartitions == null || !selectedPartitions.isPruned) {
            return null;
        }
        // A pruned-but-EMPTY selection over an EMPTY partition universe (totalPartitionNum == 0) means FE
        // never enumerated any partitions to prune against — e.g. an MVCC time-travel pin (FOR VERSION/TIME
        // AS OF, @tag, @branch) whose snapshot deliberately carries an empty partition-item map and defers
        // partition resolution to the connector's predicate pushdown. Treat it as scan-all (null) so the
        // getSplits() short-circuit does NOT fire and planScan runs, mirroring legacy PaimonScanNode (which
        // ignores selectedPartitions and re-plans through the SDK with the pushed predicate). A GENUINE
        // prune-to-zero over a NON-empty universe (totalPartitionNum > 0, e.g. MaxCompute WHERE
        // part=<absent value>) keeps the empty set so getSplits() short-circuits to zero rows — the
        // existing MaxCompute parity behavior (FIX-NONPART-PRUNE-DATALOSS sibling). NB: for a partitioned
        // MaxCompute table that genuinely has ZERO partitions this scan-all is row-equivalent to legacy's
        // unconditional empty short-circuit — MaxComputeScanPlanProvider.planScan returns no splits when
        // getFileNum() <= 0, still zero rows.
        if (selectedPartitions.selectedPartitions.isEmpty() && selectedPartitions.totalPartitionNum == 0) {
            return null;
        }
        // A predicate-driven connector re-plans through its SDK with the pushed predicate (its planScan
        // ignores requiredPartitions), so a GENUINE prune-to-zero must NOT short-circuit the scan — return
        // scan-all (null) and let planScan answer from the predicate (e.g. paimon `col IS NULL` over a
        // genuine-null partition the FE pruner rendered as a non-null sentinel). Master PaimonScanNode parity.
        if (ignorePartitionPruneShortCircuit && selectedPartitions.selectedPartitions.isEmpty()) {
            return null;
        }
        return new ArrayList<>(selectedPartitions.selectedPartitions.keySet());
    }

    /**
     * Partition counts to surface on this scan node — {@code {selectedPartitionNum, totalPartitionNum}}
     * — or {@code null} to leave the fields at their default (nothing to show). Drives the EXPLAIN
     * {@code partition=N/M} line and SQL-block-rule enforcement (via {@code getSelectedPartitionNum()}).
     *
     * <p>Mirrors legacy {@code MaxComputeScanNode}'s display gate: any <em>real</em> partition selection
     * reports {@code size/total}, whereas the {@link SelectedPartitions#NOT_PRUNED} sentinel
     * (non-partitioned table, or one not supporting internal pruning) reports nothing.</p>
     *
     * <p>The gate is {@code != NOT_PRUNED}, deliberately <b>not</b> {@code isPruned}: a partitioned table
     * queried without a partition predicate keeps the initial all-partitions selection from
     * {@link ExternalTable#initSelectedPartitions} ({@code isPruned=false} but a full, non-{@code
     * NOT_PRUNED} map; {@code PruneFileScanPartition} only runs under a {@code LogicalFilter}), and must
     * still report {@code partition=total/total} (e.g. {@code SELECT *} over a 2-partition table &rarr;
     * {@code 2/2}). An {@code isPruned} gate wrongly shows {@code 0/0}. This differs from the connector
     * pushdown gate ({@link #resolveRequiredPartitions}, which stays {@code isPruned}): an unpruned scan
     * must read ALL partitions, so it pushes no partition restriction.</p>
     */
    static long[] displayPartitionCounts(SelectedPartitions selectedPartitions) {
        if (selectedPartitions == null || selectedPartitions == SelectedPartitions.NOT_PRUNED) {
            return null;
        }
        return new long[] {
                selectedPartitions.selectedPartitions.size(), selectedPartitions.totalPartitionNum};
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        if (currentHandle instanceof PassthroughQueryTableHandle) {
            output.append(prefix).append("TABLE VALUE FUNCTION\n");
            String query = ((PassthroughQueryTableHandle) currentHandle).getQuery();
            output.append(prefix).append("QUERY: ").append(query).append("\n");
        } else {
            Map<String, String> props = getOrLoadScanNodeProperties();
            String query = props.get("query");
            output.append(prefix).append("TABLE: ")
                    .append(desc.getTable().getNameWithFullQualifiers()).append("\n");
            // Surface the backing connector/catalog type (e.g. es, jdbc, max_compute) so the
            // generic node name does not hide which connector this scan delegates to. Reuses the
            // same getDatabase().getCatalog() chain getNameWithFullQualifiers() already walks here.
            output.append(prefix).append("CONNECTOR: ")
                    .append(desc.getTable().getDatabase().getCatalog().getType()).append("\n");
            if (query != null) {
                output.append(prefix).append("QUERY: ").append(query).append("\n");
            }
            if (!conjuncts.isEmpty()) {
                Expr expr = convertConjunctsToAndCompoundPredicate(conjuncts);
                output.append(prefix).append("PREDICATES: ")
                        .append(expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE))
                        .append("\n");
            }
            // FIX-E (explain gap): the parent FileScanNode emits an
            // "inputSplitNum=N, totalFileSize=X, scanRanges=Y" line that this override drops by not
            // calling super (legacy PaimonScanNode inherited it via super.getNodeExplainString;
            // test_paimon_predict asserts inputSplitNum=N). Re-emit it byte-for-byte (incl. the
            // (approximate) batch prefix) from the same selectedSplitNum/totalFileSize/scanRangeLocations
            // the parent populates in createScanRangeLocations, immediately before partition=N/M to match
            // FileScanNode ordering. Emitted UNCONDITIONALLY for every plugin connector — like the sibling
            // partition=N/M (below) and pushdown agg= lines — since it is universal FileScanNode info, not
            // connector-specific (no source-type branch in this generic node).
            output.append(prefix);
            if (isBatchMode()) {
                output.append("(approximate)");
            }
            output.append("inputSplitNum=").append(selectedSplitNum).append(", totalFileSize=")
                    .append(totalFileSize).append(", scanRanges=").append(scanRangeLocations.size())
                    .append("\n");
            // Partition-pruning summary (selected/total), mirroring the parent
            // FileScanNode.getNodeExplainString()'s `partition=N/M` line. This override replaces the
            // parent's body wholesale (custom TABLE/QUERY/PREDICATES format), so it must re-emit the
            // line itself; the counts are populated from the Nereids pruning result in
            // getSplits()/startSplit() (see setSelectedPartitions).
            output.append(prefix).append("partition=").append(selectedPartitionNum)
                    .append("/").append(totalPartitionNum).append("\n");
            // FIX-E / FIX-R3-RESIDUAL (explain gap): the VERBOSE per-backend block (the backends: list,
            // per-file "path start/length" lines, and dataFileNum/deleteFileNum/deleteSplitNum) lives in
            // the parent FileScanNode but this override does not call super, so re-emit it under the SAME
            // gate the parent uses: VERBOSE && !isBatchMode() (FileScanNode#getNodeExplainString). Emitted
            // UNCONDITIONALLY for every plugin connector -- like the sibling inputSplitNum / partition=N/M
            // (above) and pushdown agg= (below) lines -- because it is universal FileScanNode info, not
            // connector-specific: NO source-name branch belongs in this generic node (a "paimon".equals(
            // getType()) gate here previously dropped it for non-paimon connectors). This RESTORES the
            // block that legacy MaxComputeScanNode / TrinoConnectorScanNode inherited from FileScanNode
            // pre-cutover, and is consistent for every FILE_SCAN plugin connector (es/jdbc render their
            // synthetic per-split path; connectors with no delete files render deleteFileNum=0 via
            // getDeleteFiles -> empty). Connector-SPECIFIC EXPLAIN stays delegated to
            // ConnectorScanPlanProvider.appendExplainInfo below; this block is emitted before that
            // delegation so the ordering matches the legacy PaimonScanNode (FileScanNode body, then the
            // connector's lines).
            if (detailLevel == TExplainLevel.VERBOSE && !isBatchMode()) {
                appendBackendScanRangeDetail(output, prefix);
            }
            // Delegate connector-specific EXPLAIN info to the SPI. Thread the native/total split counts
            // (FIX-E) the node accumulated in getSplits() into a copy of the props map via the synthetic
            // keys, so a connector that distinguishes native/JNI reads (paimon) can emit its
            // "paimonNativeReadSplits=<raw>/<total>" line without an SPI signature change. The copy keeps
            // the cached scanNodeProperties unpolluted; non-paimon providers ignore the extra keys.
            ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
            if (scanProvider != null) {
                Map<String, String> explainProps = new HashMap<>(props);
                explainProps.put(NATIVE_READ_SPLITS_KEY, String.valueOf(nativeReadSplitNum));
                explainProps.put(TOTAL_READ_SPLITS_KEY, String.valueOf(totalReadSplitNum));
                if (detailLevel == TExplainLevel.VERBOSE) {
                    explainProps.put(VERBOSE_EXPLAIN_KEY, "true");
                }
                onPluginClassLoader(scanProvider, () -> {
                    scanProvider.appendExplainInfo(output, prefix, explainProps);
                    return null;
                });
            }
            // FIX-E (explain gap): the "pushdown agg=<op> (n)" line lives in the parent FileScanNode
            // but this override does not call super. Re-emit it for ALL plugin connectors (universally
            // correct — its absence on plugin nodes is itself an inconsistency vs every other
            // FileScanNode). When a no-grouping COUNT(*) is pushed down, tableLevelRowCount is set in
            // getSplits() from the connector's precomputed count (or stays -1 -> the (-1) sentinel).
            output.append(prefix).append(String.format("pushdown agg=%s", getPushDownAggNoGroupingOp()));
            if (getPushDownAggNoGroupingOp() == TPushAggOp.COUNT) {
                output.append(" (").append(tableLevelRowCount).append(")");
            }
            output.append("\n");
            // Show ES terminate_after optimization when limit is pushed to ES
            if (limit > 0 && conjuncts.isEmpty()
                    && "es_http".equals(props.get(PROP_FILE_FORMAT_TYPE))) {
                output.append(prefix).append("ES terminate_after: ")
                        .append(limit).append("\n");
            }
        }
        if (useTopnFilter()) {
            String topnFilterSources = String.join(",",
                    topnFilterSortNodes.stream()
                            .map(node -> node.getId().asInt() + "").collect(Collectors.toList()));
            output.append(prefix).append("TOPN OPT:").append(topnFilterSources).append("\n");
        }
        return output.toString();
    }

    @Override
    protected TFileFormatType getFileFormatType() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String format = props.get(PROP_FILE_FORMAT_TYPE);
        if (format != null) {
            return mapFileFormatType(format);
        }
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    protected List<String> getPathPartitionKeys() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String keys = props.get(PROP_PATH_PARTITION_KEYS);
        if (keys != null && !keys.isEmpty()) {
            return Arrays.asList(keys.split(","));
        }
        return Collections.emptyList();
    }

    /**
     * Classifies a query slot's column for the BE reader (C2 WS-SYNTH-READ). This is the generic,
     * connector-agnostic port of the per-connector overrides (legacy {@code IcebergScanNode.classifyColumn},
     * {@code HiveScanNode}, {@code TVFScanNode}): it must keep the synthesized / generated special columns
     * out of the file-read set so they are materialized by the connector reader rather than read from a data
     * file where they do not exist.
     *
     * <p>Two sources, no connector-type branching:</p>
     * <ul>
     *   <li>{@code __DORIS_GLOBAL_ROWID_COL__*} — the engine-wide lazy-materialization row-id (injected by
     *       {@code LazyMaterializeTopN}, also classified by {@code HiveScanNode}/{@code TVFScanNode}) is a
     *       generic Doris mechanism, so it is classified here as {@code SYNTHESIZED} directly.</li>
     *   <li>connector special columns (e.g. iceberg's hidden row-id / v3 row-lineage) are classified by the
     *       connector through {@link ConnectorScanPlanProvider#classifyColumn(String)}, so no iceberg (or any
     *       connector) knowledge leaks into the generic node.</li>
     * </ul>
     * Everything else falls through to {@code super} (partition key / regular).
     */
    @Override
    protected TColumnCategory classifyColumn(SlotDescriptor slot, List<String> partitionKeys) {
        String name = slot.getColumn().getName();
        if (name.startsWith(Column.GLOBAL_ROWID_COL)) {
            return TColumnCategory.SYNTHESIZED;
        }
        ConnectorColumnCategory category = classifyColumnByConnector(name);
        if (category == ConnectorColumnCategory.SYNTHESIZED) {
            return TColumnCategory.SYNTHESIZED;
        }
        if (category == ConnectorColumnCategory.GENERATED) {
            return TColumnCategory.GENERATED;
        }
        return super.classifyColumn(slot, partitionKeys);
    }

    /**
     * Asks the connector how to classify a special column (iceberg's hidden row-id / v3 row-lineage), so no
     * connector knowledge leaks into {@link #classifyColumn}. Package-private + overridable so the mapping is
     * unit-testable on a Mockito mock without a live connector (mirrors {@link #sysTableSupportsTimeTravel}).
     * A connector with no scan provider (no scan capability) contributes no special columns ({@code DEFAULT}).
     */
    ConnectorColumnCategory classifyColumnByConnector(String columnName) {
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider == null) {
            return ConnectorColumnCategory.DEFAULT;
        }
        return onPluginClassLoader(scanProvider, () -> scanProvider.classifyColumn(columnName));
    }

    @Override
    protected TableIf getTargetTable() throws UserException {
        return desc.getTable();
    }

    /**
     * Runs a connector scan-plan call with the thread-context classloader pinned to the connector
     * plugin's own loader, restoring it afterward.
     *
     * <p>Plugin connectors run isolated under a child-first classloader, while fe-core ships some of
     * the same third-party libraries (e.g. iceberg) on the parent 'app' loader. Such libraries resolve
     * helper classes by name through the TCCL — iceberg-aws's {@code HttpClientProperties} loads
     * {@code ApacheHttpClientConfigurations} via {@code DynMethods}, whose default loader IS the TCCL.
     * Under the query thread's default ('app') TCCL that reflective load returns the parent copy and
     * {@link ClassCastException}s against the child-loaded plugin copy. Pinning the TCCL to the
     * provider's loader keeps every reflective load on the plugin side — the same split-brain guard
     * {@code IcebergConnector.buildCatalogAuthenticated} applies on the catalog path. Keyed off the
     * provider's own classloader, so it is connector-agnostic and a no-op for connectors that are not
     * classloader-isolated. Must wrap the call on the thread that runs it: the streaming split paths
     * execute on a pool thread that does not inherit the caller's TCCL.
     */
    private static <T> T onPluginClassLoader(ConnectorScanPlanProvider provider, Supplier<T> body) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(provider.getClass().getClassLoader());
            return body.get();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * FIX-E (explain gap): delegates the VERBOSE per-backend block's delete-file lookup to the
     * connector SPI. The parent {@link FileScanNode#getDeleteFiles} returns empty; a connector that
     * threads delete files onto its per-range thrift (paimon's deletion vectors) overrides
     * {@link ConnectorScanPlanProvider#getDeleteFiles(TTableFormatFileDesc)} to read them back. Reads
     * the table-format params off the range (null-guarded, mirroring legacy
     * {@code PaimonScanNode.getDeleteFiles}); connectors without delete files return empty, so the
     * {@code deleteFileNum} count stays 0.
     */
    @Override
    protected List<String> getDeleteFiles(TFileRangeDesc rangeDesc) {
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider == null || rangeDesc == null || !rangeDesc.isSetTableFormatParams()) {
            return Collections.emptyList();
        }
        return onPluginClassLoader(scanProvider, () -> scanProvider.getDeleteFiles(rangeDesc.getTableFormatParams()));
    }

    @Override
    protected Map<String, String> getLocationProperties() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        Map<String, String> locationProps = new HashMap<>();
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (entry.getKey().startsWith(PROP_LOCATION_PREFIX)) {
                String realKey = entry.getKey().substring(PROP_LOCATION_PREFIX.length());
                locationProps.put(realKey, entry.getValue());
            }
        }
        return locationProps;
    }

    @Override
    protected TFileAttributes getFileAttributes() throws UserException {
        Map<String, String> props = getOrLoadScanNodeProperties();
        String serDeLib = props.get(PROP_HIVE_TEXT_PREFIX + "serde_lib");
        if (serDeLib == null || serDeLib.isEmpty()) {
            return new TFileAttributes();
        }

        TFileAttributes attrs = new TFileAttributes();
        String skipLinesStr = props.get(PROP_HIVE_TEXT_PREFIX + "skip_lines");
        if (skipLinesStr != null) {
            try {
                attrs.setSkipLines(Integer.parseInt(skipLinesStr));
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        TFileTextScanRangeParams textParams = new TFileTextScanRangeParams();
        String colSep = props.get(PROP_HIVE_TEXT_PREFIX + "column_separator");
        if (colSep != null) {
            textParams.setColumnSeparator(colSep);
        }
        String lineSep = props.get(PROP_HIVE_TEXT_PREFIX + "line_delimiter");
        if (lineSep != null) {
            textParams.setLineDelimiter(lineSep);
        }
        String mapkvDelim = props.get(PROP_HIVE_TEXT_PREFIX + "mapkv_delimiter");
        if (mapkvDelim != null) {
            textParams.setMapkvDelimiter(mapkvDelim);
        }
        String collDelim = props.get(PROP_HIVE_TEXT_PREFIX + "collection_delimiter");
        if (collDelim != null) {
            textParams.setCollectionDelimiter(collDelim);
        }
        String escape = props.get(PROP_HIVE_TEXT_PREFIX + "escape");
        if (escape != null && !escape.isEmpty()) {
            textParams.setEscape(escape.getBytes()[0]);
        }
        String nullFmt = props.get(PROP_HIVE_TEXT_PREFIX + "null_format");
        if (nullFmt != null) {
            textParams.setNullFormat(nullFmt);
        }
        String enclose = props.get(PROP_HIVE_TEXT_PREFIX + "enclose");
        if (enclose != null && !enclose.isEmpty()) {
            textParams.setEnclose(enclose.getBytes()[0]);
            attrs.setTrimDoubleQuotes(true);
        }

        attrs.setTextParams(textParams);
        attrs.setHeaderType("");
        attrs.setEnableTextValidateUtf8(sessionVariable.enableTextValidateUtf8);

        String isJson = props.get(PROP_HIVE_TEXT_PREFIX + "is_json");
        if ("true".equals(isJson)) {
            attrs.setReadJsonByLine(true);
            attrs.setReadByColumnDef(true);
        }

        return attrs;
    }

    @Override
    protected void convertPredicate() {
        // Attempt filter pushdown via the connector SPI
        if (conjuncts == null || conjuncts.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        ConnectorFilterConstraint constraint = buildFilterConstraint(conjuncts);
        Optional<FilterApplicationResult<ConnectorTableHandle>> result =
                metadata.applyFilter(connectorSession, currentHandle, constraint);
        if (result.isPresent()) {
            FilterApplicationResult<ConnectorTableHandle> filterResult = result.get();
            currentHandle = filterResult.getHandle();

            // Consume remainingFilter to avoid duplicate predicate evaluation on BE:
            // - null means all predicates were fully pushed down → clear conjuncts
            // - non-null means some/all predicates remain → keep conjuncts (conservative)
            ConnectorExpression remaining = filterResult.getRemainingFilter();
            if (remaining == null) {
                conjuncts.clear();
                LOG.debug("Filter fully pushed down for plugin-driven scan, cleared conjuncts");
            } else {
                // Partial or full remaining: keep all conjuncts for BE-side evaluation.
                // Fine-grained conjunct removal (matching individual remaining sub-expressions
                // back to original Expr conjuncts) is deferred to a future enhancement.
                LOG.debug("Filter pushdown accepted with remaining filter, keeping conjuncts");
            }
        }
        // Invalidate cached properties so they are rebuilt with the updated conjuncts/handle.
        scanNodeProperties = null;
        cachedPropertiesResult = null;
        filteredToOriginalIndex = null;
    }

    /**
     * Attempts to push the limit down via the SPI applyLimit() protocol.
     * Called before getSplits(), after filter pushdown.
     *
     * <p>If the connector accepts the limit, the handle is updated.
     * The limit is still passed to planScan() as a parameter for
     * connectors that handle limit directly in planScan().</p>
     */
    private void tryPushDownLimit() {
        if (limit <= 0) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<LimitApplicationResult<ConnectorTableHandle>> result =
                metadata.applyLimit(connectorSession, currentHandle, limit);
        if (result.isPresent()) {
            currentHandle = result.get().getHandle();
            LOG.debug("Limit {} pushed down via applyLimit for plugin-driven scan", limit);
        }
    }

    /**
     * Attempts to push the projection down via the SPI applyProjection() protocol.
     * Called before getSplits(), after filter and limit pushdown.
     *
     * <p>If the connector accepts the projection, the handle is updated.</p>
     */
    private void tryPushDownProjection(List<ConnectorColumnHandle> columns) {
        if (columns.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<ProjectionApplicationResult<ConnectorTableHandle>> result =
                metadata.applyProjection(connectorSession, currentHandle, columns);
        if (result.isPresent()) {
            currentHandle = result.get().getHandle();
            LOG.debug("Projection pushed down via applyProjection for plugin-driven scan");
        }
    }

    /**
     * Threads the pinned MVCC snapshot (if any) onto the table handle via the SPI
     * {@link ConnectorMetadata#applySnapshot} protocol. WHY: an MVCC-capable connector (e.g. a
     * time-travel / MTMV-consistent read) must consume the SAME pinned point-in-time snapshot at
     * every scan-side consumption of the handle ({@code planScan} and the serialized-table /
     * {@code getScanNodeProperties} path); the pin therefore has to be threaded onto the handle
     * BEFORE each of those points or one path silently reads LATEST. {@code applySnapshot} is
     * idempotent (it re-derives the scan options from the snapshot each call), so calling this at
     * every consumption site is safe. A missing pin — before the connector is MVCC-cutover, or a
     * non-MVCC table, or a foreign (non-plugin) snapshot — leaves the handle unchanged (reads latest).
     *
     * <p>Public static so the correctness-critical pin-vs-skip decision is unit-testable directly on
     * Mockito mocks, without constructing a {@link FileQueryScanNode} (the call-site wiring is covered by
     * live e2e — see DV-019), and so the WRITE path can reuse the IDENTICAL pin logic: the connector sink
     * translator ({@code PhysicalPlanTranslator.visitPhysicalConnectorTableSink}) threads the same
     * statement pin onto the write handle so a DML's write anchors at the snapshot its scan read
     * ([SHOULD-2] / Fix B). Scan and write MUST pin identically — sharing this method guarantees that.
     */
    public static ConnectorTableHandle applyMvccSnapshotPin(ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle handle, Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent() && snapshot.get() instanceof PluginDrivenMvccSnapshot) {
            ConnectorMvccSnapshot connectorSnapshot =
                    ((PluginDrivenMvccSnapshot) snapshot.get()).getConnectorSnapshot();
            return metadata.applySnapshot(session, handle, connectorSnapshot);
        }
        // No pin in context, or a non-plugin snapshot -> read latest (unchanged handle).
        return handle;
    }

    /**
     * Resolves the pinned MVCC snapshot from the statement context and threads it onto
     * {@link #currentHandle} (mutates the handle exactly like {@link #tryPushDownProjection} /
     * {@link #tryPushDownLimit}). Called at every scan-side handle-consumption point so both the
     * split path and the serialized-table path read at the pinned snapshot.
     */
    private void pinMvccSnapshot() throws UserException {
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Optional<MvccSnapshot> snapshot = MvccUtil.getSnapshotFromContext(getTargetTable());
        if (!snapshot.isPresent()) {
            // A normal MVCC table's snapshot is materialized into the StatementContext during analysis
            // (StatementContext.loadSnapshots, keyed by the table); a plugin SYSTEM table's is NOT —
            // the sys table is not an MvccTable and BindRelation short-circuits loadSnapshots for the
            // $-suffixed relation, so getSnapshotFromContext returns empty above. Resolve the sys-table
            // FOR TIME AS OF / @branch / @tag pin directly off the source table here so the sys handle
            // reads at the pin (legacy IcebergScanNode.createTableScan parity) instead of silently
            // reading latest. Returns empty for every other case, so the normal-table path is unchanged.
            snapshot = resolveSysTableSnapshotPin();
        }
        currentHandle = applyMvccSnapshotPin(metadata, connectorSession, currentHandle, snapshot);
    }

    /**
     * Threads a distributed {@code rewrite_data_files} group's per-group file scope onto {@code handle}
     * BEFORE {@code planScan}, so the group's INSERT-SELECT scans ONLY the data files that group bin-packed
     * (mirrors {@link #applyMvccSnapshotPin}). {@code rawDataFilePaths} are the RAW paths the connector's
     * {@code planRewrite} emitted; the connector's {@link ConnectorMetadata#applyRewriteFileScope} matches its
     * re-enumerated tasks against the SAME raw strings.
     *
     * <p>A {@code null}/empty path list is a no-op (returns {@code handle} unchanged — full-table scan): an
     * absent scope must read everything, and an EMPTY scope must NOT be threaded down (it would scope to
     * "match nothing"). Public static so the pin-vs-skip decision is unit-testable directly on a Mockito mock,
     * exactly like {@link #applyMvccSnapshotPin}.</p>
     */
    public static ConnectorTableHandle applyRewriteFileScopePin(ConnectorMetadata metadata,
            ConnectorSession session, ConnectorTableHandle handle, List<String> rawDataFilePaths) {
        if (rawDataFilePaths == null || rawDataFilePaths.isEmpty()) {
            return handle;
        }
        return metadata.applyRewriteFileScope(session, handle, new HashSet<>(rawDataFilePaths));
    }

    /**
     * Resolves the per-group rewrite file scope from the statement context and threads it onto
     * {@link #currentHandle} (mutates exactly like {@link #pinMvccSnapshot}). Called at every scan-side
     * handle-consumption point so the split path, the async batch path and the serialized-table path all scan
     * the scoped file set. NON-consuming read (the per-group {@code StatementContext} is single-use, so the
     * scope is the same at every site within the statement); a no-op for every non-rewrite scan, so it is
     * byte-identical for normal reads.
     */
    private void pinRewriteFileScope() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null || ctx.getStatementContext() == null) {
            return;
        }
        List<String> scope = ctx.getStatementContext().getRewriteSourceFilePaths();
        if (scope == null || scope.isEmpty()) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        currentHandle = applyRewriteFileScopePin(metadata, connectorSession, currentHandle, scope);
    }

    /**
     * Threads a Top-N lazy-materialization signal onto {@link #currentHandle} (mutates exactly like
     * {@link #pinRewriteFileScope}) when this scan carries the engine-wide synthesized row-id column
     * ({@code __DORIS_GLOBAL_ROWID_COL__}, injected by {@code LazyMaterializeTopN} for {@code ORDER BY
     * ... LIMIT}). Under lazy materialization BE reads the sort key first, then re-fetches the OTHER
     * (non-projected) columns of the surviving rows by row-id, so a connector whose scan metadata is
     * pruned to the requested columns must rebuild it over the full schema (legacy
     * {@code IcebergScanNode.createScanRangeLocations} {@code haveTopnLazyMatCol} parity). A no-op for
     * every non-lazy-mat scan and for every connector whose {@link ConnectorMetadata#applyTopnLazyMaterialization}
     * is the default (handle unchanged), so it is byte-identical for normal reads.
     */
    private void pinTopnLazyMaterialize() {
        if (!hasTopnLazyMaterializeSlot(desc.getSlots())) {
            return;
        }
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        currentHandle = metadata.applyTopnLazyMaterialization(connectorSession, currentHandle);
    }

    /**
     * Whether any scan slot is the engine-wide synthesized lazy-materialization row-id column
     * ({@code Column.GLOBAL_ROWID_COL} prefix). Mirrors legacy {@code IcebergScanNode.createScanRangeLocations}'
     * {@code haveTopnLazyMatCol} detection and the same prefix test already used by {@link #classifyColumn}.
     * Static + package-private so the pure detection is unit-testable directly (mirrors
     * {@link #applyMvccSnapshotPin} / {@link #applyRewriteFileScopePin}).
     */
    static boolean hasTopnLazyMaterializeSlot(List<SlotDescriptor> slots) {
        for (SlotDescriptor slot : slots) {
            Column col = slot.getColumn();
            if (col != null && col.getName().startsWith(Column.GLOBAL_ROWID_COL)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Resolves the time-travel pin for a {@link PluginDrivenSysExternalTable} query whose pin never
     * enters the {@link org.apache.doris.nereids.StatementContext} MVCC map (the sys table is not an
     * {@link MvccTable}; see {@link #pinMvccSnapshot}). Delegates to the SOURCE table's
     * {@link MvccTable#loadSnapshot} — the same resolution a normal-table read uses — so the connector's
     * point-in-time conversion ({@code FOR VERSION/TIME AS OF} {@link org.apache.doris.analysis.TableSnapshot},
     * {@code @branch}/{@code @tag} {@link org.apache.doris.analysis.TableScanParams}), not-found messages and
     * mutual-exclusion check are reused verbatim. The resolved snapshot is then threaded onto the sys handle
     * by {@link #applyMvccSnapshotPin}, which {@code IcebergConnectorMetadata.applySnapshot} preserves as a
     * pinned SYSTEM handle (the connector's {@code planSystemTableScan} applies it).
     *
     * <p>Returns empty (no pin) when the target is not a sys table, when there is no time-travel selector,
     * or — defensively — when the source is not MVCC-capable (the guard
     * {@link #checkSysTableScanConstraints} already rejects that case for the relevant connectors).
     *
     * <p>Package-private + overridable so the resolution is unit-testable on a Mockito mock without a live
     * connector (mirrors {@link #applyMvccSnapshotPin} / {@link #checkSysTableScanConstraints}).
     */
    Optional<MvccSnapshot> resolveSysTableSnapshotPin() throws UserException {
        if (!(getTargetTable() instanceof PluginDrivenSysExternalTable)) {
            return Optional.empty();
        }
        if (getQueryTableSnapshot() == null && getScanParams() == null) {
            return Optional.empty();
        }
        PluginDrivenExternalTable source = ((PluginDrivenSysExternalTable) getTargetTable()).getSourceTable();
        if (!(source instanceof MvccTable)) {
            return Optional.empty();
        }
        return Optional.of(((MvccTable) source).loadSnapshot(
                Optional.ofNullable(getQueryTableSnapshot()),
                Optional.ofNullable(getScanParams())));
    }

    /**
     * Fail-loud guard for plugin system-table scans: a {@link PluginDrivenSysExternalTable} must
     * reject {@code FOR TIME AS OF} (snapshot) and {@code @incr}/scan-params queries rather than
     * silently ignore them. Mirrors legacy {@code PaimonScanNode.getProcessedTable}, which throws the
     * same two messages when the target is a {@code PaimonSysExternalTable}. Runs before split
     * generation on BOTH planning entry points ({@link #getSplits}, {@link #startSplit}).
     *
     * <p>Scope: SYS-table only. Normal-plugin-table time-travel handling is B5/MVCC and is out of
     * scope here.
     *
     * <p>Package-private (not private) so the guard can be unit-tested directly on a Mockito mock
     * with the three accessors stubbed, without constructing a full {@link FileQueryScanNode}.
     */
    void checkSysTableScanConstraints() throws UserException {
        if (!(getTargetTable() instanceof PluginDrivenSysExternalTable)) {
            return;
        }
        boolean timeTravelSupported = sysTableSupportsTimeTravel();
        if (getScanParams() != null) {
            // A connector whose sys tables honor a pin (iceberg) still rejects @incr — incremental read
            // of a synthetic metadata table is undefined; a connector that does not (paimon, the default)
            // rejects EVERY scan-param. Branch/tag are allowed only for the former.
            if (!timeTravelSupported || getScanParams().incrementalRead()) {
                throw new UserException("Plugin system tables do not support scan params.");
            }
        }
        if (getQueryTableSnapshot() != null && !timeTravelSupported) {
            throw new UserException("Plugin system tables do not support time travel.");
        }
    }

    /**
     * Whether the connector's system tables honor a time-travel / branch-tag pin (iceberg) versus reject
     * it (paimon, the default). Package-private + overridable so {@link #checkSysTableScanConstraints}
     * stays unit-testable on a Mockito mock without a live connector (mirrors the guard's own visibility).
     */
    boolean sysTableSupportsTimeTravel() {
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider == null) {
            return false;
        }
        return onPluginClassLoader(scanProvider, scanProvider::supportsSystemTableTimeTravel);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        checkSysTableScanConstraints();
        // Attempt limit and projection pushdown via SPI protocol
        tryPushDownLimit();

        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider == null) {
            LOG.warn("Connector does not provide a scan plan provider, returning empty splits");
            return Collections.emptyList();
        }

        // Push the Nereids partition-pruning result down to the connector so the read session
        // covers only the surviving partitions. A pruned-to-zero set means no data to read,
        // mirroring legacy MaxComputeScanNode.getSplits()'s empty-selection short-circuit — UNLESS the
        // connector is predicate-driven (ignorePartitionPruneShortCircuit), in which case a prune-to-zero
        // maps to scan-all and planScan re-plans from the pushed predicate (paimon `col IS NULL` parity).
        boolean ignorePartitionPruneShortCircuit = onPluginClassLoader(
                scanProvider, scanProvider::ignorePartitionPruneShortCircuit);
        List<String> requiredPartitions = resolveRequiredPartitions(
                selectedPartitions, ignorePartitionPruneShortCircuit);
        // Surface the partition counts for EXPLAIN (partition=N/M) and SQL-block-rule enforcement,
        // mirroring legacy MaxComputeScanNode.getSplits():720-722. Set BEFORE the pruned-to-zero
        // short-circuit below so a 0-partition selection still reports partition=0/total (e.g. WHERE
        // part=<absent value>). Batch mode populates these in startSplit() instead. See
        // displayPartitionCounts for why the gate covers the no-predicate all-partitions case.
        long[] partitionCounts = displayPartitionCounts(selectedPartitions);
        if (partitionCounts != null) {
            this.selectedPartitionNum = partitionCounts[0];
            this.totalPartitionNum = partitionCounts[1];
        }
        if (requiredPartitions != null && requiredPartitions.isEmpty()) {
            return Collections.emptyList();
        }

        List<ConnectorColumnHandle> columns = buildColumnHandles();
        tryPushDownProjection(columns);
        Optional<ConnectorExpression> remainingFilter = buildRemainingFilter();
        // Pin the MVCC snapshot onto currentHandle AFTER projection/filter pushdown rebuilt it and
        // immediately before planScan consumes it, so the native split path reads at the pinned
        // snapshot. getSplits already declares UserException, so a getTargetTable() failure propagates.
        pinMvccSnapshot();
        // Scope the scan to a distributed rewrite group's files (no-op for every non-rewrite read).
        pinRewriteFileScope();

        // If buildRemainingFilter stripped non-pushable (CAST) conjuncts (filteredToOriginalIndex
        // != null), suppress source-side LIMIT pushdown: the connector now sees a filter that no
        // longer reflects those predicates and could apply a LIMIT (e.g. MaxCompute's row-offset
        // limit-split optimization, which fires on an empty/partition-only filter) over rows the
        // stripped predicate has NOT filtered. Since BE re-evaluates the stripped predicate only on
        // the rows the source returns, that would under-return. Legacy disabled limit-split whenever
        // a non-partition-equality (incl. CAST) predicate was present; this mirrors it.
        long sourceLimit = effectiveSourceLimit(limit, filteredToOriginalIndex != null);
        // Forward the no-grouping COUNT(*) signal to the connector (FIX-COUNT-PUSHDOWN). The op is set
        // on this node by the Nereids translator (PhysicalPlanTranslator) and shipped to BE via
        // FileScanNode.toThrift, but a connector that can serve a precomputed row count
        // (paimon DataSplit.mergedRowCount()) needs the signal here to emit it; otherwise BE
        // materializes the full post-merge row set just to count. Connectors that do not override the
        // count-pushdown overload ignore the flag (default delegates to the 6-arg planScan).
        boolean countPushdown = getPushDownAggNoGroupingOp() == TPushAggOp.COUNT;
        List<ConnectorScanRange> ranges = onPluginClassLoader(scanProvider, () -> scanProvider.planScan(
                connectorSession, currentHandle, columns, remainingFilter, sourceLimit,
                requiredPartitions, countPushdown));

        List<Split> splits = new ArrayList<>(ranges.size());
        for (ConnectorScanRange range : ranges) {
            splits.add(new PluginDrivenSplit(range));
        }
        // FIX-E (explain gap): accumulate the native/total scan-range counts (for the connector
        // EXPLAIN line paimonNativeReadSplits) and, under COUNT(*) pushdown, the precomputed merged row
        // count (for FileScanNode's "pushdown agg=COUNT (n)" line). Both come from generic
        // ConnectorScanRange getters (default false / -1), so non-paimon connectors are unaffected.
        this.nativeReadSplitNum = countNativeReadRanges(ranges);
        this.totalReadSplitNum = ranges.size();
        long pushDownRowCount = resolvePushDownRowCount(countPushdown, ranges);
        if (pushDownRowCount >= 0) {
            // Only set when a range actually carries a precomputed count (e.g. paimon's collapsed count
            // range). Deletion-vector tables emit no count range, so tableLevelRowCount stays -1 and the
            // line renders the (-1) sentinel — the correctness-critical no-precomputed-count case.
            setPushDownCount(pushDownRowCount);
        }
        return splits;
    }

    /**
     * Counts the scan ranges read by BE's native (ORC/Parquet) reader (vs JNI), via the generic
     * {@link ConnectorScanRange#isNativeReadRange()} (default false). Drives the EXPLAIN
     * {@code paimonNativeReadSplits=<native>/<total>} numerator. Pure static so the accounting is
     * unit-testable without driving a full {@code planScan}.
     */
    static int countNativeReadRanges(List<ConnectorScanRange> ranges) {
        int nativeCount = 0;
        for (ConnectorScanRange range : ranges) {
            if (range.isNativeReadRange()) {
                nativeCount++;
            }
        }
        return nativeCount;
    }

    /**
     * Resolves the pushed-down COUNT(*) row count to surface on the EXPLAIN
     * {@code pushdown agg=COUNT (n)} line: the first range carrying a precomputed count
     * ({@link ConnectorScanRange#getPushDownRowCount()} {@code >= 0}) when count pushdown is active,
     * else {@code -1}. The {@code -1} return is load-bearing (Rule 9): a deletion-vector table emits
     * NO count range, so the sentinel must survive and render as {@code (-1)} — BE then counts by
     * reading. Returns {@code -1} immediately when count pushdown is not active (a non-COUNT scan must
     * never pick up a stray precomputed count). Pure static so the sentinel survival is unit-testable.
     */
    static long resolvePushDownRowCount(boolean countPushdown, List<ConnectorScanRange> ranges) {
        if (!countPushdown) {
            return -1;
        }
        for (ConnectorScanRange range : ranges) {
            if (range.getPushDownRowCount() >= 0) {
                return range.getPushDownRowCount();
            }
        }
        return -1;
    }

    /**
     * Source-side LIMIT to pass to {@code planScan}: the real limit normally, but {@code -1}
     * (no source limit) when non-pushable conjuncts were stripped from the filter. A source LIMIT
     * applied before a stripped (BE-only) predicate would return too few rows (BE can only filter
     * the returned rows down, not recover rows the source never returned). Extracted as a pure
     * static so the correctness-critical decision is unit-testable without a {@link FileQueryScanNode}.
     */
    static long effectiveSourceLimit(long limit, boolean nonPushableConjunctsStripped) {
        return nonPushableConjunctsStripped ? -1L : limit;
    }

    /**
     * Enables batched / streaming split generation for large partitioned scans, mirroring legacy
     * {@code MaxComputeScanNode.isBatchMode()}. Three gates are evaluated generically from state the
     * node already holds (partition pruning + slots + the {@code num_partitions_in_batch_mode}
     * threshold); the connector-specific gate (legacy {@code odpsTable.getFileNum() > 0}) is
     * delegated to {@link ConnectorScanPlanProvider#supportsBatchScan}.
     */
    @Override
    public boolean isBatchMode() {
        if (isBatchModeCache == null) {
            isBatchModeCache = computeBatchMode();
        }
        return isBatchModeCache;
    }

    private boolean computeBatchMode() {
        // getScanPlanProvider() may be null for connectors without scan capability; mirror the
        // null-guard in getSplits() so isBatchMode (run on the dispatch + explain paths) never NPEs.
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider == null) {
            return false;
        }
        boolean hasSlots = !desc.getSlots().isEmpty();
        // Streaming (file-count) batch flavor (FIX-M3): the connector owns the whole decision (e.g.
        // Iceberg's matched-file count vs num_files_in_batch_mode); the engine only requires output slots
        // (a scan with no slots needs no file ranges). Checked before the partition-count flavor because a
        // connector implements at most one — Iceberg streams files, MaxCompute slices partitions.
        if (hasSlots) {
            boolean countPushdown = getPushDownAggNoGroupingOp() == TPushAggOp.COUNT;
            long estimate = onPluginClassLoader(scanProvider, () -> scanProvider.streamingSplitEstimate(
                    connectorSession, currentHandle, buildRemainingFilter(), countPushdown));
            if (estimate >= 0) {
                streamingSplitEstimate = estimate;
                streamingBatch = true;
                return true;
            }
        }
        // Partition-count batch flavor (legacy MaxCompute): its connector odpsTable.getFileNum()>0 check is
        // folded into supportsBatchScan; the partition threshold is evaluated generically.
        boolean supportsBatchScan = onPluginClassLoader(scanProvider,
                () -> scanProvider.supportsBatchScan(connectorSession, currentHandle));
        return shouldUseBatchMode(selectedPartitions, hasSlots,
                supportsBatchScan, sessionVariable.getNumPartitionsInBatchMode());
    }

    /**
     * Pure batch-mode gate, mirroring legacy {@code MaxComputeScanNode.isBatchMode()} (its connector
     * {@code odpsTable.getFileNum() > 0} check is folded into {@code supportsBatchScan}). Extracted
     * as a static helper so the four-input decision is unit-testable without constructing a
     * {@link FileQueryScanNode} (the async/wiring half is covered by live e2e — see DV-019).
     *
     * <ul>
     *   <li>not partitioned / not pruned ({@code selectedPartitions} null or {@code !isPruned}) &rarr; false;</li>
     *   <li>no required slots &rarr; false;</li>
     *   <li>connector does not support batch scan (incl. no scan provider) &rarr; false;</li>
     *   <li>otherwise batch iff {@code numPartitionsInBatchMode > 0} and the pruned partition count
     *       reaches that threshold.</li>
     * </ul>
     *
     * <p>The {@code !isPruned} check subsumes BOTH legacy gates ({@code getPartitionColumns().isEmpty()}
     * and the reference check {@code != NOT_PRUNED}): a non-partitioned external table always carries
     * {@code NOT_PRUNED} (which has {@code isPruned=false}), so collapsing them is not a dropped gate —
     * it is in fact marginally stronger than legacy's reference identity check.</p>
     */
    static boolean shouldUseBatchMode(SelectedPartitions selectedPartitions, boolean hasSlots,
            boolean supportsBatchScan, int numPartitionsInBatchMode) {
        if (selectedPartitions == null || !selectedPartitions.isPruned) {
            return false;
        }
        if (!hasSlots) {
            return false;
        }
        if (!supportsBatchScan) {
            return false;
        }
        return numPartitionsInBatchMode > 0
                && selectedPartitions.selectedPartitions.size() >= numPartitionsInBatchMode;
    }

    @Override
    public int numApproximateSplits() {
        if (streamingBatch) {
            // Streaming batch (FIX-M3): the connector's matched-file estimate. Legacy IcebergScanNode batch
            // returned ~partition count; the file estimate is a strictly better, always-non-negative BE
            // concurrency hint. Cap at Integer.MAX_VALUE for pathologically large tables.
            return streamingSplitEstimate > Integer.MAX_VALUE
                    ? Integer.MAX_VALUE : (int) streamingSplitEstimate;
        }
        // Number of pruned partitions; must be non-negative in batch mode (FileQueryScanNode rejects
        // negative). Under the isBatchMode gate this is >= num_partitions_in_batch_mode >= 1.
        return selectedPartitions == null ? -1 : selectedPartitions.selectedPartitions.size();
    }

    /**
     * Asynchronously generates splits in batches of {@code num_partitions_in_batch_mode} partitions,
     * streaming each batch into {@link #splitAssignment}. Mirrors legacy
     * {@code MaxComputeScanNode.startSplit}: one read session per partition batch (built by the
     * connector via {@link ConnectorScanPlanProvider#planScanForPartitionBatch}) on the shared
     * schedule executor, with the same completion/error protocol against {@code SplitAssignment}.
     *
     * <p>Batch mode deliberately does NOT push the limit (passes {@code -1}): legacy's batch path
     * ignores limit, and the LIMIT-split optimization stays on the non-batch {@link #getSplits}
     * path only (the two are mutually exclusive).</p>
     */
    @Override
    public void startSplit(int numBackends) {
        if (streamingBatch) {
            // File-count streaming flavor (FIX-M3): pump a connector-driven lazy source instead of
            // slicing partitions. Mutually exclusive with the partition-slicing path below.
            startStreamingSplit();
            return;
        }
        try {
            checkSysTableScanConstraints();
        } catch (UserException e) {
            // startSplit cannot throw checked exceptions; surface the fail-loud guard through the
            // SplitAssignment error channel (same protocol the async batch path below uses) so the
            // query fails rather than silently ignoring scan-params/time-travel on a sys table.
            splitAssignment.setException(e);
            return;
        }
        long[] partitionCounts = displayPartitionCounts(selectedPartitions);
        if (partitionCounts != null) {
            this.selectedPartitionNum = partitionCounts[0];
            this.totalPartitionNum = partitionCounts[1];
        }
        if (selectedPartitions.selectedPartitions.isEmpty()) {
            // Unreachable under the isBatchMode gate (size >= num_partitions_in_batch_mode >= 1);
            // kept for fidelity with legacy MaxComputeScanNode.startSplit's empty short-circuit.
            return;
        }

        // Mirror getSplits()'s projection + filter pushdown (but NOT the limit) before going async.
        // tryPushDownProjection mutates currentHandle, so capture the resolved handle afterwards.
        final List<ConnectorColumnHandle> columns = buildColumnHandles();
        tryPushDownProjection(columns);
        final Optional<ConnectorExpression> remainingFilter = buildRemainingFilter();
        // Pin the MVCC snapshot onto currentHandle before the resolved handle is captured below, so
        // the async batch path (planScanForPartitionBatch) reads at the pinned snapshot. startSplit
        // cannot throw checked exceptions, so surface a getTargetTable() failure through the
        // SplitAssignment error channel (same protocol as checkSysTableScanConstraints above).
        try {
            pinMvccSnapshot();
        } catch (UserException e) {
            splitAssignment.setException(e);
            return;
        }
        // Scope the scan to a distributed rewrite group's files (no-op for every non-rewrite read).
        pinRewriteFileScope();
        final ConnectorTableHandle handle = currentHandle;
        final ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        final List<String> allPartitions =
                new ArrayList<>(selectedPartitions.selectedPartitions.keySet());
        final int batchSize = sessionVariable.getNumPartitionsInBatchMode();

        Executor scheduleExecutor = Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor();
        AtomicReference<UserException> batchException = new AtomicReference<>(null);
        AtomicInteger numFinishedPartitions = new AtomicInteger(0);

        CompletableFuture.runAsync(() -> {
            for (int begin = 0; begin < allPartitions.size(); begin += batchSize) {
                int end = Math.min(begin + batchSize, allPartitions.size());
                if (batchException.get() != null || splitAssignment.isStop()) {
                    break;
                }
                List<String> batch = allPartitions.subList(begin, end);
                int curBatchSize = end - begin;
                try {
                    CompletableFuture.runAsync(() -> {
                        try {
                            List<ConnectorScanRange> ranges = onPluginClassLoader(scanProvider,
                                    () -> scanProvider.planScanForPartitionBatch(
                                            connectorSession, handle, columns, remainingFilter, -1L, batch));
                            List<Split> batchSplits = new ArrayList<>(ranges.size());
                            for (ConnectorScanRange range : ranges) {
                                batchSplits.add(new PluginDrivenSplit(range));
                            }
                            if (splitAssignment.needMoreSplit()) {
                                splitAssignment.addToQueue(batchSplits);
                            }
                        } catch (Exception e) {
                            batchException.set(new UserException(e.getMessage(), e));
                        } finally {
                            if (batchException.get() != null) {
                                splitAssignment.setException(batchException.get());
                            }
                            if (numFinishedPartitions.addAndGet(curBatchSize) == allPartitions.size()) {
                                splitAssignment.finishSchedule();
                            }
                        }
                    }, scheduleExecutor);
                } catch (Exception e) {
                    batchException.set(new UserException(e.getMessage(), e));
                }
                if (batchException.get() != null) {
                    splitAssignment.setException(batchException.get());
                }
            }
        }, scheduleExecutor);
    }

    /**
     * Streams splits from a connector-driven lazy {@link ConnectorSplitSource} into
     * {@link #splitAssignment} with backpressure, mirroring legacy {@code IcebergScanNode.doStartSplit}.
     * Used by the file-count streaming batch flavor (see {@link #computeBatchMode}); the partition-count
     * flavor stays on the {@link #startSplit} partition-slicing path. Deliberately does NOT push the limit
     * (passes {@code -1}): the LIMIT-split optimization stays on the non-batch {@link #getSplits} path only.
     */
    private void startStreamingSplit() {
        try {
            checkSysTableScanConstraints();
        } catch (UserException e) {
            // startSplit cannot throw checked exceptions; surface the fail-loud guard through the
            // SplitAssignment error channel so the query fails rather than silently ignoring the guard.
            splitAssignment.setException(e);
            return;
        }
        // Mirror getSplits()'s projection + filter pushdown (but NOT the limit) before going async.
        // tryPushDownProjection mutates currentHandle, so capture the resolved handle afterwards.
        final List<ConnectorColumnHandle> columns = buildColumnHandles();
        tryPushDownProjection(columns);
        final Optional<ConnectorExpression> remainingFilter = buildRemainingFilter();
        // Pin the MVCC snapshot + rewrite scope onto currentHandle before capturing the resolved handle,
        // so the lazy source plans at the PINNED snapshot (rows are always correct). The streamingSplitEstimate
        // gate ran pre-pin (current snapshot), so it only affects the approximate batch decision + concurrency
        // hint, never the rows. Narrow caveat: a time-travel query to a past snapshot much LARGER than current
        // (table since compacted/expired) may under-count -> pick the eager path -> lose streaming's OOM
        // protection for that scan. Acceptable (perf-only, rare); documented in the FIX-M3 design.
        try {
            pinMvccSnapshot();
        } catch (UserException e) {
            splitAssignment.setException(e);
            return;
        }
        pinRewriteFileScope();
        final ConnectorTableHandle handle = currentHandle;
        final ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        Executor scheduleExecutor = Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor();
        CompletableFuture.runAsync(() -> {
            ConnectorSplitSource source = null;
            try {
                source = onPluginClassLoader(scanProvider,
                        () -> scanProvider.streamSplits(connectorSession, handle, columns, remainingFilter, -1L));
                // Pull ranges with backpressure (needMoreSplit) and pump them one at a time, exactly like
                // legacy doStartSplit. The bounded SplitAssignment queue throttles the lazy source so FE
                // heap stays bounded for million-file scans.
                while (splitAssignment.needMoreSplit() && source.hasNext()) {
                    List<Split> one = new ArrayList<>(1);
                    one.add(new PluginDrivenSplit(source.next()));
                    splitAssignment.addToQueue(one);
                }
                splitAssignment.finishSchedule();
            } catch (Exception e) {
                splitAssignment.setException(new UserException(e.getMessage(), e));
            } finally {
                // Close in a finally that SWALLOWS close errors (NOT try-with-resources, whose close runs
                // before the catch): a close() failure must not fail a scan whose splits were already
                // enumerated + finishSchedule()-d (legacy doStartSplit swallowed close errors identically).
                if (source != null) {
                    try {
                        source.close();
                    } catch (Exception ce) {
                        LOG.warn("Failed to close streaming split source for {}", handle, ce);
                    }
                }
            }
        }, scheduleExecutor);
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (!(split instanceof PluginDrivenSplit)) {
            return;
        }
        PluginDrivenSplit pluginSplit = (PluginDrivenSplit) split;
        ConnectorScanRange scanRange = pluginSplit.getConnectorScanRange();

        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(scanRange.getTableFormatType());

        // Delegate format-specific Thrift construction to the connector SPI
        scanRange.populateRangeParams(tableFormatFileDesc, rangeDesc);

        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }


    @Override
    protected Optional<String> getSerializedTable() {
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider != null) {
            Map<String, String> props = getOrLoadScanNodeProperties();
            String serialized = onPluginClassLoader(scanProvider, () -> scanProvider.getSerializedTable(props));
            if (serialized != null) {
                return Optional.of(serialized);
            }
        }
        return Optional.empty();
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        // Delegate scan-level Thrift params to the connector SPI
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
        if (scanProvider != null) {
            Map<String, String> props = getOrLoadScanNodeProperties();
            onPluginClassLoader(scanProvider, () -> {
                scanProvider.populateScanLevelParams(params, props);
                return null;
            });
        }
        pruneConjunctsFromNodeProperties();

        // Push down limit to ES via terminate_after optimization.
        // When all predicates are pushed to ES (conjuncts empty) and limit fits in one batch,
        // ES can use terminate_after to stop scanning early instead of scrolling all results.
        if (limit > 0 && limit <= sessionVariable.batchSize && conjuncts.isEmpty()
                && params.isSetEsProperties()) {
            params.getEsProperties().put("limit", String.valueOf(limit));
        }
    }


    /**
     * Prunes pushed-down conjuncts using the structured result from
     * {@link ConnectorScanPlanProvider#getScanNodePropertiesResult()}.
     *
     * <p>Only conjuncts whose indices are in the not-pushed set are retained.
     * If the connector has no not-pushed tracking (empty set), all conjuncts
     * are considered pushed and cleared.</p>
     */
    private void pruneConjunctsFromNodeProperties() {
        if (conjuncts == null || conjuncts.isEmpty()) {
            return;
        }
        ScanNodePropertiesResult result = getOrLoadPropertiesResult();

        if (!result.hasConjunctTracking()) {
            // No conjunct tracking — do not prune (keep all conjuncts for safety)
            return;
        }

        // notPushedSet indices are relative to the filtered conjunct list
        // (after CAST expr removal). Map them back to original conjunct indices.
        Set<Integer> notPushedSet = result.getNotPushedConjunctIndices();
        Set<Integer> originalNotPushed = new HashSet<>();
        if (filteredToOriginalIndex != null) {
            for (int filteredIdx : notPushedSet) {
                if (filteredIdx < filteredToOriginalIndex.size()) {
                    originalNotPushed.add(filteredToOriginalIndex.get(filteredIdx));
                }
            }
        } else {
            // No CAST filtering was applied — indices map 1:1
            originalNotPushed.addAll(notPushedSet);
        }

        // Also keep any conjuncts that were filtered out (CAST expressions)
        // since those were never sent to the connector for pushdown
        if (filteredToOriginalIndex != null) {
            Set<Integer> sentToConnector = new HashSet<>(filteredToOriginalIndex);
            for (int i = 0; i < conjuncts.size(); i++) {
                if (!sentToConnector.contains(i)) {
                    originalNotPushed.add(i);
                }
            }
        }

        List<Expr> remaining = new ArrayList<>();
        for (int i = 0; i < conjuncts.size(); i++) {
            if (originalNotPushed.contains(i)) {
                remaining.add(conjuncts.get(i));
            }
        }
        conjuncts.clear();
        conjuncts.addAll(remaining);
    }

    /**
     * Lazily loads and caches the ScanNodePropertiesResult from the connector.
     * Both getOrLoadScanNodeProperties() and pruneConjunctsFromNodeProperties()
     * use this to avoid redundant computation.
     */
    private ScanNodePropertiesResult getOrLoadPropertiesResult() {
        if (cachedPropertiesResult == null) {
            ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider();
            if (scanProvider != null) {
                List<ConnectorColumnHandle> columns = buildColumnHandles();
                Optional<ConnectorExpression> filter = buildRemainingFilter();
                // Pin the MVCC snapshot onto currentHandle before getScanNodePropertiesResult
                // consumes it: this single cached result feeds the serialized-table (JNI) path,
                // scan-level params, explain, and file attributes, so the pin must land here or the
                // serialized-table path silently reads LATEST while the split path reads the pin.
                // This method is private and called from contexts that do not declare UserException
                // (e.g. getNodeExplainString), so a getTargetTable() failure is surfaced by wrapping
                // it in a RuntimeException (same unchecked error channel as create() above) rather
                // than dropped.
                try {
                    pinMvccSnapshot();
                } catch (UserException e) {
                    throw new RuntimeException("Failed to pin MVCC snapshot for plugin-driven scan", e);
                }
                // Scope the scan to a distributed rewrite group's files (no-op for every non-rewrite read).
                pinRewriteFileScope();
                // Signal Top-N lazy materialization so a connector with a column-pruned scan dictionary
                // rebuilds it over the full schema (no-op for every non-lazy-mat read / every connector
                // without a pruned dictionary).
                pinTopnLazyMaterialize();
                cachedPropertiesResult = onPluginClassLoader(scanProvider,
                        () -> scanProvider.getScanNodePropertiesResult(
                                connectorSession, currentHandle, columns, filter));
            }
            if (cachedPropertiesResult == null) {
                cachedPropertiesResult = new ScanNodePropertiesResult(Collections.emptyMap());
            }
        }
        return cachedPropertiesResult;
    }

    /**
     * Lazily loads scan node properties from the connector's scan plan provider.
     */
    private Map<String, String> getOrLoadScanNodeProperties() {
        if (scanNodeProperties == null) {
            scanNodeProperties = getOrLoadPropertiesResult().getProperties();
            if (scanNodeProperties == null) {
                scanNodeProperties = Collections.emptyMap();
            }
        }
        return scanNodeProperties;
    }

    /**
     * Maps a file format name string to the corresponding TFileFormatType.
     */
    private static TFileFormatType mapFileFormatType(String format) {
        switch (format.toLowerCase()) {
            case "parquet":
                return TFileFormatType.FORMAT_PARQUET;
            case "orc":
                return TFileFormatType.FORMAT_ORC;
            case "text":
            case "csv":
                return TFileFormatType.FORMAT_CSV_PLAIN;
            case "json":
                return TFileFormatType.FORMAT_JSON;
            case "avro":
                return TFileFormatType.FORMAT_AVRO;
            case "es_http":
                return TFileFormatType.FORMAT_ES_HTTP;
            default:
                return TFileFormatType.FORMAT_JNI;
        }
    }

    /**
     * Builds column handles from the tuple descriptor's slot descriptors.
     * These tell the connector which columns are needed for the query,
     * enabling optimized column selection (e.g., SELECT col1, col2 instead of SELECT *).
     */
    private List<ConnectorColumnHandle> buildColumnHandles() {
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        Map<String, ConnectorColumnHandle> allHandles =
                metadata.getColumnHandles(connectorSession, currentHandle);
        if (allHandles.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorColumnHandle> selected = new ArrayList<>();
        for (org.apache.doris.analysis.SlotDescriptor slot : desc.getSlots()) {
            if (slot.getColumn() != null) {
                ConnectorColumnHandle ch = allHandles.get(slot.getColumn().getName());
                if (ch != null) {
                    selected.add(ch);
                }
            }
        }
        return selected;
    }

    /**
     * Builds a {@link ConnectorFilterConstraint} from the current conjuncts.
     */
    private ConnectorFilterConstraint buildFilterConstraint(List<Expr> exprs) {
        ConnectorExpression combined = ExprToConnectorExpressionConverter.convertConjuncts(exprs);
        return new ConnectorFilterConstraint(combined, Collections.emptyMap());
    }

    /**
     * Builds the remaining filter expression from unconsumed conjuncts.
     * If no conjuncts remain, returns {@link Optional#empty()}.
     * Filters out CAST-containing predicates when the connector does not support CAST pushdown.
     */
    private Optional<ConnectorExpression> buildRemainingFilter() {
        if (conjuncts == null || conjuncts.isEmpty()) {
            filteredToOriginalIndex = null;
            return Optional.empty();
        }
        List<Expr> pushableConjuncts = conjuncts;
        ConnectorMetadata metadata = connector.getMetadata(connectorSession);
        if (!metadata.supportsCastPredicatePushdown(connectorSession)) {
            filteredToOriginalIndex = new ArrayList<>();
            pushableConjuncts = new ArrayList<>();
            for (int i = 0; i < conjuncts.size(); i++) {
                if (!containsCastExpr(conjuncts.get(i))) {
                    pushableConjuncts.add(conjuncts.get(i));
                    filteredToOriginalIndex.add(i);
                }
            }
            // If no filtering occurred, clear the mapping (1:1)
            if (filteredToOriginalIndex.size() == conjuncts.size()) {
                filteredToOriginalIndex = null;
            }
        } else {
            filteredToOriginalIndex = null;
        }
        if (pushableConjuncts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ExprToConnectorExpressionConverter.convertConjuncts(pushableConjuncts));
    }

    private static boolean containsCastExpr(Expr expr) {
        List<CastExpr> castExprs = new ArrayList<>();
        expr.collect(CastExpr.class, castExprs);
        return !castExprs.isEmpty();
    }
}
