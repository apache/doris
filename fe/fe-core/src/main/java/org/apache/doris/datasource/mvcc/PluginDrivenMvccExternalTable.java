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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.SupportBinarySearchFilteringPartitions;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartition;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTableFreshness;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenMetadata;
import org.apache.doris.datasource.plugin.PluginDrivenSchemaCacheValue;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Generic MVCC/MTMV-capable {@link PluginDrivenExternalTable} for connectors that expose a
 * point-in-time snapshot (e.g. Paimon, Iceberg, Hudi). All behavior is source-agnostic and driven
 * through the connector SPI; the data-source-specific rendering of partition names/dates happens in
 * the connector, so this class never parses raw values or imports any data-source library.
 *
 * <p>Selected by a capability factory and wired into the scan node in a later dispatch; until then
 * it has no production caller and is exercised only by direct-construction unit tests.</p>
 *
 * <p><b>MVCC/MTMV contract:</b> a connector that advertises this capability MUST supply a real
 * per-partition {@code lastModifiedMillis}. An {@link ConnectorPartitionInfo#UNKNOWN}(-1) is not a
 * valid timestamp: it pins {@code MTMVTimestampSnapshot(-1)} in {@link #getPartitionSnapshot}, which
 * degrades MTMV to conservative over-refresh (the partition never matches its prior snapshot).</p>
 */
public class PluginDrivenMvccExternalTable extends PluginDrivenExternalTable
        implements MTMVRelatedTableIf, MTMVBaseTableIf, MvccTable, SupportBinarySearchFilteringPartitions {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenMvccExternalTable.class);

    /** Matches an all-digits string (epoch millis / snapshot id). Parity with {@code PaimonUtil.isDigitalString}. */
    private static final Pattern DIGITAL_REGEX = Pattern.compile("\\d+");

    /** No-arg constructor for GSON deserialization. */
    public PluginDrivenMvccExternalTable() {
        super();
    }

    public PluginDrivenMvccExternalTable(long id, String name, String remoteName,
            ExternalCatalog catalog, ExternalDatabase db) {
        super(id, name, remoteName, catalog, db);
    }

    // ──────────────────── snapshot materialization ────────────────────

    /**
     * Returns the pinned snapshot if the caller supplied one (read the PIN, do NOT re-list), else
     * materializes the LATEST snapshot from the connector. The query-begin pin path goes through
     * {@link #loadSnapshot} which calls {@link #materializeLatest()} once; subsequent accessors
     * receive that pin here and never re-query the connector (single-pin invariant).
     */
    private PluginDrivenMvccSnapshot getOrMaterialize(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            return (PluginDrivenMvccSnapshot) snapshot.get();
        }
        return materializeLatest();
    }

    /**
     * Lists the partition set at LATEST and pins the connector snapshot. The per-partition build is
     * delegated to {@link #listLatestPartitions} (shared with the @incr path, which legacy also reads
     * at LATEST).
     */
    private PluginDrivenMvccSnapshot materializeLatest() {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            // The backing catalog was concurrently DROPPED: onClose() nulled the (transient) connector but
            // left objectCreated=true, so makeSureInitialized() does not re-create it and getConnector()
            // returns null. A stale metadata-table access (e.g. an mv_infos()/jobs() scan over all MTMVs ->
            // isMTMVSync -> a related table here) must DEGRADE to a valid empty pin so it yields an empty
            // partition view instead of NPE-ing and aborting the whole metadata query. Mirrors the
            // table-dropped (no-handle) branch below. On a HEALTHY catalog the connector is never null after
            // makeSureInitialized() (initLocalObjectsImpl throws if it cannot create one), so this guard only
            // covers the dropped-catalog race and cannot mask a genuine init failure.
            return new PluginDrivenMvccSnapshot(emptySnapshot(),
                    Collections.emptyMap(), Collections.emptyMap());
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);

        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            // No handle (e.g. table dropped): still return a valid empty pin so callers degrade to
            // UNPARTITIONED / snapshot id -1 instead of NPE-ing.
            return new PluginDrivenMvccSnapshot(emptySnapshot(),
                    Collections.emptyMap(), Collections.emptyMap());
        }
        ConnectorTableHandle handle = handleOpt.get();

        // An empty (no-snapshot) connector still pins: fall back to a snapshot id of -1.
        ConnectorMvccSnapshot connectorSnapshot =
                metadata.beginQuerySnapshot(session, handle).orElseGet(this::emptySnapshot);

        // Range-view path (e.g. iceberg): thread the query's pin onto the handle FIRST (applySnapshot), so
        // the partition/freshness enumeration stays consistent with the data-scan pin, then ask the connector
        // for its range-aware view. A connector without a range view returns empty -> fall through to the
        // legacy listPartitions/LIST/timestamp path below (byte-unchanged; the no-op applySnapshot for the
        // latest pin is side-effect-free for both paimon and iceberg).
        ConnectorTableHandle pinnedHandle = metadata.applySnapshot(session, handle, connectorSnapshot);
        Optional<ConnectorMvccPartitionView> viewOpt =
                metadata.getMvccPartitionView(session, pinnedHandle);
        if (viewOpt.isPresent()) {
            ConnectorMvccPartitionView view = viewOpt.get();
            // A non-RANGE (UNPARTITIONED) view is the connector's "not RANGE / not MTMV-range-eligible" verdict
            // (iceberg's range view only covers single time-transform specs). If the table nonetheless declares
            // partition columns (e.g. iceberg identity / bucket / truncate partitions), enumerate them via the
            // generic LIST path so partition pruning / selectedPartitionNum / SQL-block-rule enforcement see the
            // real partition set — WITHOUT which they wrongly report 0 partitions. The connector's UNPARTITIONED
            // verdict is preserved on the pin (partitionType), so getPartitionType() and isValidRelatedTable()
            // (MTMV eligibility) stay byte-unchanged; only getNameToPartitionItem() gains the LIST partitions.
            // A RANGE view (range items) and a genuinely unpartitioned table (no partition columns) are
            // unaffected. Freshness matches the legacy LIST path (timestamps / 0), harmless here because the
            // UNPARTITIONED verdict keeps this table out of the snapshot-id MTMV path.
            if (view.getStyle() != ConnectorMvccPartitionView.Style.RANGE && !getPartitionColumns().isEmpty()) {
                Map<String, PartitionItem> listItems = Maps.newHashMap();
                Map<String, Long> listLastModifiedMillis = Maps.newHashMap();
                listLatestPartitions(metadata, session, handle, listItems, listLastModifiedMillis);
                return new PluginDrivenMvccSnapshot(connectorSnapshot, listItems, listLastModifiedMillis,
                        null, PartitionType.UNPARTITIONED, false, 0L);
            }
            return buildFromRangeView(connectorSnapshot, view);
        }

        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
        Map<String, Long> nameToLastModifiedMillis = Maps.newHashMap();
        listLatestPartitions(metadata, session, handle, nameToPartitionItem, nameToLastModifiedMillis);
        return new PluginDrivenMvccSnapshot(connectorSnapshot, nameToPartitionItem,
                nameToLastModifiedMillis);
    }

    /**
     * Builds a pin from a connector-supplied {@link ConnectorMvccPartitionView} (range-view path). The
     * connector has already done ALL source-specific math (transform-to-range, partition-evolution overlap
     * merge, per-partition snapshot-id resolution); this turns the pre-rendered bounds into generic
     * {@link RangePartitionItem}s and stores the partition type / freshness kind / newest-update-time the
     * accessors then read. A partition that fails to build FAILS LOUD (parity with legacy
     * {@code IcebergUtils.loadPartitionInfo}, which does not swallow a bad partition — unlike the LIST path's
     * per-partition log-and-skip).
     */
    private PluginDrivenMvccSnapshot buildFromRangeView(ConnectorMvccSnapshot connectorSnapshot,
            ConnectorMvccPartitionView view) {
        PartitionType partitionType = view.getStyle() == ConnectorMvccPartitionView.Style.RANGE
                ? PartitionType.RANGE : PartitionType.UNPARTITIONED;
        boolean snapshotIdFreshness =
                view.getFreshness() == ConnectorMvccPartitionView.Freshness.SNAPSHOT_ID;
        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
        Map<String, Long> nameToFreshnessValue = Maps.newHashMap();
        if (view.getStyle() == ConnectorMvccPartitionView.Style.RANGE) {
            List<Column> partitionColumns = getPartitionColumns();
            for (ConnectorMvccPartition partition : view.getPartitions()) {
                try {
                    nameToPartitionItem.put(partition.getName(),
                            toRangePartitionItem(partition, partitionColumns));
                } catch (AnalysisException e) {
                    // Fail loud: a range partition that cannot build is a real metadata error, not a
                    // skippable row (legacy loadPartitionInfo lets getPartitionRange throw).
                    throw new RuntimeException("Failed to build range partition item for "
                            + partition.getName() + ", partitionColumns: " + partitionColumns, e);
                }
                nameToFreshnessValue.put(partition.getName(), partition.getFreshnessValue());
            }
        }
        return new PluginDrivenMvccSnapshot(connectorSnapshot, nameToPartitionItem, nameToFreshnessValue,
                null, partitionType, snapshotIdFreshness, view.getNewestUpdateTimeMillis());
    }

    /**
     * Assembles a {@link RangePartitionItem} from the connector's pre-rendered {@code [lower, upper)} bounds
     * and the table's partition column types. An EMPTY upper-bound tuple denotes the NULL-min partition: the
     * exclusive upper is the column-type/scale-aware {@code lowerKey.successor()} (only fe-core owns the Doris
     * {@code Column}/{@code PartitionKey}), matching the source's null-partition behavior (e.g. iceberg's
     * {@code nullLowKey.successor()}). Mirrors {@code IcebergUtils.getPartitionRange}'s key building.
     */
    private static RangePartitionItem toRangePartitionItem(ConnectorMvccPartition partition,
            List<Column> partitionColumns) throws AnalysisException {
        PartitionKey lowerKey = PartitionKey.createPartitionKey(
                toPartitionValues(partition.getLowerBound()), partitionColumns);
        PartitionKey upperKey = partition.getUpperBound().isEmpty()
                ? lowerKey.successor()
                : PartitionKey.createPartitionKey(toPartitionValues(partition.getUpperBound()), partitionColumns);
        return new RangePartitionItem(Range.closedOpen(lowerKey, upperKey));
    }

    /** Wraps each pre-rendered bound string into a (non-null) {@link PartitionValue}. */
    private static List<PartitionValue> toPartitionValues(List<String> bound) {
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(bound.size());
        for (String v : bound) {
            values.add(new PartitionValue(v));
        }
        return values;
    }

    /**
     * Lists the partition set at LATEST into the two supplied maps (rendered name -&gt; built
     * {@link PartitionItem} / -&gt; last-modified epoch millis). Mirrors legacy
     * {@code PaimonUtil.generatePartitionInfo}: per-partition build is wrapped in try/catch so a single
     * un-parseable name is logged and skipped (leaving the listed-name set larger than the built-item
     * set, which {@link PluginDrivenMvccSnapshot#isPartitionInvalid} then treats as UNPARTITIONED)
     * rather than failing the whole query.
     */
    private void listLatestPartitions(ConnectorMetadata metadata, ConnectorSession session,
            ConnectorTableHandle handle, Map<String, PartitionItem> nameToPartitionItem,
            Map<String, Long> nameToLastModifiedMillis) {
        List<Column> partitionColumns = getPartitionColumns();
        List<Type> types = partitionColumns.stream().map(Column::getType).collect(Collectors.toList());
        List<ConnectorPartitionInfo> parts = metadata.listPartitions(session, handle, Optional.empty());
        for (ConnectorPartitionInfo part : parts) {
            String partitionName = part.getPartitionName();
            nameToLastModifiedMillis.put(partitionName, part.getLastModifiedMillis());
            try {
                // The connector supplies the parsed values in name-segment order; building from them is
                // byte-parity with legacy. Two shapes are tolerated by skipping the partition rather than
                // failing the whole query (parity PaimonUtil.generatePartitionInfo):
                //   - a value that is un-representable in its column type;
                //   - a value/column count mismatch, which is LEGITIMATE under iceberg partition spec
                //     evolution: the column list comes from the CURRENT spec while each row's values come
                //     from the spec its data file was written under, so rows written before an
                //     ADD/DROP PARTITION FIELD carry fewer values (a table that began life unpartitioned
                //     carries none). Skipping leaves the built-item set short of the listed-name set,
                //     which isPartitionInvalid turns into UNPARTITIONED: the query still returns correct
                //     rows, it only loses partition pruning. Do NOT hoist this check out of the catch to
                //     "fail loud" — that was tried (cfb0958e607) and every real-world hit was a legitimate
                //     spec evolution, not a mis-wired connector, taking down 6 suites (CI 996541).
                nameToPartitionItem.put(partitionName,
                        toListPartitionItem(partitionName, types,
                                part.getOrderedPartitionValues(), part.getPartitionValueNullFlags()));
            } catch (Exception e) {
                LOG.warn("toListPartitionItem failed, partitionColumns: {}, partitionName: {}",
                        partitionColumns, partitionName, e);
            }
        }
    }

    private ConnectorMvccSnapshot emptySnapshot() {
        return ConnectorMvccSnapshot.builder().snapshotId(-1L).build();
    }

    /**
     * Builds a {@link ListPartitionItem} from a RENDERED partition name (e.g. {@code "dt=2024-01-01"}) and the
     * connector-supplied per-value SQL-NULL flags. Source-agnostic: the connector — not fe-core — decides which
     * values are genuine NULL.
     *
     * <p>Each parsed value {@code i} builds {@code new PartitionValue(value, nullFlags.get(i))}. A connector that
     * renders a genuine-NULL partition value (hive's {@code __HIVE_DEFAULT_PARTITION__}, paimon's
     * {@code partition.default-name}) supplies {@code true} for that position, so
     * {@link PartitionKey#createListPartitionKeyWithTypes} emits a typed {@code NullLiteral} instead of parsing
     * the sentinel string into the column type — which for a non-string column (INT/DATE/...) would throw and
     * silently drop the partition (table then mis-reported UNPARTITIONED, {@code partition=0/0}). The genuine-null
     * partition then prunes on {@code col IS NULL} and an MTMV refresh materializes its null rows. A connector
     * that supplies no flags ({@code nullFlags} empty) treats every value as non-null — unchanged behavior for
     * connectors that do not opt in. {@code fe-core} never string-compares a sentinel (iron rule): hive and paimon
     * render the identical {@code __HIVE_DEFAULT_PARTITION__} string with connector-specific NULL semantics, so
     * nullness must be connector-supplied.
     */
    private static ListPartitionItem toListPartitionItem(String partitionName, List<Type> types,
            List<String> connectorValues, List<Boolean> nullFlags) throws AnalysisException {
        // The connector supplies the already-parsed values in name-segment order (hive/paimon/iceberg/hudi).
        // There is no name-parsing fallback anymore. This size check is LOAD-BEARING, not defensive: it is
        // what turns a heterogeneous-arity partition (legitimate under iceberg partition spec evolution)
        // into the skip -> UNPARTITIONED degrade. The caller relies on it throwing INSIDE its try/catch.
        List<String> partitionValues = connectorValues;
        Preconditions.checkState(partitionValues.size() == types.size(), partitionName + " vs. " + types);
        // Fail loud: a connector that opts in MUST supply one flag per value; a short list would silently
        // default the tail to isNull=false and re-introduce the drop bug. Empty = not opted in = OK.
        Preconditions.checkState(nullFlags.isEmpty() || nullFlags.size() == types.size(),
                "nullFlags " + nullFlags + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (int i = 0; i < partitionValues.size(); i++) {
            boolean isNull = i < nullFlags.size() && nullFlags.get(i);
            values.add(new PartitionValue(partitionValues.get(i), isNull));
        }
        PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types, true);
        return new ListPartitionItem(Lists.newArrayList(key));
    }

    // ──────────────────── MvccTable ────────────────────

    @Override
    public MvccSnapshot loadSnapshot(Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        if (!tableSnapshot.isPresent() && !scanParams.isPresent()) {
            // B5a implicit query-begin (latest) pin.
            return materializeLatest();
        }
        // Mutual exclusion — parity with legacy PaimonScanNode.getProcessedTable:891-892.
        if (tableSnapshot.isPresent() && scanParams.isPresent()) {
            throw new RuntimeException("Can not specify scan params and table snapshot at same time.");
        }
        ConnectorTimeTravelSpec spec = toTimeTravelSpec(tableSnapshot, scanParams);

        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        String dbName = db != null ? db.getRemoteName() : "";
        String tableName = getRemoteName();
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            throw new RuntimeException("can not find table for time travel: " + dbName + "." + tableName);
        }
        ConnectorTableHandle handle = handleOpt.get();

        // The connector owns all provider-specific resolution (snapshot-id lookup, datetime parsing,
        // tag/branch resolution, incremental-window validation). It returns empty when the target is
        // not found; a DorisConnectorException (TZ-alias / incremental-validation) propagates as-is
        // (fail-loud — a degraded result would read wrong rows for a time-travel query).
        Optional<ConnectorMvccSnapshot> resolved = metadata.resolveTimeTravel(session, handle, spec);
        if (!resolved.isPresent()) {
            throw new RuntimeException(notFoundMessage(spec));
        }
        ConnectorMvccSnapshot connectorSnapshot = resolved.get();

        if (spec.getKind() == ConnectorTimeTravelSpec.Kind.INCREMENTAL) {
            // @incr is NOT a point-in-time snapshot pin. Legacy PaimonExternalTable.getPaimonSnapshotCacheValue
            // falls through (it is neither tag/branch nor FOR VERSION/TIME AS OF) to getLatestSnapshotCacheValue
            // — i.e. the LATEST partition view + LATEST schema — and applies the incremental-between window at
            // SCAN time. Mirror that: list the LATEST partitions and use the LATEST schema (pinnedSchema == null
            // so getSchemaCacheValue() falls back to latest), while carrying the incremental scan options on the
            // pin (connectorSnapshot.getProperties()); the scan node's applySnapshot threads them onto the handle.
            // Partitions are listed on the BASE handle at latest — the full latest set, identical to the
            // normal-read materializeLatest path — NOT a snapshot-pinned handle.
            Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
            Map<String, Long> nameToLastModifiedMillis = Maps.newHashMap();
            listLatestPartitions(metadata, session, handle, nameToPartitionItem, nameToLastModifiedMillis);
            return new PluginDrivenMvccSnapshot(connectorSnapshot, nameToPartitionItem,
                    nameToLastModifiedMillis, null);
        }

        // Schema-at-snapshot: thread the pin onto the handle FIRST (applySnapshot), so a BRANCH pin
        // yields a branch-aware handle whose schemaManager resolves the branch schema; then resolve
        // the schema AT the pinned schemaId. For non-branch specs applySnapshot only adds scan options
        // that getTableSchema ignores, so passing the pinned handle is harmless. (Apply-before-
        // getTableSchema is REQUIRED for branch — using the base handle would resolve the branch
        // schemaId against the base table's schemaManager = wrong schema.)
        ConnectorTableHandle pinnedHandle = metadata.applySnapshot(session, handle, connectorSnapshot);
        ConnectorTableSchema atSchema = metadata.getTableSchema(session, pinnedHandle, connectorSnapshot);
        PluginDrivenSchemaCacheValue pinnedSchema =
                toSchemaCacheValue(metadata, session, dbName, tableName, atSchema);

        // Explicit point-in-time time-travel (snapshot id / tag / timestamp / branch) does NOT list
        // partitions (EMPTY partition maps) — parity with legacy PaimonPartitionInfo.EMPTY. The empty
        // maps make isPartitionInvalid() == (0!=0) == false, so getPartitionColumns(snapshot) flows
        // through super -> the schema-aware getSchemaCacheValue() below -> the pinned schema's partition
        // columns. Partition pruning is deferred to the connector's predicate pushdown (the generic scan
        // node's resolveRequiredPartitions treats this empty-universe pin as scan-all).
        return new PluginDrivenMvccSnapshot(connectorSnapshot,
                Collections.emptyMap(), Collections.emptyMap(), pinnedSchema);
    }

    /**
     * Source-agnostic dispatch of the analyzer's {@code FOR VERSION/TIME AS OF} ({@link TableSnapshot})
     * or {@code @tag/@branch/@incr} ({@link TableScanParams}) into a {@link ConnectorTimeTravelSpec}.
     * A digital {@code FOR VERSION AS OF} is a snapshot id; a non-digital one is a source-resolved ref
     * ({@link ConnectorTimeTravelSpec.Kind#VERSION_REF}) — fe-core does NOT pre-decide tag-vs-branch
     * (the connector owns that: iceberg accepts a branch or a tag, paimon resolves it as a tag).
     */
    private ConnectorTimeTravelSpec toTimeTravelSpec(Optional<TableSnapshot> ts, Optional<TableScanParams> sp) {
        if (ts.isPresent()) {
            TableSnapshot snap = ts.get();
            String value = snap.getValue();
            if (snap.getType() == TableSnapshot.VersionType.TIME) {
                return ConnectorTimeTravelSpec.timestamp(value, isDigital(value));   // FOR TIME AS OF
            }
            // FOR VERSION AS OF: digital -> snapshot id, non-digital -> source-resolved ref (branch/tag).
            return isDigital(value)
                    ? ConnectorTimeTravelSpec.snapshotId(value)
                    : ConnectorTimeTravelSpec.versionRef(value);
        }
        TableScanParams params = sp.get();
        if (params.isTag()) {
            return ConnectorTimeTravelSpec.tag(extractBranchOrTagName(params));
        }
        if (params.isBranch()) {
            return ConnectorTimeTravelSpec.branch(extractBranchOrTagName(params));
        }
        if (params.incrementalRead()) {
            return ConnectorTimeTravelSpec.incremental(params.getMapParams());
        }
        throw new RuntimeException("unsupported scan params: " + params.getParamType());
    }

    /** Parity: {@code PaimonUtil.isDigitalString}. */
    private static boolean isDigital(String value) {
        return value != null && DIGITAL_REGEX.matcher(value).matches();
    }

    /** Parity: {@code PaimonUtil.extractBranchOrTagName} (uses {@code TableScanParams.PARAMS_NAME == "name"}). */
    private static String extractBranchOrTagName(TableScanParams params) {
        if (!params.getMapParams().isEmpty()) {
            if (!params.getMapParams().containsKey(TableScanParams.PARAMS_NAME)) {
                throw new IllegalArgumentException("must contain key 'name' in params");
            }
            return params.getMapParams().get(TableScanParams.PARAMS_NAME);
        }
        if (params.getListParams().isEmpty() || params.getListParams().get(0) == null) {
            throw new IllegalArgumentException("must contain a branch/tag name in params");
        }
        return params.getListParams().get(0);
    }

    /** Translates a {@code resolveTimeTravel}-returned empty into a kind-specific user error message. */
    private static String notFoundMessage(ConnectorTimeTravelSpec spec) {
        switch (spec.getKind()) {
            case SNAPSHOT_ID:
                return "can't find snapshot by id: " + spec.getStringValue();   // parity PaimonUtil:687
            case VERSION_REF:
                // Non-numeric FOR VERSION AS OF may name a branch OR a tag (iceberg resolves both), but the
                // source-agnostic wording must NOT claim a branch lookup a tag-only source never performed
                // (paimon: FOR VERSION AS OF == tag), and "no such tag" is never false. Empty fall-through to
                // TAG keeps the message byte-identical to legacy paimon. (iceberg legacy's more precise "tag
                // or branch" wording is a pre-existing cosmetic gap, out of this functional fix's scope.)
            case TAG:
                return "can't find snapshot by tag: " + spec.getStringValue();  // parity PaimonUtil:694
            case BRANCH:
                return "can't find branch: " + spec.getStringValue();           // parity PaimonUtil:707
            case TIMESTAMP:
                // Best-effort: the connector returns empty (it owns the parsed millis + earliest
                // snapshot, which fe-core cannot see), so this diverges from legacy's detailed
                // "...the earliest snapshot's timestamp is [...]" message in TEXT ONLY (same error
                // condition). Documented divergence.
                return "can't find snapshot earlier than or equal to time: " + spec.getStringValue();
            default:
                return "can't resolve time travel: " + spec;
        }
    }

    // ──────────────────── schema (snapshot-aware) ────────────────────

    /**
     * Returns the schema AS OF the snapshot resolved from the AMBIENT context, else the latest schema.
     *
     * <p>Version-BLIND: it asks the statement "what is pinned for this table?" without saying WHICH
     * reference is asking, so a statement pinning this table at two versions (e.g. a self-join of two
     * {@code @tag} references) cannot be disambiguated and degrades to LATEST here — a schema no reference
     * asked for. Correct only for statement-global callers (MTMV refresh, preload, the write sink) that
     * genuinely have no reference to speak of. <b>The plan path must call {@link #getSchemaCacheValue(
     * Optional)} with the reference's own pin instead.</b>
     */
    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        return schemaAt(MvccUtil.getSnapshotFromContext(this));
    }

    /**
     * Returns the schema AS OF {@code snapshot} — the pin resolved for ONE specific table reference — when
     * it carries a pinned schema (schema-at-snapshot under schema evolution), else the latest schema.
     * Parity with legacy {@code PaimonExternalTable.getSchemaCacheValue}, which returns the schema of the
     * context-pinned snapshot.
     *
     * <p>Deliberately NOT delegated to/from the no-arg override: an empty {@code snapshot} means "this
     * reference has no pin" (=> latest), whereas the no-arg form means "resolve from the ambient context".
     * They are siblings; collapsing them would strip ambient resolution from the statement-global callers.
     */
    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue(Optional<MvccSnapshot> snapshot) {
        return schemaAt(snapshot);
    }

    private Optional<SchemaCacheValue> schemaAt(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent() && snapshot.get() instanceof PluginDrivenMvccSnapshot) {
            SchemaCacheValue pinned = ((PluginDrivenMvccSnapshot) snapshot.get()).getPinnedSchema();
            if (pinned != null) {
                return Optional.of(pinned);     // time-travel: schema AS OF the pinned snapshot
            }
        }
        return getLatestSchemaCacheValue();     // latest (B5a pin has pinnedSchema==null, or no pin)
    }

    /**
     * The LATEST (non-pinned) schema. For a no-cache catalog (the connector's {@code schemaCacheTtlSecondOverride}
     * is {@code <= 0}, e.g. {@code meta.cache.paimon.table.ttl-second=0}) the schema is read FRESH via
     * {@link #initSchema()}, bypassing the generic name-keyed schema cache.
     *
     * <p>Why bypass rather than rely on the cache TTL: the SPI routes the latest schema through the generic
     * {@code DefaultExternalMetaCache} schema entry keyed by table NAME only (no schemaId, unlike master's
     * {@code PaimonSchemaCacheKey(nameMapping, schemaId)}), and that entry's TTL spec is frozen at first build
     * ({@code AbstractExternalMetaCache.initCatalog} computeIfAbsent), so a {@code ttl-second=0} cannot reliably
     * bust it after an external schema change. Reading fresh restores master's single-knob semantics
     * ({@code meta.cache.paimon.table.ttl-second=0} -> always-fresh schema) and is cheap at ttl=0 by definition;
     * {@code initSchema()} reloads via the connector's live {@code catalog.getTable} (master parity). The cached
     * catalog (override absent or {@code > 0}) keeps the cached path; {@code REFRESH TABLE} still busts it.
     */
    protected Optional<SchemaCacheValue> getLatestSchemaCacheValue() {
        Connector localConnector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        if (schemaCacheDisabled(localConnector)) {
            return initSchema();
        }
        return cachedSchemaCacheValue();
    }

    /**
     * The generic name-keyed schema cache read ({@code super.getSchemaCacheValue()}). Isolated as a seam so
     * {@link #getLatestSchemaCacheValue()} can bypass it for a no-cache catalog and so tests can stub it.
     */
    protected Optional<SchemaCacheValue> cachedSchemaCacheValue() {
        return super.getSchemaCacheValue();
    }

    /**
     * Whether the connector disables its schema cache (its {@code schemaCacheTtlSecondOverride()} is present
     * and {@code <= 0} — the no-cache catalog, {@code meta.cache.paimon.table.ttl-second=0}). Such a catalog
     * must serve a FRESH schema on every read, restoring master's single-knob semantics. A null/empty/positive
     * override keeps the cached path.
     */
    static boolean schemaCacheDisabled(Connector connector) {
        if (connector == null) {
            return false;
        }
        OptionalLong override = connector.schemaCacheTtlSecondOverride();
        return override != null && override.isPresent() && override.getAsLong() <= 0;
    }

    // ──────────────────── partition view (snapshot-aware) ────────────────────

    @Override
    public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> snapshot) {
        return getOrMaterialize(snapshot).getNameToPartitionItem();
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot) {
        return new HashMap<>(getNameToPartitionItems(snapshot));
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        PluginDrivenMvccSnapshot pin = getOrMaterialize(snapshot);
        if (pin.getPartitionType() != null) {
            // Range-view path: the connector already decided RANGE vs UNPARTITIONED (its eligibility gate).
            return pin.getPartitionType();
        }
        if (pin.isPartitionInvalid()) {
            return PartitionType.UNPARTITIONED;
        }
        return getPartitionColumns(snapshot).size() > 0 ? PartitionType.LIST : PartitionType.UNPARTITIONED;
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        PluginDrivenMvccSnapshot pin = getOrMaterialize(snapshot);
        if (pin.getPartitionType() != null) {
            // Range-view path: do NOT empty the columns here (parity master getIcebergPartitionColumns, which
            // always returns the spec columns); UNPARTITIONED is enforced via getPartitionType above.
            return super.getPartitionColumns(snapshot);
        }
        // Legacy empties the partition columns on an invalid partition set so the table is treated
        // as UNPARTITIONED everywhere downstream.
        return pin.isPartitionInvalid() ? Collections.emptyList() : super.getPartitionColumns(snapshot);
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
    }

    // ── SupportBinarySearchFilteringPartitions: lets NereidsSortedPartitionsCacheManager cache the
    //    pre-built SortedPartitionRanges across queries, keyed by the pinned connector snapshot. ──
    @Override
    public Map<?, PartitionItem> getOriginPartitions(CatalogRelation scan) {
        // The SAME frozen map the pin exposes — no re-list, so ranges stay consistent with
        // nameToPartitionItem (no #65659 TOCTOU).
        return getNameToPartitionItems(pinnedSnapshot(scan));
    }

    @Override
    public Object getPartitionMetaVersion(CatalogRelation scan) {
        // The version token MUST equal the EXACT partition content the ranges are built from
        // (getOriginPartitions reads the SAME pin), so Cache B can never disagree with the pin's own
        // nameToPartitionItem regardless of how/when a connector's listPartitions cache (Cache A)
        // refreshes: the cache rebuilds precisely when the partition set changes, never on a stale set.
        // A compact copy of just the name strings (NOT the live keySet view), so the cross-query cache
        // entry does not retain the whole pin's partition-item map.
        //
        // The former "<snapshotId>@<schemaId>" O(1) token was REMOVED: it is only sound where a
        // connector's listPartitions content is a pure function of the snapshot id, which is NOT true for
        // iceberg NON-RANGE tables (identity / bucket / truncate / multi-field partitioning). There the
        // pin's nameToPartitionItem is served by listPartitions (Cache A, keyed (-1,-1), enumerated at
        // currentSnapshot with its own TTL, ignoring the pin) while the snapshot-id token comes from a
        // DIFFERENT cache (IcebergLatestSnapshotCache) with its own TTL; the two expire independently, so
        // a snapshot-id token could serve SortedPartitionRanges STALER than the pin's own partitions ->
        // within-query disagreement -> silent under-inclusive pruning. Deriving the version from the
        // frozen name set makes it uniform across ALL engines -- the same rigorous scheme snapshot-less
        // hive already relied on -- at the cost of an O(partitions) set copy per lookup.
        return new HashSet<>(getOriginPartitions(scan).keySet());
    }

    @Override
    public long getPartitionMetaLoadTimeMillis(CatalogRelation scan) {
        // No insert-frequency signal for external tables; 0 = always allow sorting (the manager's
        // "skip sort if loaded within cacheSortedPartitionIntervalSecond" heuristic never trips).
        return 0L;
    }

    // getSortedPartitionRanges is now a pure pass-through to ExternalTable's cache-manager delegation
    // (5c17b748880's snapshotId==-1 short-circuit was removed: getPartitionMetaVersion above derives a
    // content-comparable version from the frozen partition name set for ALL engines, so Cache B is
    // correct uniformly -- snapshot-less hive and real-snapshot iceberg/paimon alike) -- no override
    // needed here.

    private Optional<MvccSnapshot> pinnedSnapshot(CatalogRelation scan) {
        if (scan instanceof LogicalFileScan) {
            LogicalFileScan fileScan = (LogicalFileScan) scan;
            return MvccUtil.getSnapshotFromContext(this, fileScan.getTableSnapshot(), fileScan.getScanParams());
        }
        return MvccUtil.getSnapshotFromContext(this);
    }

    // ──────────────────── MTMV snapshots ────────────────────

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot) throws AnalysisException {
        PluginDrivenMvccSnapshot pin = getOrMaterialize(snapshot);
        Long value = pin.getNameToLastModifiedMillis().get(partitionName);
        if (value == null) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        if (pin.isSnapshotIdFreshness()) {
            // Range-view path with snapshot-id freshness pins the per-partition snapshot id (parity master
            // IcebergExternalTable.getPartitionSnapshot -> MTMVSnapshotIdSnapshot). The connector pre-resolved
            // the `<= 0 -> table snapshot id` fallback, so a non-empty table never carries a non-positive value.
            return new MTMVSnapshotIdSnapshot(value);
        }
        if (pin.getConnectorSnapshot().isLastModifiedFreshness()) {
            // Last-modified connector (e.g. hive): the REAL per-partition modify time is not in the pin — the
            // connector's listPartitions is names-only on the scan hot path (pin value is the -1 sentinel) — so
            // fetch it on demand here, on the MTMV refresh path only (parity legacy HiveDlaTable
            // .getPartitionSnapshot -> MTMVTimestampSnapshot(partition.getLastModifiedTime())). The pin flag
            // gates this, so a snapshot-id connector (paimon/iceberg) NEVER reaches the probe. An empty return
            // means the partition vanished after the materialize-time existence check above (a refresh-time
            // race): raise the legacy "can not find partition" (parity checkPartitionExists).
            OptionalLong onDemand = queryPartitionFreshnessMillis(partitionName);
            if (!onDemand.isPresent()) {
                throw new AnalysisException("can not find partition: " + partitionName);
            }
            return new MTMVTimestampSnapshot(onDemand.getAsLong());
        }
        // Pin-timestamp connector (paimon): the pin's per-partition last-modified millis is authoritative
        // (byte-unchanged — no probe).
        return new MTMVTimestampSnapshot(value);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getTableSnapshot(snapshot);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        // Freshness-kind-aware (mirrors getPartitionSnapshot), gated by the pin fe-core already holds so a
        // snapshot-id connector pays ZERO extra metadata calls. A last-modified connector (e.g. hive, whose
        // whole-table change signal is transient_lastDdlTime / the max partition modify time, NOT a snapshot
        // id) flags the query-begin pin; fe-core then wraps getTableFreshness into an MTMVMaxTimestampSnapshot
        // (byte-parity with legacy HiveDlaTable.getTableSnapshot). WITHOUT this a plain-hive empty pin's
        // snapshot id is a constant -1, so an MV over a hive base table would never detect change. A snapshot-id
        // connector (paimon/iceberg) leaves the flag false and keeps the snapshot-id table snapshot, taking the
        // EXACT pre-change path (getOrMaterialize was already required for the id — no added round-trip).
        PluginDrivenMvccSnapshot pin = getOrMaterialize(snapshot);
        if (pin.getConnectorSnapshot().isLastModifiedFreshness()) {
            Optional<ConnectorTableFreshness> tableFreshness = queryTableFreshness();
            if (tableFreshness.isPresent()) {
                return new MTMVMaxTimestampSnapshot(tableFreshness.get().getName(),
                        tableFreshness.get().getTimestampMillis());
            }
        }
        return new MTMVSnapshotIdSnapshot(pin.getConnectorSnapshot().getSnapshotId());
    }

    // ──────────────────── on-demand freshness (last-modified connectors) ────────────────────

    /**
     * Whole-table freshness from a last-modified connector (present only for e.g. hive), or empty for a
     * snapshot-id connector / a dropped catalog/table. See {@link ConnectorMetadata#getTableFreshness}.
     */
    private Optional<ConnectorTableFreshness> queryTableFreshness() {
        Optional<FreshnessProbe> probe = resolveFreshnessProbe();
        if (!probe.isPresent()) {
            return Optional.empty();
        }
        FreshnessProbe p = probe.get();
        return p.metadata.getTableFreshness(p.session, p.handle);
    }

    /**
     * Per-partition last-modified millis from a last-modified connector (present only for e.g. hive), or
     * empty for a snapshot-id connector / a dropped catalog/table. See
     * {@link ConnectorMetadata#getPartitionFreshnessMillis}.
     */
    private OptionalLong queryPartitionFreshnessMillis(String partitionName) {
        Optional<FreshnessProbe> probe = resolveFreshnessProbe();
        if (!probe.isPresent()) {
            return OptionalLong.empty();
        }
        FreshnessProbe p = probe.get();
        return p.metadata.getPartitionFreshnessMillis(p.session, p.handle, partitionName);
    }

    /**
     * Resolves (session, metadata, handle) for an on-demand freshness probe, or empty when the catalog/table
     * is gone (degrade to the snapshot-id / pin path). Unlike {@link #materializeLatest} this lists NOTHING
     * and pins NOTHING — a last-modified connector fetches only the freshness it needs, off the scan hot path.
     */
    private Optional<FreshnessProbe> resolveFreshnessProbe() {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return Optional.empty();
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        return handleOpt.map(handle -> new FreshnessProbe(session, metadata, handle));
    }

    /** Bundles the (session, metadata, handle) an on-demand freshness probe needs. */
    private static final class FreshnessProbe {
        private final ConnectorSession session;
        private final ConnectorMetadata metadata;
        private final ConnectorTableHandle handle;

        private FreshnessProbe(ConnectorSession session, ConnectorMetadata metadata,
                ConnectorTableHandle handle) {
            this.session = session;
            this.metadata = metadata;
            this.handle = handle;
        }
    }

    @Override
    public long getNewestUpdateVersionOrTime() {
        // Dictionary-update path: always probe LATEST (bypass any context pin), mirroring legacy
        // which passes empty/empty to force a fresh listing.
        PluginDrivenMvccSnapshot pin = materializeLatest();
        // Last-modified connector (e.g. hive): the whole-table newest-change signal is a modify TIMESTAMP
        // (transient_lastDdlTime / the max partition modify time), NOT the partition listing — which for hive is
        // names-only, so every nameToLastModifiedMillis below is -1, gets filtered, and collapses to a CONSTANT 0.
        // That constant would make Dictionary.hasNewerSourceVersion compare equal forever, so a SQL dictionary / MV
        // over a hive base table would NEVER auto-refresh. Mirror getTableSnapshot: when the pin flags last-modified
        // freshness, return the connector's whole-table freshness millis (cache-backed getTableFreshness, so the
        // periodic dictionary poll stays cheap), else 0 (dropped catalog/table or a genuinely empty partition set —
        // parity legacy). A snapshot-id connector (paimon/iceberg) leaves the flag false and takes the EXACT
        // pre-change path below: a single boolean read, zero added metadata calls — byte- and cost-neutral.
        if (pin.getConnectorSnapshot().isLastModifiedFreshness()) {
            return queryTableFreshness().map(ConnectorTableFreshness::getTimestampMillis).orElse(0L);
        }
        if (pin.getPartitionType() != null) {
            // Range-view path: nameToLastModifiedMillis holds (non-monotonic) snapshot ids, NOT a usable
            // change marker. Use the connector-supplied newest-update-time, which IS monotonic (parity master
            // IcebergExternalTable.getNewestUpdateVersionOrTime = max(partition.lastUpdateTime)). The dictionary
            // requires a monotonically non-decreasing value or it throws (Dictionary.hasNewerSourceVersion).
            return pin.getNewestUpdateTimeMillis();
        }
        // Skip the UNKNOWN(-1) sentinel (a connector that did not collect a modified time): legacy
        // used Paimon's lastFileCreationTime() which has no -1 sentinel, so feeding -1 into max()
        // would let the sentinel win on an all-unknown table (returning -1 instead of the legacy 0).
        return pin.getNameToLastModifiedMillis().values().stream()
                .mapToLong(Long::longValue).filter(v -> v >= 0).max().orElse(0L);
    }

    @Override
    public boolean isValidRelatedTable() {
        // MTMV refresh safety gate (MTMVTask): a base table that evolved into an unsupported partitioning
        // (e.g. a single time transform changed to bucket, or gained a second partition column) must stop the
        // refresh loud (parity master IcebergExternalTable.isValidRelatedTable). The connector encodes its
        // eligibility verdict in the range view's style: a valid related table is RANGE, an ineligible one is
        // UNPARTITIONED. The legacy path (no range view) keeps the interface default (always valid; paimon does
        // not override isValidRelatedTable either). Probe LATEST, bypassing any context pin, like the gate does.
        // Cost note: unlike master's cached spec-only check, this materializes (a remote partition enumeration for
        // a valid table; an invalid one early-returns before the scan). Bounded — the only generic caller is
        // MTMVTask, once per refresh — so the extra listing is acceptable. A cheap specs-only eligibility SPI is a
        // possible future optimization if it ever matters.
        PluginDrivenMvccSnapshot pin = materializeLatest();
        if (pin.getPartitionType() != null) {
            return pin.getPartitionType() == PartitionType.RANGE;
        }
        return true;
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        // Returns true so MTMV creation over a snapshot connector is not blocked: a source may write a
        // physical "null" partition that does not match Doris' empty-partition semantics (e.g. paimon
        // writes both null and the literal 'null' to the 'null' partition). Returning false would
        // reject the MV; the connector owns the null-partition semantics, so we allow it. Parity with
        // legacy PaimonExternalTable.isPartitionColumnAllowNull.
        return true;
    }

    // ──────────────────── MTMVBaseTableIf ────────────────────

    @Override
    public void beforeMTMVRefresh(MTMV mtmv) {
        // No-op: parity with legacy PaimonExternalTable.beforeMTMVRefresh.
    }
}
