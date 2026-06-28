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
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.datasource.hive.HiveUtil;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
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
        implements MTMVRelatedTableIf, MTMVBaseTableIf, MvccTable {

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
        ConnectorMetadata metadata = connector.getMetadata(session);

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
            return buildFromRangeView(connectorSnapshot, viewOpt.get());
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
                // The connector already renders values (incl. dates) into getPartitionName(), so
                // building from the rendered name is byte-parity with legacy. Partition values may be
                // malformed; catch to avoid affecting the query (parity generatePartitionInfo).
                nameToPartitionItem.put(partitionName, toListPartitionItem(partitionName, types));
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
     * Builds a {@link ListPartitionItem} from a RENDERED partition name (e.g. {@code "dt=2024-01-01"}).
     * Copied verbatim from legacy {@code PaimonUtil.toListPartitionItem}; it is source-agnostic.
     */
    private static ListPartitionItem toListPartitionItem(String partitionName, List<Type> types)
            throws AnalysisException {
        // Partition name will be in format: nation=cn/city=beijing
        // parse it to get values "cn" and "beijing"
        List<String> partitionValues = HiveUtil.toPartitionValues(partitionName);
        Preconditions.checkState(partitionValues.size() == types.size(), partitionName + " vs. " + types);
        List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
        for (String partitionValue : partitionValues) {
            // Master parity (PaimonUtil.toListPartitionItem: `new PartitionValue(value, false)`): every
            // partition value — including a genuine-NULL value the connector rendered via its sentinel
            // (e.g. paimon's partition.default-name normalized to __HIVE_DEFAULT_PARTITION__) — builds a
            // NON-null (isNull=false) partition value. So the genuine-null partition is a plain
            // StringLiteral; an MTMV refresh emits `col IN ('<sentinel>')` which the scan's genuine SQL-NULL
            // rows never match (the null rows are dropped from the MV, like master), and its MTMV name is
            // derived from the sentinel (distinct from a literal 'NULL' partition — no p_NULL collision).
            // `col IS NULL` still returns the genuine-null rows: the paimon scan is predicate-driven and the
            // connector opts out of the FE prune-to-zero short-circuit (see Connector capability consulted by
            // PluginDrivenScanNode.resolveRequiredPartitions), so the SDK re-plans with the pushed predicate.
            values.add(new PartitionValue(partitionValue, false));
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
        ConnectorMetadata metadata = connector.getMetadata(session);
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
     * Mirrors the legacy {@code PaimonExternalTable.getPaimonSnapshotCacheValue} + {@code PaimonScanNode}
     * dispatch: a digital {@code FOR VERSION AS OF} is a snapshot id, a non-digital one is a tag name.
     */
    private ConnectorTimeTravelSpec toTimeTravelSpec(Optional<TableSnapshot> ts, Optional<TableScanParams> sp) {
        if (ts.isPresent()) {
            TableSnapshot snap = ts.get();
            String value = snap.getValue();
            if (snap.getType() == TableSnapshot.VersionType.TIME) {
                return ConnectorTimeTravelSpec.timestamp(value, isDigital(value));   // FOR TIME AS OF
            }
            // FOR VERSION AS OF: digital -> snapshot id, non-digital -> tag name.
            return isDigital(value)
                    ? ConnectorTimeTravelSpec.snapshotId(value)
                    : ConnectorTimeTravelSpec.tag(value);
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
     * Returns the schema AS OF the context-pinned snapshot when an explicit time-travel pin carries a
     * pinned schema (schema-at-snapshot under schema evolution), else the latest schema. Parity with
     * legacy {@code PaimonExternalTable.getSchemaCacheValue}, which returns the schema of the
     * context-pinned snapshot.
     */
    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        Optional<MvccSnapshot> ctx = MvccUtil.getSnapshotFromContext(this);
        if (ctx.isPresent() && ctx.get() instanceof PluginDrivenMvccSnapshot) {
            SchemaCacheValue pinned = ((PluginDrivenMvccSnapshot) ctx.get()).getPinnedSchema();
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
            return super.getPartitionColumns();
        }
        // Legacy empties the partition columns on an invalid partition set so the table is treated
        // as UNPARTITIONED everywhere downstream.
        return pin.isPartitionInvalid() ? Collections.emptyList() : super.getPartitionColumns();
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
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
        // Range-view path with snapshot-id freshness pins the per-partition snapshot id (parity master
        // IcebergExternalTable.getPartitionSnapshot -> MTMVSnapshotIdSnapshot). The connector pre-resolved the
        // `<= 0 -> table snapshot id` fallback, so a non-empty table never carries a non-positive value here.
        // The legacy path keeps the last-modified-millis timestamp (paimon parity).
        return pin.isSnapshotIdFreshness()
                ? new MTMVSnapshotIdSnapshot(value)
                : new MTMVTimestampSnapshot(value);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getTableSnapshot(snapshot);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        return new MTMVSnapshotIdSnapshot(getOrMaterialize(snapshot).getConnectorSnapshot().getSnapshotId());
    }

    @Override
    public long getNewestUpdateVersionOrTime() {
        // Dictionary-update path: always probe LATEST (bypass any context pin), mirroring legacy
        // which passes empty/empty to force a fresh listing.
        PluginDrivenMvccSnapshot pin = materializeLatest();
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
