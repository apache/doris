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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
        Map<String, Long> nameToLastModifiedMillis = Maps.newHashMap();
        listLatestPartitions(metadata, session, handle, nameToPartitionItem, nameToLastModifiedMillis);
        return new PluginDrivenMvccSnapshot(connectorSnapshot, nameToPartitionItem,
                nameToLastModifiedMillis);
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
            // A value equal to the Doris-canonical null sentinel marks a genuine NULL partition.
            // Connectors normalize their own sentinel (e.g. paimon's partition.default-name) to this
            // in the rendered partition name, mirroring TablePartitionValues.toListPartitionItem so
            // that `col IS NULL` prunes to the null partition instead of pruning it away.
            values.add(new PartitionValue(partitionValue,
                    TablePartitionValues.HIVE_DEFAULT_PARTITION.equals(partitionValue)));
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

    /** Seam for the LATEST (non-pinned) schema; default delegates to the cached super. Overridable in tests. */
    protected Optional<SchemaCacheValue> getLatestSchemaCacheValue() {
        return super.getSchemaCacheValue();
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
        if (getOrMaterialize(snapshot).isPartitionInvalid()) {
            return PartitionType.UNPARTITIONED;
        }
        return getPartitionColumns(snapshot).size() > 0 ? PartitionType.LIST : PartitionType.UNPARTITIONED;
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        // Legacy empties the partition columns on an invalid partition set so the table is treated
        // as UNPARTITIONED everywhere downstream.
        return getOrMaterialize(snapshot).isPartitionInvalid() ? Collections.emptyList() : super.getPartitionColumns();
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
        Long ts = getOrMaterialize(snapshot).getNameToLastModifiedMillis().get(partitionName);
        if (ts == null) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return new MTMVTimestampSnapshot(ts);
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
        // Skip the UNKNOWN(-1) sentinel (a connector that did not collect a modified time): legacy
        // used Paimon's lastFileCreationTime() which has no -1 sentinel, so feeding -1 into max()
        // would let the sentinel win on an all-unknown table (returning -1 instead of the legacy 0).
        return materializeLatest().getNameToLastModifiedMillis().values().stream()
                .mapToLong(Long::longValue).filter(v -> v >= 0).max().orElse(0L);
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
