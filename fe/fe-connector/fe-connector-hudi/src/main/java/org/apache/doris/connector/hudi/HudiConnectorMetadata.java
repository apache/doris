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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link ConnectorMetadata} implementation for Hudi catalogs.
 *
 * <p>Phase 1 provides read-only metadata operations:
 * <ul>
 *   <li>List databases and tables (via HMS)</li>
 *   <li>Get table schema from Hudi's Avro schema (via HoodieTableMetaClient)</li>
 *   <li>Detect Hudi table type (COW vs MOR)</li>
 * </ul>
 *
 * <p>Schema is derived from the Hudi MetaClient's latest Avro schema rather than
 * HMS columns, because Hudi tables with schema evolution may have a different
 * schema than what is registered in HMS.</p>
 */
public class HudiConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(HudiConnectorMetadata.class);

    // Catalog property gating the partition-name source (mirrors legacy HMSExternalTable.USE_HIVE_SYNC_PARTITION).
    private static final String USE_HIVE_SYNC_PARTITION = "use_hive_sync_partition";

    // Internal MVCC-snapshot property carrying the FOR TIME AS OF instant from resolveTimeTravel to applySnapshot
    // (FE-internal transport, paimon scan-options model). It is NEVER serialized to BE: fe-core only feeds the
    // snapshot to applySnapshot, which consumes this property and stamps HudiTableHandle.queryInstant. Not a
    // BE-facing key.
    private static final String HUDI_QUERY_INSTANT_PROPERTY = "hudi.query-instant";

    // Internal MVCC-snapshot properties carrying the resolved @incr (begin, end] window from resolveTimeTravel to
    // applySnapshot (same FE-internal transport as HUDI_QUERY_INSTANT_PROPERTY). NEVER serialized to BE: fe-core's
    // INCREMENTAL loadSnapshot branch lists the LATEST partitions + LATEST schema itself and only carries these on
    // the pin; applySnapshot consumes them to stamp HudiTableHandle.begin/endInstant. Not BE-facing keys.
    private static final String HUDI_INCREMENTAL_BEGIN_PROPERTY = "hudi.incremental-begin";
    private static final String HUDI_INCREMENTAL_END_PROPERTY = "hudi.incremental-end";

    // Prefix under which the RAW @incr option params ride the same FE-internal MVCC-snapshot transport, so the
    // ported IncrementalRelation family reads its glob / fallback / hollow-commit-policy options at planScan
    // exactly as legacy did (planScan has only the handle, not the original scan params). Each raw key becomes
    // "hudi.incr-opt.<key>" in the snapshot properties and is stripped back in applySnapshot into
    // HudiTableHandle.incrementalParams. The "hudi." prefix (never "hoodie.") keeps it namespace-consistent with
    // the other FE-internal carriers and guarantees no raw hoodie.* key masquerades as a BE-facing property (and
    // the transport is FE-only regardless: fe-core consumes the snapshot ONLY via applySnapshot, never
    // serializing its properties to BE).
    private static final String HUDI_INCR_OPT_PREFIX = "hudi.incr-opt.";

    // The @incr window param keys fe-core threads verbatim via getIncrementalParams() (byte-faithful to legacy
    // LogicalHudiScan.withScanParams, which reads "beginTime"/"endTime" from the scan params).
    private static final String INCR_BEGIN_TIME_KEY = "beginTime";
    private static final String INCR_END_TIME_KEY = "endTime";

    // Window sentinels (byte-faithful to legacy IncrementalRelation.EARLIEST_TIME / LATEST_TIME; "000" is the
    // earliest-resolved / empty-timeline bound). fe-core's legacy IncrementalRelation constants live in fe-core
    // and must not be imported across the plugin boundary, so they are re-declared here.
    private static final String INCR_EARLIEST_SENTINEL = "earliest";
    private static final String INCR_LATEST_SENTINEL = "latest";
    private static final String INCR_ZERO_INSTANT = "000";

    // The legacy hollow-commit policy key and the one non-default value that shifts window resolution. Under
    // USE_TRANSITION_TIME legacy resolves the default / "latest" end on the completion-time axis
    // (COWIncrementalRelation:94-96 / MORIncrementalRelation:88-90) and its file selection uses
    // findInstantsInRangeByCompletionTime; resolving the end here on the SAME axis keeps ONE handle-stamped end
    // correct for both file selection and the later synthetic _hoodie_commit_time row filter. Any other value
    // (or absent) -> the default requested-time axis = byte-identical to the pre-policy behaviour.
    private static final String INCR_HOLLOW_POLICY_KEY = "hoodie.read.timeline.holes.resolution.policy";
    private static final String INCR_STATE_TRANSITION_POLICY = "USE_TRANSITION_TIME";

    private final HmsClient hmsClient;
    private final Map<String, String> properties;
    // Runs the metaClient-touching partition/snapshot work under the plugin UGI doAs + TCCL pin (see R4).
    private final HudiMetaClientExecutor metaClientExecutor;

    public HudiConnectorMetadata(HmsClient hmsClient, Map<String, String> properties,
            HudiMetaClientExecutor metaClientExecutor) {
        this.hmsClient = hmsClient;
        this.properties = properties;
        this.metaClientExecutor = metaClientExecutor;
    }

    // ========== ConnectorSchemaOps ==========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return hmsClient.listDatabases();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        try {
            hmsClient.getDatabase(dbName);
            return true;
        } catch (HmsClientException e) {
            LOG.debug("Database '{}' not found: {}", dbName, e.getMessage());
            return false;
        }
    }

    // ========== ConnectorTableOps ==========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return hmsClient.listTables(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        if (!hmsClient.tableExists(dbName, tableName)) {
            return Optional.empty();
        }
        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        String location = tableInfo.getLocation();
        String hudiTableType = detectHudiTableType(tableInfo);

        // Extract partition key names
        List<String> partKeyNames = Collections.emptyList();
        if (tableInfo.getPartitionKeys() != null && !tableInfo.getPartitionKeys().isEmpty()) {
            partKeyNames = tableInfo.getPartitionKeys().stream()
                    .map(ConnectorColumn::getName)
                    .collect(Collectors.toList());
        }

        return Optional.of(new HudiTableHandle.Builder(dbName, tableName, location, hudiTableType)
                .inputFormat(tableInfo.getInputFormat())
                .serdeLib(tableInfo.getSerializationLib())
                .partitionKeyNames(partKeyNames)
                .tableParameters(tableInfo.getParameters())
                .build());
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        ConnectorTableSchema schema = getTableSchema(session, handle);
        List<String> partKeyNames = hudiHandle.getPartitionKeyNames();

        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>();
        for (ConnectorColumn col : schema.getColumns()) {
            boolean isPartKey = partKeyNames != null
                    && partKeyNames.contains(col.getName());
            handles.put(col.getName(),
                    new HudiColumnHandle(col.getName(),
                            col.getType().getTypeName(), isPartKey));
        }
        return handles;
    }

    @Override
    public Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorFilterConstraint constraint) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        List<String> partKeyNames = hudiHandle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            return Optional.empty();
        }

        // Extract equality/IN predicates on partition columns from the expression.
        // No partition predicate -> leave the handle untouched so resolvePartitions
        // falls back to Hudi's own metadata listing (HoodieTableMetadata.getAllPartitionPaths).
        Map<String, List<String>> partitionPredicates = extractPartitionPredicates(
                constraint.getExpression(), partKeyNames);
        if (partitionPredicates.isEmpty()) {
            return Optional.empty();
        }

        // List candidate partition names from HMS (e.g. "year=2024/month=01"). These
        // relative paths double as partition identifiers consumed by HudiScanPlanProvider.
        // Keep maxParts=-1 (unlimited): no silent partition truncation.
        List<String> allPartNames = hmsClient.listPartitionNames(
                hudiHandle.getDbName(), hudiHandle.getTableName(), -1);
        if (allPartNames == null || allPartNames.isEmpty()) {
            return Optional.empty();
        }

        List<String> matchedPartNames = prunePartitionNames(
                allPartNames, partKeyNames, partitionPredicates);
        if (matchedPartNames.size() == allPartNames.size()) {
            // No pruning effect
            return Optional.empty();
        }

        LOG.info("Partition pruning: {}.{} all={} pruned={}",
                hudiHandle.getDbName(), hudiHandle.getTableName(),
                allPartNames.size(), matchedPartNames.size());

        // Build updated handle carrying only the matched partition paths for scan planning.
        HudiTableHandle updatedHandle = hudiHandle.toBuilder()
                .prunedPartitionPaths(matchedPartNames)
                .build();

        return Optional.of(new FilterApplicationResult<>(updatedHandle, constraint.getExpression(), false));
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        String basePath = hudiHandle.getBasePath();

        List<ConnectorColumn> columns;
        if (basePath != null && !basePath.isEmpty()) {
            columns = getSchemaFromMetaClient(basePath);
        } else {
            columns = getSchemaFromHms(hudiHandle.getDbName(), hudiHandle.getTableName());
        }

        Map<String, String> tableProperties = Collections.singletonMap(
                "hudi.table.type", hudiHandle.getHudiTableType());
        return new ConnectorTableSchema(
                hudiHandle.getTableName(), columns, "HUDI", tableProperties);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // ========== ConnectorMvccOps (MTMV freshness) ==========

    /**
     * Pins the LATEST completed instant as the query-begin MVCC snapshot (snapshot-id freshness, paimon model).
     * The generic model then serves {@code MTMVSnapshotIdSnapshot(instant)} for the table and
     * {@code MTMVTimestampSnapshot(instant)} per partition, so a hudi materialized view auto-refreshes on a new
     * base commit and stays stable otherwise. This is an INTENTIONAL improvement over legacy {@code HudiDlaTable}
     * (which pinned a constant {@code 0L} and never detected change), NOT a byte-parity port.
     *
     * <p>{@code lastModifiedFreshness} is deliberately LEFT FALSE — that flag routes a last-modified connector
     * (hive) to the on-demand {@code getTableFreshness}/{@code getPartitionFreshnessMillis} probes; a snapshot-id
     * connector like hudi keeps the instant pin and pays zero extra metadata calls. {@code schemaId} is left
     * default ({@code -1}): explicit time travel is a later step.
     */
    @Override
    public Optional<ConnectorMvccSnapshot> beginQuerySnapshot(
            ConnectorSession session, ConnectorTableHandle handle) {
        return buildBeginQuerySnapshot(latestInstant((HudiTableHandle) handle));
    }

    /** Builds the query-begin snapshot from a pinned instant. Static for offline unit testing. */
    static Optional<ConnectorMvccSnapshot> buildBeginQuerySnapshot(long instant) {
        return Optional.of(ConnectorMvccSnapshot.builder().snapshotId(instant).build());
    }

    /**
     * Resolves an explicit {@code FOR TIME AS OF} / {@code FOR VERSION AS OF} into a pinned snapshot, owning all
     * hudi-specific parsing (byte-faithful to legacy {@code HudiScanNode}). The generic seam
     * ({@code PluginDrivenMvccExternalTable.loadSnapshot}) turns an empty result into a "not found" error and
     * lets a thrown exception propagate verbatim.
     *
     * <ul>
     *   <li>{@code TIMESTAMP} ({@code FOR TIME AS OF}) &mdash; strip {@code [-: ]} from the value and pin it as a
     *       completed-timeline instant read BEFORE-OR-ON at scan time (legacy {@code HudiScanNode.java:211}). NO
     *       epoch-millis conversion, NO session time zone (unlike paimon), NO timeline-existence validation:
     *       legacy validates nothing and never errors on a well-formed {@code FOR TIME AS OF} (a too-early /
     *       future instant simply reads empty / latest via {@code getLatest*BeforeOrOn}). So ALWAYS return a
     *       non-empty pin &mdash; never empty, never throw for a not-found timestamp. {@code spec.isDigital()} is
     *       ignored (legacy strips regardless). The instant rides {@link #HUDI_QUERY_INSTANT_PROPERTY};
     *       {@link #applySnapshot} stamps it onto the handle.</li>
     *   <li>{@code SNAPSHOT_ID} / {@code VERSION_REF} ({@code FOR VERSION AS OF}, numeric / named) &mdash; hudi
     *       rejects both with the byte-for-byte legacy message ({@code HudiScanNode.java:209}). It is THROWN (not
     *       {@code Optional.empty()}) so the exact wording reaches the user; an empty return would surface
     *       fe-core's wrong-domain "can't find snapshot" text.</li>
     *   <li>{@code INCREMENTAL} ({@code @incr(...)}) &mdash; see {@link #resolveIncremental}: resolves the
     *       {@code (begin, end]} window and returns a NON-EMPTY property-only pin. NEVER empty for a valid
     *       window (the generic {@code loadSnapshot} fail-loud has no INCREMENTAL arm, so empty would surface a
     *       wrong-domain "can't resolve time travel" message).</li>
     *   <li>Other kinds ({@code TAG}/{@code BRANCH} &mdash; hudi has none) &rarr; {@code Optional.empty()} = the
     *       SPI default (no worse than not overriding).</li>
     * </ul>
     */
    @Override
    public Optional<ConnectorMvccSnapshot> resolveTimeTravel(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorTimeTravelSpec spec) {
        switch (spec.getKind()) {
            case TIMESTAMP:
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .property(HUDI_QUERY_INSTANT_PROPERTY, spec.getStringValue().replaceAll("[-: ]", ""))
                        .build());
            case SNAPSHOT_ID:
            case VERSION_REF:
                throw new DorisConnectorException(
                        "Hudi does not support `FOR VERSION AS OF`, please use `FOR TIME AS OF`");
            case INCREMENTAL:
                return Optional.of(resolveIncremental((HudiTableHandle) handle, spec.getIncrementalParams()));
            default:
                return Optional.empty();
        }
    }

    /**
     * Resolves an {@code @incr(...)} incremental read into a NON-EMPTY property-only pin carrying the resolved
     * {@code (begin, end]} completed-timeline window. Consolidates legacy's per-relation window resolution
     * (spread byte-identically across {@code COWIncrementalRelation}/{@code MORIncrementalRelation} constructors)
     * into ONE connector locus, so the resolved bounds are on the handle for both file selection (a later step)
     * and the synthetic {@code _hoodie_commit_time} filter. Runs the single metaClient touch (latest completed
     * instant) under {@link HudiMetaClientExecutor#execute} (TCCL pin + plugin UGI {@code doAs}).
     *
     * <ul>
     *   <li>Empty completed timeline &rarr; the {@code (000, 000]} window &mdash; legacy {@code withScanParams}
     *       short-circuits to {@code EmptyIncrementalRelation} <em>before</em> the begin-required check, so a
     *       missing {@code beginTime} is NOT an error here; the window selects nothing.</li>
     *   <li>{@code beginTime} is required (legacy fail-loud, byte-for-byte message); {@code "earliest"} &rarr;
     *       {@code "000"}.</li>
     *   <li>{@code endTime} defaults to the latest completed instant; {@code "latest"} &rarr; the latest completed
     *       instant. The sentinel test is on the RESOLVED end value (legacy COW form,
     *       {@code COWIncrementalRelation:98}); the single locus inherently avoids the dead-code MOR bug
     *       ({@code MORIncrementalRelation:92}, which tested {@code latestTime} and so never fired). The
     *       "latest completed instant" is taken on the COMPLETION-time axis under the {@code USE_TRANSITION_TIME}
     *       hollow-commit policy (legacy parity) so the ONE resolved end matches the ported relation's
     *       completion-time file selection AND the later synthetic {@code _hoodie_commit_time} row filter — the
     *       connector never lets the file set and the row filter diverge on the window. An explicitly-supplied
     *       {@code endTime} is used verbatim on either axis (the user supplies an axis-appropriate value).</li>
     * </ul>
     *
     * <p>The pin is property-only: {@code snapshotId}/{@code schemaId} are inert because fe-core's INCREMENTAL
     * {@code loadSnapshot} branch lists the LATEST partitions + LATEST schema itself and reads only these window
     * properties. COW/MOR/RO-as-RT file selection is deferred to {@code planScan}; {@link #applySnapshot} stamps
     * the window onto the handle.</p>
     */
    private ConnectorMvccSnapshot resolveIncremental(HudiTableHandle handle, Map<String, String> params) {
        // Resolve the latest completed instant on the completion-time axis when the hollow-commit policy is
        // USE_TRANSITION_TIME (legacy parity, see INCR_HOLLOW_POLICY_KEY); the default axis is requested-time.
        boolean useCompletionTime = INCR_STATE_TRANSITION_POLICY.equals(params.get(INCR_HOLLOW_POLICY_KEY));
        Optional<String> latestTime = metaClientExecutor.execute(() ->
                HudiScanPlanProvider.latestCompletedInstantTime(
                        HudiScanPlanProvider.buildMetaClient(buildHadoopConf(), handle.getBasePath()),
                        useCompletionTime));
        String begin;
        String end;
        if (!latestTime.isPresent()) {
            // Empty completed timeline: legacy short-circuits to EmptyIncrementalRelation (begin = end = "000")
            // WITHOUT the begin-required check.
            begin = INCR_ZERO_INSTANT;
            end = INCR_ZERO_INSTANT;
        } else {
            begin = params.get(INCR_BEGIN_TIME_KEY);
            if (begin == null) {
                throw new DorisConnectorException("Specify the begin instant time to pull from using "
                        + "option hoodie.datasource.read.begin.instanttime");
            }
            if (INCR_EARLIEST_SENTINEL.equals(begin)) {
                begin = INCR_ZERO_INSTANT;
            }
            end = params.getOrDefault(INCR_END_TIME_KEY, latestTime.get());
            if (INCR_LATEST_SENTINEL.equals(end)) {
                end = latestTime.get();
            }
        }
        ConnectorMvccSnapshot.Builder builder = ConnectorMvccSnapshot.builder()
                .property(HUDI_INCREMENTAL_BEGIN_PROPERTY, begin)
                .property(HUDI_INCREMENTAL_END_PROPERTY, end);
        // Carry the raw @incr option params forward so planScan can feed them to the ported relations (glob /
        // fallback / hollow-commit policy). Skip null values (Builder.property NPEs on null; the begin/end keys
        // above are their own carriers and are not re-copied here — they use the "hudi.incremental-*" namespace,
        // which does not match HUDI_INCR_OPT_PREFIX).
        params.forEach((k, v) -> {
            if (v != null) {
                builder.property(HUDI_INCR_OPT_PREFIX + k, v);
            }
        });
        return builder.build();
    }

    /**
     * Threads a resolved pin onto the handle BEFORE planScan, reading the FE-internal carrier properties set by
     * {@link #resolveTimeTravel} and stamping via {@code toBuilder()}, which PRESERVES any
     * {@code prunedPartitionPaths} applyFilter set earlier (applyFilter runs before applySnapshot at scan time,
     * so a rebuild-from-scratch would drop the pruning). Two mutually exclusive carriers:
     *
     * <ul>
     *   <li>{@link #HUDI_QUERY_INSTANT_PROPERTY} ({@code FOR TIME AS OF}) &rarr; stamp {@code queryInstant}.</li>
     *   <li>{@link #HUDI_INCREMENTAL_BEGIN_PROPERTY}/{@link #HUDI_INCREMENTAL_END_PROPERTY} ({@code @incr})
     *       &rarr; stamp {@code begin/endInstant}.</li>
     * </ul>
     *
     * <p>For the query-begin latest pin ({@code beginQuerySnapshot} carries only a {@code snapshotId}, NO
     * property) &mdash; and for a null snapshot &mdash; the handle is returned UNCHANGED, so a plain read stays
     * byte-identical to today (planScan falls back to {@code timeline.lastInstant()}). Mirrors paimon's
     * empty-properties / invalid-pin no-op.</p>
     */
    @Override
    public ConnectorTableHandle applySnapshot(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        if (snapshot == null) {
            return handle;
        }
        Map<String, String> properties = snapshot.getProperties();
        String queryInstant = properties.get(HUDI_QUERY_INSTANT_PROPERTY);
        if (queryInstant != null) {
            return ((HudiTableHandle) handle).toBuilder().queryInstant(queryInstant).build();
        }
        String beginInstant = properties.get(HUDI_INCREMENTAL_BEGIN_PROPERTY);
        if (beginInstant != null) {
            // Reconstruct the raw @incr option params from their prefixed carriers (the begin/end keys use a
            // different "hudi.incremental-*" namespace, so they are not collected here).
            Map<String, String> incrementalParams = new HashMap<>();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (entry.getKey().startsWith(HUDI_INCR_OPT_PREFIX)) {
                    incrementalParams.put(
                            entry.getKey().substring(HUDI_INCR_OPT_PREFIX.length()), entry.getValue());
                }
            }
            return ((HudiTableHandle) handle).toBuilder()
                    .beginInstant(beginInstant)
                    .endInstant(properties.get(HUDI_INCREMENTAL_END_PROPERTY))
                    .incrementalParams(incrementalParams)
                    .build();
        }
        return handle;
    }

    // ========== ConnectorTableOps (partitions) ==========

    /**
     * Lists all partitions with metadata. {@code filter} is intentionally ignored (paimon / maxcompute parity):
     * the generic model lists the full universe and prunes FE-side.
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        return collectPartitions((HudiTableHandle) handle);
    }

    @Override
    public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
        List<ConnectorPartitionInfo> partitions = collectPartitions((HudiTableHandle) handle);
        List<String> names = new ArrayList<>(partitions.size());
        for (ConnectorPartitionInfo partition : partitions) {
            names.add(partition.getPartitionName());
        }
        return names;
    }

    @Override
    public List<List<String>> listPartitionValues(ConnectorSession session,
            ConnectorTableHandle handle, List<String> partitionColumns) {
        List<ConnectorPartitionInfo> partitions = collectPartitions((HudiTableHandle) handle);
        List<List<String>> result = new ArrayList<>(partitions.size());
        for (ConnectorPartitionInfo partition : partitions) {
            Map<String, String> rawValues = partition.getPartitionValues();
            // Preserve the requested partitionColumns order (feeds the partition_values() TVF, whose inner-list
            // order must match the input), mirroring PaimonConnectorMetadata.listPartitionValues.
            List<String> values = new ArrayList<>(partitionColumns.size());
            for (String column : partitionColumns) {
                values.add(rawValues.get(column));
            }
            result.add(values);
        }
        return result;
    }

    /**
     * Shared partition collector backing {@link #listPartitions}, {@link #listPartitionNames} and
     * {@link #listPartitionValues}. Lists the raw partition identifiers from the
     * {@code use_hive_sync_partition}-aware source (mirroring legacy
     * {@code HudiExternalMetaCache.loadPartitionNames}), then renders one {@link ConnectorPartitionInfo} per
     * partition. Unpartitioned &rarr; {@code emptyList()} (legacy never lists partitions for an unpartitioned
     * table). Explicit time-travel (non-latest) partition listing is a later step.
     */
    private List<ConnectorPartitionInfo> collectPartitions(HudiTableHandle handle) {
        List<String> partKeyNames = handle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            return Collections.emptyList();
        }
        if (useHiveSyncPartition()) {
            // hive-sync tables register their partitions in HMS: list the names from there (already authed via
            // hmsClient, no metaClient), like legacy. The instant still comes from the timeline. If HMS has none
            // (a hive-sync table not yet synced), fall back to the hudi metadata listing (legacy parity).
            List<String> hmsNames = hmsClient.listPartitionNames(
                    handle.getDbName(), handle.getTableName(), -1);
            if (hmsNames != null && !hmsNames.isEmpty()) {
                return buildPartitionInfos(hmsNames, partKeyNames, latestInstant(handle));
            }
            LOG.warn("hive-sync hudi table {}.{} has no HMS partitions; "
                    + "falling back to hudi metadata partition listing",
                    handle.getDbName(), handle.getTableName());
        }
        // Non-hive-sync (or hive-sync HMS-empty fallback): the instant and the partition paths both come from
        // the metaClient, built ONCE under the plugin auth + TCCL pin. Byte-consistent with the scan's unpruned
        // partition source (resolvePartitions -> listAllPartitionPaths), which is the R2 prune-to-zero guard.
        Map.Entry<Long, List<String>> listing = metaClientExecutor.execute(() -> {
            HoodieTableMetaClient metaClient =
                    HudiScanPlanProvider.buildMetaClient(buildHadoopConf(), handle.getBasePath());
            return new AbstractMap.SimpleImmutableEntry<>(
                    HudiScanPlanProvider.latestCompletedInstant(metaClient),
                    HudiScanPlanProvider.listAllPartitionPaths(metaClient));
        });
        return buildPartitionInfos(listing.getValue(), partKeyNames, listing.getKey());
    }

    /**
     * Renders one {@link ConnectorPartitionInfo} per raw partition path. {@code partitionName} = hive-style
     * (for the fe-core re-parse), {@code partitionValues} = the unescaped value map (for the TVF),
     * {@code lastModifiedMillis} = the pinned instant (a stable, monotonic freshness marker feeding
     * {@code MTMVTimestampSnapshot}; row/size/file counts stay {@code UNKNOWN}). Static + package-private for
     * offline unit testing.
     */
    static List<ConnectorPartitionInfo> buildPartitionInfos(
            List<String> rawPaths, List<String> partKeyNames, long instant) {
        List<ConnectorPartitionInfo> result = new ArrayList<>(rawPaths.size());
        for (String rawPath : rawPaths) {
            // Parse the unescaped values ONCE; render the hive-style name from the SAME values so the name and
            // the values map agree by construction (the name re-parses back to these values in fe-core).
            Map<String, String> values = HudiScanPlanProvider.parsePartitionValues(rawPath, partKeyNames);
            String name = HudiScanPlanProvider.renderHiveStylePartitionName(partKeyNames, values);
            result.add(new ConnectorPartitionInfo(name, values, Collections.emptyMap(),
                    ConnectorPartitionInfo.UNKNOWN, ConnectorPartitionInfo.UNKNOWN,
                    instant, ConnectorPartitionInfo.UNKNOWN));
        }
        return result;
    }

    /** Pins the latest completed instant, building the metaClient under the plugin auth + TCCL pin. */
    private long latestInstant(HudiTableHandle handle) {
        return metaClientExecutor.execute(() ->
                HudiScanPlanProvider.latestCompletedInstant(
                        HudiScanPlanProvider.buildMetaClient(buildHadoopConf(), handle.getBasePath())));
    }

    private boolean useHiveSyncPartition() {
        return Boolean.parseBoolean(properties.getOrDefault(USE_HIVE_SYNC_PARTITION, "false"));
    }

    /**
     * Builds the BE table descriptor for a hudi table: a {@code TTableType.HIVE_TABLE} carrying a
     * {@link THiveTable}, a direct port of legacy hudi (which rode {@code HMSExternalTable.toThrift} /
     * {@code HudiScanNode extends HiveScanNode} = HIVE_TABLE). Without this override the SPI default returns
     * {@code null} and fe-core ({@code PluginDrivenExternalTable.toThrift}) falls back to a generic
     * {@code SCHEMA_TABLE} descriptor, so BE builds a SchemaTableDescriptor instead of a HiveTableDescriptor.
     * Mirrors {@code HiveConnectorMetadata.buildTableDescriptor}; the SPI signature carries no handle, so this
     * single override serves base and system tables alike.
     */
    @Override
    public TTableDescriptor buildTableDescriptor(ConnectorSession session,
            long tableId, String tableName, String dbName, String remoteName, int numCols, long catalogId) {
        THiveTable tHiveTable = new THiveTable(dbName, tableName, new HashMap<>());
        TTableDescriptor desc = new TTableDescriptor(
                tableId, TTableType.HIVE_TABLE, numCols, 0, tableName, dbName);
        desc.setHiveTable(tHiveTable);
        return desc;
    }

    // ========== Internal helpers ==========

    /**
     * Read schema from HoodieTableMetaClient's latest Avro schema.
     * This is the authoritative schema for Hudi tables.
     */
    private List<ConnectorColumn> getSchemaFromMetaClient(String basePath) {
        try {
            Configuration conf = buildHadoopConf();
            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                    .setConf(new org.apache.hudi.storage.hadoop.HadoopStorageConfiguration(conf))
                    .setBasePath(basePath)
                    .build();
            TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
            Schema avroSchema = schemaResolver.getTableAvroSchema();
            return avroSchemaToColumns(avroSchema);
        } catch (Exception e) {
            LOG.warn("Failed to get schema from Hudi MetaClient for path '{}': {}",
                    basePath, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Fallback: read schema from HMS if MetaClient fails.
     */
    private List<ConnectorColumn> getSchemaFromHms(String dbName, String tableName) {
        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        List<ConnectorColumn> columns = new ArrayList<>();
        if (tableInfo.getColumns() != null) {
            columns.addAll(tableInfo.getColumns());
        }
        if (tableInfo.getPartitionKeys() != null) {
            columns.addAll(tableInfo.getPartitionKeys());
        }
        return columns;
    }

    /**
     * Convert Avro schema fields to ConnectorColumn list.
     *
     * <p>Package-private and static so it can be unit-tested directly with a
     * hand-built Avro schema (no live HoodieTableMetaClient needed).</p>
     */
    static List<ConnectorColumn> avroSchemaToColumns(Schema avroSchema) {
        List<Schema.Field> fields = avroSchema.getFields();
        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            boolean nullable = isNullable(field.schema());
            Schema fieldSchema = unwrapNullable(field.schema());
            ConnectorType connectorType = HudiTypeMapping.fromAvroSchema(fieldSchema);
            String comment = field.doc() != null ? field.doc() : "";
            // Lower-case the top-level column name to mirror legacy
            // HMSExternalTable.initHudiSchema (name().toLowerCase(Locale.ROOT)).
            // Nested struct field names are left as-is here and in HudiTypeMapping,
            // matching legacy (which lowercases only the top-level column name).
            String columnName = field.name().toLowerCase(Locale.ROOT);
            columns.add(new ConnectorColumn(columnName, connectorType, comment, nullable, null));
        }
        return columns;
    }

    private static boolean isNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .anyMatch(s -> s.getType() == Schema.Type.NULL);
        }
        return false;
    }

    private static Schema unwrapNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> nonNull = new ArrayList<>();
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
                    nonNull.add(s);
                }
            }
            if (nonNull.size() == 1) {
                return nonNull.get(0);
            }
        }
        return schema;
    }

    /**
     * Detect whether this is a COW (Copy on Write) or MOR (Merge on Read) Hudi table.
     */
    private String detectHudiTableType(HmsTableInfo tableInfo) {
        String inputFormat = tableInfo.getInputFormat();
        if (inputFormat != null) {
            if (inputFormat.contains("HoodieParquetRealtimeInputFormat")
                    || inputFormat.contains("realtime")) {
                return "MERGE_ON_READ";
            }
            if (inputFormat.contains("HoodieParquetInputFormat")
                    || inputFormat.contains("hoodie")) {
                return "COPY_ON_WRITE";
            }
        }
        Map<String, String> params = tableInfo.getParameters();
        if (params != null) {
            String sparkProvider = params.get("spark.sql.sources.provider");
            if ("hudi".equalsIgnoreCase(sparkProvider)) {
                return "COPY_ON_WRITE";
            }
        }
        return "UNKNOWN";
    }

    private Configuration buildHadoopConf() {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("hadoop.") || key.startsWith("fs.")
                    || key.startsWith("dfs.") || key.startsWith("hive.")) {
                conf.set(key, entry.getValue());
            }
        }
        return conf;
    }

    // ========== Partition pruning helpers ==========
    // Mirrors HiveConnectorMetadata's EQ/IN partition pruning. Duplicated rather than
    // shared because fe-connector-hudi depends on fe-connector-hms, not fe-connector-hive;
    // consolidate during the Hive (P7) migration. See P3-T05 design.

    /**
     * Extracts equality predicates on partition columns from the expression tree.
     * Supports: col = 'value', col IN ('v1', 'v2', ...), AND combinations.
     */
    private Map<String, List<String>> extractPartitionPredicates(
            ConnectorExpression expr, List<String> partKeyNames) {
        Set<String> partKeySet = partKeyNames.stream().collect(Collectors.toSet());
        Map<String, List<String>> result = new HashMap<>();
        extractPredicatesRecursive(expr, partKeySet, result);
        return result;
    }

    private void extractPredicatesRecursive(ConnectorExpression expr,
            Set<String> partKeySet, Map<String, List<String>> result) {
        if (expr instanceof ConnectorAnd) {
            for (ConnectorExpression child : ((ConnectorAnd) expr).getConjuncts()) {
                extractPredicatesRecursive(child, partKeySet, result);
            }
        } else if (expr instanceof ConnectorComparison) {
            ConnectorComparison cmp = (ConnectorComparison) expr;
            if (cmp.getOperator() == ConnectorComparison.Operator.EQ) {
                String colName = extractColumnName(cmp.getLeft());
                String value = extractLiteralValue(cmp.getRight());
                if (colName != null && value != null && partKeySet.contains(colName)) {
                    result.computeIfAbsent(colName, k -> new ArrayList<>()).add(value);
                }
            }
        } else if (expr instanceof ConnectorIn) {
            ConnectorIn inExpr = (ConnectorIn) expr;
            if (!inExpr.isNegated()) {
                String colName = extractColumnName(inExpr.getValue());
                if (colName != null && partKeySet.contains(colName)) {
                    List<String> values = new ArrayList<>();
                    for (ConnectorExpression item : inExpr.getInList()) {
                        String val = extractLiteralValue(item);
                        if (val != null) {
                            values.add(val);
                        }
                    }
                    if (!values.isEmpty()) {
                        result.computeIfAbsent(colName, k -> new ArrayList<>()).addAll(values);
                    }
                }
            }
        }
    }

    private String extractColumnName(ConnectorExpression expr) {
        if (expr instanceof org.apache.doris.connector.api.pushdown.ConnectorColumnRef) {
            return ((org.apache.doris.connector.api.pushdown.ConnectorColumnRef) expr).getColumnName();
        }
        return null;
    }

    private String extractLiteralValue(ConnectorExpression expr) {
        if (expr instanceof ConnectorLiteral) {
            Object val = ((ConnectorLiteral) expr).getValue();
            return val != null ? String.valueOf(val) : null;
        }
        return null;
    }

    /**
     * Prunes partition names based on extracted equality predicates.
     * Partition names follow the Hive convention: key1=val1/key2=val2
     */
    private List<String> prunePartitionNames(List<String> allPartNames,
            List<String> partKeyNames, Map<String, List<String>> predicates) {
        List<String> matched = new ArrayList<>();
        for (String partName : allPartNames) {
            Map<String, String> partValues = parsePartitionName(partName, partKeyNames);
            if (matchesPredicates(partValues, predicates)) {
                matched.add(partName);
            }
        }
        return matched;
    }

    private Map<String, String> parsePartitionName(String partName,
            List<String> partKeyNames) {
        Map<String, String> values = new HashMap<>();
        String[] parts = partName.split("/");
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                values.put(part.substring(0, eq), part.substring(eq + 1));
            }
        }
        return values;
    }

    private boolean matchesPredicates(Map<String, String> partValues,
            Map<String, List<String>> predicates) {
        for (Map.Entry<String, List<String>> entry : predicates.entrySet()) {
            String colName = entry.getKey();
            List<String> allowedValues = entry.getValue();
            String actualValue = partValues.get(colName);
            if (actualValue == null || !allowedValues.contains(actualValue)) {
                return false;
            }
        }
        return true;
    }
}
