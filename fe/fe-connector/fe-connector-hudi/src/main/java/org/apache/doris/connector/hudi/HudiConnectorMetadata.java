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
import org.apache.doris.connector.api.handle.ConnectorTransaction;
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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
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

    // Hive-canonical partition text for a DATETIME/TIMESTAMP literal: space separator, full seconds. See
    // hiveDateTimeString / extractLiteralValue (H2: String.valueOf(LocalDateTime) would yield ISO "…T…" and drop
    // zero seconds, never matching the stored Hive partition value).
    private static final DateTimeFormatter HIVE_DATETIME_SECONDS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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

    // The Hudi per-record commit-time meta column the synthetic incremental row filter references
    // (HoodieRecord.COMMIT_TIME_METADATA_FIELD; lower-cased in the connector schema). Byte-faithful to legacy
    // LogicalHudiScan.generateIncrementalExpression, which matched the scan-output slot by this exact name.
    private static final String HUDI_COMMIT_TIME_COLUMN = "_hoodie_commit_time";

    private final HmsClient hmsClient;
    private final Map<String, String> properties;
    // Runs the metaClient-touching partition/snapshot work under the plugin UGI doAs + TCCL pin (see R4).
    private final HudiMetaClientExecutor metaClientExecutor;
    // Canonical fs.s3a.*/hadoop.* storage config translated from the catalog's fe-filesystem StorageProperties,
    // overlaid into the FE-side metaClient's Hadoop conf so its S3AFileSystem has object-store credentials (the
    // raw s3.access_key/… aliases are not Hadoop keys). Empty in same-loader unit tests (which override
    // getSchemaFromMetaClient or never touch S3). See HudiScanPlanProvider#storageHadoopConfig.
    private final Map<String, String> storageHadoopConfig;

    public HudiConnectorMetadata(HmsClient hmsClient, Map<String, String> properties,
            HudiMetaClientExecutor metaClientExecutor) {
        this(hmsClient, properties, metaClientExecutor, Collections.emptyMap());
    }

    public HudiConnectorMetadata(HmsClient hmsClient, Map<String, String> properties,
            HudiMetaClientExecutor metaClientExecutor, Map<String, String> storageHadoopConfig) {
        this.hmsClient = hmsClient;
        this.properties = properties;
        this.metaClientExecutor = metaClientExecutor;
        this.storageHadoopConfig = storageHadoopConfig;
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
            // Thread the Hudi InternalSchema field id (set by getSchemaFromMetaClient) onto the handle so
            // schema-evolution reads match old files BY FIELD ID (HD-C4b). UNSET (-1) when unresolved -> the
            // scan-level dict is gated OFF for the whole scan (per-file BY_NAME), not per-column.
            handles.put(col.getName(),
                    new HudiColumnHandle(col.getName(),
                            col.getType().getTypeName(), isPartKey, col.getUniqueId()));
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

        // H3: candidate partition paths MUST be the SAME shape the scan feeds fsView (Hudi RELATIVE STORAGE
        // paths), and use_hive_sync_partition-aware (mirroring collectPartitions). The old code fed HMS hive-style
        // names ("year=2024/month=01") unconditionally; for a non-hive-style table (Hudi default) the physical
        // layout is positional ("2024/01"), so fsView (keyed by relative storage paths) matched nothing -> 0
        // splits for any filtered query. Keep maxParts=-1 (unlimited): no silent partition truncation.
        boolean hiveSync = useHiveSyncPartition();
        List<String> allPartPaths;
        List<String> matchedPartPaths;
        if (hiveSync) {
            // hive-sync: HMS registers the hive-style names, which ARE the relative storage layout, so fsView
            // accepts them directly (no relativization, matching legacy / collectPartitions). Prune the HMS names.
            allPartPaths = hmsClient.listPartitionNames(
                    hudiHandle.getDbName(), hudiHandle.getTableName(), -1);
            if (allPartPaths == null || allPartPaths.isEmpty()) {
                return Optional.empty();
            }
            matchedPartPaths = prunePartitionNames(allPartPaths, partKeyNames, partitionPredicates);
        } else {
            // non-hive-sync (Hudi default): list the RELATIVE storage paths from Hudi metadata -- the SAME source
            // the unpruned scan (resolvePartitions -> listAllPartitionPaths) uses -- under the plugin auth + TCCL
            // pin. Net-neutral: resolvePartitions short-circuits once prunedPaths is set, so a filtered query lists
            // exactly once (here) instead of there. parsePartitionValues handles the positional layout ("2024/01").
            allPartPaths = metaClientExecutor.execute(() ->
                    HudiScanPlanProvider.listAllPartitionPaths(
                            HudiScanPlanProvider.buildMetaClient(buildHadoopConf(), hudiHandle.getBasePath())));
            if (allPartPaths == null || allPartPaths.isEmpty()) {
                return Optional.empty();
            }
            matchedPartPaths = prunePartitionPaths(allPartPaths, partKeyNames, partitionPredicates);
        }
        if (matchedPartPaths.size() == allPartPaths.size()) {
            // No pruning effect
            return Optional.empty();
        }

        LOG.info("Partition pruning: {}.{} hiveSync={} all={} pruned={}",
                hudiHandle.getDbName(), hudiHandle.getTableName(),
                hiveSync, allPartPaths.size(), matchedPartPaths.size());

        // Build updated handle carrying only the matched (relative-shape) partition paths for scan planning.
        HudiTableHandle updatedHandle = hudiHandle.toBuilder()
                .prunedPartitionPaths(matchedPartPaths)
                .build();

        return Optional.of(new FilterApplicationResult<>(updatedHandle, constraint.getExpression(), false));
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        // Latest schema (no time-travel pin). Shares the single build path with the at-instant overload below
        // (null instant = latest) so the two can never drift.
        return buildTableSchema((HudiTableHandle) handle, null);
    }

    /**
     * Returns the schema AS OF the pinned instant for a {@code FOR TIME AS OF} read under schema evolution
     * (HD-C5a), closing HD-C2 residual #1 (the SPI default previously returned the LATEST schema, ignoring the
     * pin). Keys off {@link HudiTableHandle#getQueryInstant()} &mdash; the instant {@link #applySnapshot} stamps
     * onto the handle &mdash; NOT {@code snapshot.getSchemaId()}, which stays {@code -1} for hudi (hudi pins by
     * instant, not by a numeric schema id, so the generic schema-id carrier is inert here; fe-core passes the
     * post-{@code applySnapshot} handle, so the instant is already on it). A {@code null} instant (a plain read,
     * or a non-{@code FOR TIME} pin such as {@code @incr}, whose {@code loadSnapshot} branch lists the LATEST
     * schema itself and never reaches here) resolves via the SAME shared build method with a null instant, so
     * latest and at-instant cannot drift. Hive gateway delegation for this 3-arg overload already shipped
     * ({@code HiveConnectorMetadata}), so no hive change is needed.
     */
    @Override
    public ConnectorTableSchema getTableSchema(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        return buildTableSchema(hudiHandle, hudiHandle.getQueryInstant());
    }

    /**
     * Single build path for {@link #getTableSchema}: reads the columns from the Hudi metaClient AS OF
     * {@code queryInstant} ({@code null} = latest) and wraps them in a {@link ConnectorTableSchema}. Shared by
     * the latest (2-arg) and at-instant (3-arg) entry points so they cannot diverge.
     */
    private ConnectorTableSchema buildTableSchema(HudiTableHandle hudiHandle, String queryInstant) {
        String basePath = hudiHandle.getBasePath();

        List<ConnectorColumn> columns;
        if (basePath != null && !basePath.isEmpty()) {
            columns = getSchemaFromMetaClient(basePath, queryInstant);
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

    // ========== Read-only write-reject safety net ==========

    /**
     * Hudi-on-HMS tables are READ-ONLY in this catalog (data is written by Spark/Flink; Doris only reads). A hudi
     * table's write is already rejected UP FRONT by the engine's admission gate — {@code
     * supportedWriteOperations(handle)} derives from {@link #getWritePlanProvider(ConnectorTableHandle)} which
     * this connector leaves at the SPI default {@code null} &rarr; empty operation set &rarr; the {@code
     * PhysicalPlanTranslator} INSERT/row-level-DML gates throw a clean {@code AnalysisException} before ever
     * opening a transaction; {@code EXECUTE} is rejected by {@code getProcedureOps} &rarr; {@code null}; every DDL
     * op throws the SPI default {@code "... not supported"}. This override is the explicit LAST-LINE DEFENSE: it
     * replaces the generic {@code "Transactions not supported"} default with a hudi-specific read-only message so
     * that any future write path reaching transaction-open (bypassing the gate) fails loud with the RIGHT reason
     * rather than a confusing generic one. Overriding the no-arg form covers the per-handle overload too (its SPI
     * default delegates here). It is deliberately NOT placed on {@code getWritePlanProvider}: the admission gate
     * calls {@code getWritePlanProvider} to DECIDE, so throwing there would make the gate throw a {@code
     * DorisConnectorException} the engine misclassifies as an internal error instead of the clean "does not
     * support INSERT" — keep the provider {@code null} and reject explicitly only here (dormant until hms enters
     * {@code SPI_READY_TYPES}).
     */
    @Override
    public ConnectorTransaction beginTransaction(ConnectorSession session) {
        throw new DorisConnectorException(
                "Hudi tables are read-only in this catalog; INSERT/UPDATE/DELETE/MERGE are not supported");
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

    /**
     * Supplies the ROW-LEVEL correctness filter for an {@code @incr} incremental read as a connector-neutral
     * residual predicate (the neutral synthetic-predicate SPI). A COW base file rewritten inside the
     * {@code (begin, end]} window also carries forward older-commit rows, so selecting the touched files is NOT
     * enough — the engine must additionally apply {@code _hoodie_commit_time > begin AND _hoodie_commit_time
     * <= end} at the row level. This is exactly the filter legacy injected via
     * {@code LogicalHudiScan.generateIncrementalExpression}, re-homed here so fe-core stays source-agnostic
     * (it reverse-converts the returned {@link ConnectorExpression}s and wraps a filter; it never branches on
     * the connector).
     *
     * <p>The window is read from the SAME resolved {@code snapshot} that {@link #applySnapshot} consumes
     * ({@link #HUDI_INCREMENTAL_BEGIN_PROPERTY}/{@link #HUDI_INCREMENTAL_END_PROPERTY}, stamped ONCE by
     * {@link #resolveIncremental}), so the row filter and the file-selection window are the single same
     * resolution and can never diverge on an advancing timeline. Both bounds are STRING literals over a STRING
     * column ref — lexicographic compare over fixed-width Hudi instants, byte-faithful to legacy (a numeric
     * coercion would silently corrupt the ordering).</p>
     *
     * <p>Returns an EMPTY list for any non-incremental read (a plain latest read or {@code FOR TIME AS OF}
     * pin carries no {@code (begin, end]} window), so those plans stay byte-identical.</p>
     */
    @Override
    public List<ConnectorExpression> getSyntheticScanPredicates(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        if (snapshot == null) {
            return Collections.emptyList();
        }
        Map<String, String> properties = snapshot.getProperties();
        String begin = properties.get(HUDI_INCREMENTAL_BEGIN_PROPERTY);
        String end = properties.get(HUDI_INCREMENTAL_END_PROPERTY);
        if (begin == null || end == null) {
            // Not an incremental read (plain / FOR TIME AS OF pins carry no window) -> no synthetic filter.
            return Collections.emptyList();
        }
        ConnectorType stringType = ConnectorType.of("STRING");
        org.apache.doris.connector.api.pushdown.ConnectorColumnRef commitTime =
                new org.apache.doris.connector.api.pushdown.ConnectorColumnRef(HUDI_COMMIT_TIME_COLUMN, stringType);
        ConnectorExpression lower = new ConnectorComparison(
                ConnectorComparison.Operator.GT, commitTime, ConnectorLiteral.ofString(begin));
        ConnectorExpression upper = new ConnectorComparison(
                ConnectorComparison.Operator.LE, commitTime, ConnectorLiteral.ofString(end));
        // Two flat conjuncts: fe-core ANDs them into one LogicalFilter (byte-faithful to legacy
        // ImmutableSet.of(great, less)); no ConnectorAnd wrapper is needed.
        return List.of(lower, upper);
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

    /**
     * Engine-neutral rows for the {@code hudi_meta()} / TIMELINE metadata table: one row per instant of the FULL
     * active timeline (all states, NOT the completed-only helper — the TVF shows a {@code state} column), each
     * mapped to the 4 String cells the TVF renders (requestedTime / action / state / completionTime). The metaClient
     * touch runs under the plugin auth + TCCL pin (like {@link #latestInstant} / {@link #collectPartitions}), so
     * fe-core adds no pin of its own. {@code completionTime} is {@code null} for a non-completed instant (rendered
     * SQL NULL) — byte-parity with the legacy fe-core inline loop. Unknown {@code kind} returns nothing.
     */
    @Override
    public List<List<String>> getMetadataTableRows(ConnectorSession session, ConnectorTableHandle handle,
            String kind) {
        if (!"timeline".equals(kind)) {
            return Collections.emptyList();
        }
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        return metaClientExecutor.execute(() -> {
            HoodieTableMetaClient metaClient =
                    HudiScanPlanProvider.buildMetaClient(buildHadoopConf(), hudiHandle.getBasePath());
            List<List<String>> rows = new ArrayList<>();
            for (HoodieInstant instant : metaClient.getActiveTimeline().getInstants()) {
                rows.add(Arrays.asList(instant.requestedTime(), instant.getAction(),
                        instant.getState().name(), instant.getCompletionTime()));
            }
            return rows;
        });
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
     * Reads the columns from the Hudi metaClient AS OF {@code queryInstant} ({@code null} = the latest Avro
     * schema, the authoritative schema for a plain read). The whole metaClient touch runs under
     * {@link HudiMetaClientExecutor#execute} (plugin UGI {@code doAs} + TCCL pin) &mdash; HD-C5a closes the
     * previously-unwrapped auth/TCCL gap, because {@code getTableSchema} can be called off the TCCL-pinned scan
     * thread (e.g. catalog metadata load / MTMV refresh).
     *
     * <p><b>At-instant ({@code queryInstant != null}, {@code FOR TIME AS OF}).</b> Byte-faithful to legacy
     * {@code HiveMetaStoreClientHelper.getHudiTableSchema} + {@code HMSExternalTable.initHudiSchema}: reload the
     * active timeline, then {@code getTableInternalSchemaFromCommitMetadata(instant)}. When present
     * ({@code hoodie.schema.on.read.enable} is ON) the columns/ids come from that AT-INSTANT
     * {@link InternalSchema} (stable ids across renames), so a renamed column shows its historical name and BE
     * matches its old files BY FIELD ID. When absent (evolution off) it falls through to the latest path &mdash;
     * byte-equivalent for a non-evolution table whose schema never changed (legacy's latest-fallback, design
     * decision D3).</p>
     *
     * <p>Package-private (not static) so a same-loader test can override it to assert the {@code queryInstant}
     * is threaded from the handle without a live metaClient (the actual at-instant read is e2e).</p>
     */
    List<ConnectorColumn> getSchemaFromMetaClient(String basePath, String queryInstant) {
        try {
            return metaClientExecutor.execute(() -> {
                HoodieTableMetaClient metaClient =
                        HudiScanPlanProvider.buildMetaClient(buildHadoopConf(), basePath);
                TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
                if (queryInstant != null) {
                    // Reload so a recently-committed instant's schema file is readable, not a stale cached one
                    // (legacy getHudiTableSchema reloads for exactly this reason).
                    metaClient.reloadActiveTimeline();
                    Option<InternalSchema> atInstant =
                            schemaResolver.getTableInternalSchemaFromCommitMetadata(queryInstant);
                    if (atInstant.isPresent()) {
                        return columnsFromInternalSchema(atInstant.get());
                    }
                    // schema.on.read off -> no commit-metadata InternalSchema -> latest fallback (D3):
                    // byte-equivalent for a non-evolution table (its schema never changed).
                }
                // Latest schema. Include the 5 `_hoodie_*` meta columns (byte-faithful to legacy
                // getHudiTableSchema, which passes `true` unconditionally). The explicit `true` (a) restores
                // legacy SELECT * / DESCRIBE parity even for a populate.meta.fields=false table and (b) keeps
                // `_hoodie_commit_time` a visible, name-bindable column for the synthetic incremental row filter.
                Schema avroSchema = schemaResolver.getTableAvroSchema(true);
                List<ConnectorColumn> columns = avroSchemaToColumns(avroSchema);
                return attachHudiFieldIds(schemaResolver, avroSchema, columns);
            });
        } catch (Exception e) {
            // Pass the throwable (not e.getMessage()) so the full cause chain + stack survives the
            // HudiMetaClientExecutor.execute DorisConnectorException wrapper (whose fixed message would otherwise
            // mask WHY this best-effort read degraded to an empty column list / BY_NAME).
            LOG.warn("Failed to get schema from Hudi MetaClient for path '{}' (instant={})",
                    basePath, queryInstant, e);
            return Collections.emptyList();
        }
    }

    /**
     * Builds the column list from an AT-INSTANT {@link InternalSchema} (schema-on-read time travel). Mirror of
     * legacy {@code HMSExternalTable.initHudiSchema}: convert the InternalSchema to Avro to derive the column
     * names/types AT the instant, then attach each top-level field id from the SAME InternalSchema (stable
     * across renames). The record name passed to {@code convert} is cosmetic (only the ROOT record is named; the
     * derived columns come from its fields), so a constant is used. Field-id resolution and lowercasing match
     * the latest path exactly (shared {@link #attachTopLevelFieldIds}).
     */
    private List<ConnectorColumn> columnsFromInternalSchema(InternalSchema internalSchema) {
        Schema avroSchema = AvroInternalSchemaConverter.convert(internalSchema, "hudi_table");
        List<ConnectorColumn> columns = avroSchemaToColumns(avroSchema);
        return attachTopLevelFieldIds(columns, internalSchema);
    }

    /**
     * Resolve the mode-aware InternalSchema for the LATEST commit and attach each column's top-level field id
     * (HD-C4b). Mirror of legacy {@code HiveMetaStoreClientHelper.getHudiTableSchema}: when {@code
     * hoodie.schema.on.read.enable} is on, the ids come from the commit-metadata {@link InternalSchema} (STABLE
     * across renames, so a renamed column keeps its id and BE matches its old files BY FIELD ID); otherwise from
     * {@code AvroInternalSchemaConverter.convert(latest avro)} (positional ids). The no-arg {@code
     * getTableInternalSchemaFromCommitMetadata()} pins the latest commit — steady-state / no time-travel pin (an
     * at-instant variant is a later step).
     *
     * <p>On ANY resolution failure the columns are returned unchanged (ids stay {@code UNSET_UNIQUE_ID} -> BE
     * BY_NAME, the safe baseline) rather than dropping the whole schema — a schema-evolution id hiccup must not
     * fail a plain read.</p>
     *
     * <p>Unlike legacy (which sources columns AND ids from the same InternalSchema and zips positionally), the
     * connector keeps its independent {@code getTableAvroSchema(true)} column source (preserving the shipped
     * meta-column-inclusive schema), so ids are matched BY NAME in {@link #attachTopLevelFieldIds}. This runs
     * inside the {@link HudiMetaClientExecutor#execute} wrapper that {@link #getSchemaFromMetaClient} now
     * establishes (HD-C5a closed the previously-unwrapped auth/TCCL gap).</p>
     */
    private List<ConnectorColumn> attachHudiFieldIds(TableSchemaResolver schemaResolver, Schema latestAvro,
            List<ConnectorColumn> columns) {
        try {
            InternalSchema internalSchema =
                    HudiSchemaUtils.resolveTableInternalSchema(schemaResolver, latestAvro).internalSchema;
            return attachTopLevelFieldIds(columns, internalSchema);
        } catch (Exception e) {
            LOG.warn("Failed to resolve Hudi field ids; falling back to name-based (BY_NAME) matching: {}",
                    e.getMessage());
            return columns;
        }
    }

    /**
     * Attach each column's top-level field id from {@code internalSchema}, matched by (lower-cased) name. Port of
     * legacy {@code HudiUtils.updateHudiColumnUniqueId} at the top level only: the handle carries the top-level
     * field id, while nested field ids for the BE schema dictionary come straight from the InternalSchema via
     * {@link HudiSchemaUtils}. A column with no matching InternalSchema field (e.g. a {@code _hoodie_*} meta
     * column absent from a commit-metadata schema) keeps {@link ConnectorColumn#UNSET_UNIQUE_ID}; because BE's
     * field-id mode is per-file (not per-column), that unresolved id gates the whole scan-level dict OFF ->
     * BE BY_NAME for the entire scan (see {@code HudiSchemaUtils.buildSchemaEvolutionProp}), never a silent
     * per-column drop. Package-private + static for same-loader unit testing.
     */
    static List<ConnectorColumn> attachTopLevelFieldIds(List<ConnectorColumn> columns, InternalSchema internalSchema) {
        Map<String, Integer> idByName = new HashMap<>();
        for (Types.Field field : internalSchema.getRecord().fields()) {
            idByName.put(field.name().toLowerCase(Locale.ROOT), field.fieldId());
        }
        List<ConnectorColumn> result = new ArrayList<>(columns.size());
        for (ConnectorColumn col : columns) {
            Integer id = idByName.get(col.getName().toLowerCase(Locale.ROOT));
            result.add(id != null ? col.withUniqueId(id) : col);
        }
        return result;
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
        // Storage credentials (fs.s3a.* …) first so an inline user fs./dfs./hadoop. key still wins below; see
        // storageHadoopConfig field. Without it the metaClient's S3AFileSystem cannot read hoodie.properties.
        storageHadoopConfig.forEach(conf::set);
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
        if (!(expr instanceof ConnectorLiteral)) {
            return null;
        }
        Object val = ((ConnectorLiteral) expr).getValue();
        if (val == null) {
            return null;
        }
        if (val instanceof LocalDateTime) {
            // H2: a DATETIME/TIMESTAMP partition literal arrives as a LocalDateTime (DATE arrives as LocalDate).
            // String.valueOf would call toString() -> ISO "2024-01-01T10:00" (T separator, dropped zero seconds),
            // which never string-equals the Hive-canonical stored partition value "2024-01-01 10:00:00" in
            // matchesPredicates -> the whole table prunes to 0 rows. Render Hive-canonical text instead.
            return hiveDateTimeString((LocalDateTime) val);
        }
        return String.valueOf(val);
    }

    /**
     * Renders a DATETIME/TIMESTAMP literal as Hive-canonical partition text: {@code yyyy-MM-dd HH:mm:ss} (space
     * separator, full seconds), appending trailing-zero-trimmed microseconds only when a sub-second part is
     * present. Matches the stored Hive partition value for a scale-0 DATETIME partition (the only realistic case)
     * so the pruning string-compare hits. Package-private static for offline unit testing.
     *
     * <p>Contract: {@code convertDateLiteral} produces microsecond precision (nano = micros*1000), so the nano is
     * always a multiple of 1000; a sub-microsecond nano (unreachable on the pruning path) would be truncated.
     */
    static String hiveDateTimeString(LocalDateTime ldt) {
        String base = ldt.format(HIVE_DATETIME_SECONDS_FORMAT);
        int nano = ldt.getNano();
        if (nano == 0) {
            return base;
        }
        String micros = String.format("%06d", nano / 1000);
        int end = micros.length();
        while (end > 1 && micros.charAt(end - 1) == '0') {
            end--;
        }
        return base + "." + micros.substring(0, end);
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

    /**
     * Prunes Hudi RELATIVE partition paths (positional {@code "2024/01"} or hive-style {@code
     * "year=2024/month=01"}) using {@link HudiScanPlanProvider#parsePartitionValues} (handles both layouts and
     * unescapes) + {@link #matchesPredicates}. Used by the non-hive-sync {@link #applyFilter} branch, whose
     * candidate source is the Hudi metadata listing — the same relative-path shape the scan feeds fsView. Static +
     * package-private for offline unit testing.
     */
    static List<String> prunePartitionPaths(List<String> allPartPaths,
            List<String> partKeyNames, Map<String, List<String>> predicates) {
        List<String> matched = new ArrayList<>();
        for (String partPath : allPartPaths) {
            Map<String, String> partValues = HudiScanPlanProvider.parsePartitionValues(partPath, partKeyNames);
            if (matchesPredicates(partValues, predicates)) {
                matched.add(partPath);
            }
        }
        return matched;
    }

    static Map<String, String> parsePartitionName(String partName,
            List<String> partKeyNames) {
        Map<String, String> values = new HashMap<>();
        String[] parts = partName.split("/");
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                // Unescape the VALUE: HMS get_partition_names returns Hive-escaped names (e.g. "%3A" for ':').
                // The predicate literal side (extractLiteralValue) is unescaped, so matchesPredicates' string
                // compare needs the value unescaped too — otherwise an escaped partition value silently drops
                // rows. Mirrors the sibling scan-side parse (HudiScanPlanProvider.parsePartitionValues) and
                // legacy FileUtils.unescapePathName. The key is a column name (never escaped), left as-is.
                values.put(part.substring(0, eq),
                        HudiScanPlanProvider.unescapePathName(part.substring(eq + 1)));
            }
        }
        return values;
    }

    static boolean matchesPredicates(Map<String, String> partValues,
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
