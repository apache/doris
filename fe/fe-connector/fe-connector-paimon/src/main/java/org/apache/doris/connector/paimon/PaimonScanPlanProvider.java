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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.metastore.paimon.jdbc.PaimonJdbcMetaStoreProperties;
import org.apache.doris.connector.metastore.spi.JdbcDriverSupport;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPaimonDeletionFileDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.system.ReadOptimizedTable;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Scan plan provider for Paimon tables.
 *
 * <p>Three split paths:
 * <ol>
 *   <li><b>JNI reader</b> (default): Serializes the entire Paimon {@code Split} object.
 *       BE calls back into Paimon Java code via JNI.</li>
 *   <li><b>Native reader</b>: When {@code DataSplit.convertToRawFiles()} succeeds and
 *       all files are ORC/Parquet. BE reads files natively.</li>
 *   <li><b>COUNT pushdown</b>: When the query is COUNT(*) and the split has
 *       pre-computed merged row count.</li>
 * </ol>
 *
 * <p><b>Partition pruning (P5-T09): pure predicate pushdown.</b> Only the 4-arg
 * {@link #planScan} is overridden; the engine's 6-arg {@code planScan(..., requiredPartitions)}
 * (the Nereids-pruned partition set) is intentionally NOT overridden. Paimon prunes partitions
 * <em>and</em> data files internally: the Doris filter is converted by
 * {@link PaimonPredicateConverter} and pushed via {@code ReadBuilder.withFilter}, and the Paimon
 * SDK's {@code newScan().plan().splits()} eliminates non-matching partitions/files from those
 * predicates. Partition columns are ordinary columns in Paimon's {@code RowType}, so a partition
 * predicate is just another pushed predicate. This differs from MaxCompute (whose ODPS read
 * session needs explicit {@code PartitionSpec}s and therefore consumes {@code requiredPartitions});
 * for Paimon the engine set would be redundant with the predicate it already pushes. The SPI
 * default chain (6-arg &rarr; 5-arg &rarr; 4-arg) routes correctly with {@code requiredPartitions}
 * dropped. As of B5 the connector emits {@code partition_columns} (see
 * {@code PaimonConnectorMetadata.buildTableSchema}), so FE now treats Paimon tables as partitioned and
 * the Nereids-pruned set feeds FE EXPLAIN ({@code partition=N/M}) only. Because Paimon is fully
 * predicate-driven, this provider returns {@code true} from {@link #ignorePartitionPruneShortCircuit()}:
 * a GENUINE prune-to-zero (FE pruning emptied the partition set) is NOT short-circuited to zero rows but
 * mapped to scan-all, so {@code planScan} re-plans from the pushed predicate. This is load-bearing once a
 * genuine-null partition is rendered as a NON-null sentinel ({@code isNull=false}, master parity): {@code
 * col IS NULL} prunes every partition away at FE, yet the genuine-null rows must still be returned via the
 * pushed predicate (the legacy {@code PaimonScanNode} never consults the FE partition selection). The
 * time-travel pin (empty partition-item map over an empty universe) was already guarded the same way in
 * {@code PluginDrivenScanNode.resolveRequiredPartitions}. None of this affects read-row correctness.
 */
public class PaimonScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(PaimonScanPlanProvider.class);

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE_REF =
            new TypeReference<Map<String, String>>() {};

    // Session variable name (byte-identical to SessionVariable.FORCE_JNI_SCANNER) surfaced through
    // ConnectorSession.getSessionProperties() (VariableMgr.toMap). When true it is the user/session JNI
    // escape hatch: every native-eligible DataSplit is routed to the JNI reader (legacy
    // PaimonScanNode.getSplits gate, sessionVariable.isForceJniScanner()), bypassing the native ORC/Parquet
    // readers to dodge native-reader bugs. Default false (legacy default).
    //
    // NOTE: enable_paimon_cpp_reader is deliberately NOT read here. Upstream #66008 removed the paimon-cpp
    // arm from PaimonScanNode.setPaimonParams (file-scanner-v2 has no split-aware paimon-cpp adapter and
    // hard-rejects a PAIMON_CPP range), so the flag no longer influences planning — see
    // PaimonScanRange.populateRangeParams.
    private static final String FORCE_JNI_SCANNER = "force_jni_scanner";

    // Session variable name (byte-identical to SessionVariable.IGNORE_SPLIT_TYPE) surfaced through the same
    // VariableMgr.toMap channel. A debugging escape hatch to isolate reader bugs: IGNORE_JNI drops every JNI
    // split, IGNORE_NATIVE drops every native split (legacy PaimonScanNode.getSplits). IGNORE_PAIMON_CPP is a
    // documented option but was NEVER consulted by legacy getSplits, so it stays a no-op here (legacy parity).
    // Default NONE, so normal reads are unaffected.
    private static final String IGNORE_SPLIT_TYPE = "ignore_split_type";
    private static final String IGNORE_SPLIT_TYPE_JNI = "IGNORE_JNI";
    private static final String IGNORE_SPLIT_TYPE_NATIVE = "IGNORE_NATIVE";
    private static final String ENABLE_EXTERNAL_SCAN_TASK_REUSE = "enable_external_scan_task_reuse";
    static final String SCAN_REUSE_NAMESPACE = "paimon.scan-reuse";

    // FIX-NATIVE-SUBSPLIT (M-3): file-split session vars (byte-identical to SessionVariable.{FILE_SPLIT_SIZE,
    // MAX_INITIAL_FILE_SPLIT_SIZE, MAX_FILE_SPLIT_SIZE, MAX_INITIAL_FILE_SPLIT_NUM, MAX_FILE_SPLIT_NUM}),
    // read via the same VariableMgr.toMap channel as FORCE_JNI_SCANNER. They drive the native
    // sub-split target size, mirroring legacy PaimonScanNode.determineTargetFileSplitSize without
    // importing fe-core SessionVariable/FileSplitter. Defaults below are byte-identical to SessionVariable.
    private static final String FILE_SPLIT_SIZE = "file_split_size";
    private static final String MAX_INITIAL_FILE_SPLIT_SIZE = "max_initial_file_split_size";
    private static final String MAX_FILE_SPLIT_SIZE = "max_file_split_size";
    private static final String MAX_INITIAL_FILE_SPLIT_NUM = "max_initial_file_split_num";
    private static final String MAX_FILE_SPLIT_NUM = "max_file_split_num";
    private static final long DEFAULT_MAX_INITIAL_FILE_SPLIT_SIZE = 32L * 1024 * 1024;
    private static final long DEFAULT_MAX_FILE_SPLIT_SIZE = 64L * 1024 * 1024;
    private static final long DEFAULT_MAX_INITIAL_FILE_SPLIT_NUM = 200L;
    private static final long DEFAULT_MAX_FILE_SPLIT_NUM = 100000L;

    // FIX-SCHEMA-EVOLUTION (B-1a): scan-level prop carrying the base64 TBinaryProtocol-serialized
    // schema dictionary (a throwaway TFileScanRangeParams holding current_schema_id +
    // history_schema_info). getScanNodeProperties builds it from the live table; populateScanLevelParams
    // applies it to the real params. Transport via the props map because getScanPlanProvider() returns a
    // fresh provider per call (no shared instance state between the two SPI methods).
    private static final String SCHEMA_EVOLUTION_PROP = "paimon.schema_evolution";
    // Legacy parity: current_schema_id is the -1 sentinel ("latest"); the current/target schema is
    // also pushed into history_schema_info under this key (PaimonScanNode.doInitialize -> -1L).
    private static final long CURRENT_SCHEMA_ID = -1L;

    // Connector-private scan node property key (the engine never reads it): carries the base64-serialized
    // paimon Table from getScanNodeProperties to populateScanLevelParams, which puts it on the thrift.
    private static final String PROP_SERIALIZED_TABLE = "paimon.serialized_table";
    private static final String PROP_SERIALIZED_TABLE_CACHE_KEY =
            "paimon.serialized_table_cache_key";
    private static final String DORIS_MANIFEST_PARALLELISM_CAP =
            "doris.scan.manifest.parallelism-cap";
    private static final String DORIS_SERIALIZED_SYSTEM_SOURCE = "doris.serialized-system-source";
    private static final String DORIS_SYSTEM_TABLE_TYPE = "doris.system-table-type";

    private final PaimonCatalogProperties catalogProps;
    private final PaimonCatalogOps catalogOps;
    private final ConnectorContext context;
    // FIX-B-R2-be: connector-level (per-catalog, long-lived) memo of the per-committed-schema-id field
    // read used by the schema-evolution dict (buildSchemaEvolutionParam). Injected by PaimonConnector so
    // it is the SAME instance getMetadata uses (the cached fact (handle,schemaId)->schema fields is shared
    // with the B-MC2 time-travel path). The public 2/3-arg ctors give each provider its OWN fresh memo so
    // the existing construction sites are unchanged (first build = direct read = pre-fix behavior).
    private final PaimonSchemaAtMemo schemaAtMemo;

    // FIX-SCAN-METRICS: per-query stash of the paimon SDK scan diagnostics harvested during planScan, keyed
    // by session queryId. fe-core drains it (collectScanProfiles) right after planScan on the same thread;
    // releaseReadTransaction reclaims any entry a thrown planScan left behind. Bounded to the sync planScan
    // path (paimon never streams), so the CopyOnWriteArrayList value is only ever appended single-threaded.
    private final ConcurrentHashMap<String, List<ConnectorScanProfile>> scanProfileStash = new ConcurrentHashMap<>();

    public PaimonScanPlanProvider(PaimonCatalogProperties catalogProps, PaimonCatalogOps catalogOps) {
        this(catalogProps, catalogOps, null);
    }

    public PaimonScanPlanProvider(PaimonCatalogProperties catalogProps, PaimonCatalogOps catalogOps,
            ConnectorContext context) {
        this(catalogProps, catalogOps, context, new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE));
    }

    PaimonScanPlanProvider(PaimonCatalogProperties catalogProps, PaimonCatalogOps catalogOps,
            ConnectorContext context, PaimonSchemaAtMemo schemaAtMemo) {
        this.catalogProps = catalogProps;
        this.catalogOps = catalogOps;
        this.context = context;
        this.schemaAtMemo = schemaAtMemo;
    }

    /** Test-only: the schema memo this provider was wired with (to pin the connector injection). */
    PaimonSchemaAtMemo schemaAtMemoForTest() {
        return schemaAtMemo;
    }

    /**
     * Reads the {@code force_jni_scanner} session flag from the SPI session properties (forwarded by the
     * engine via {@code VariableMgr.toMap}). When true the JNI escape
     * hatch is engaged: native-eligible DataSplits are routed to JNI (see
     * {@link #shouldUseNativeReader}), bypassing the native ORC/Parquet readers to dodge native-reader
     * bugs. Variant projections remain native because JNI has no Variant carrier. Default false
     * (legacy default), so normal reads are unaffected. Package-private static for offline unit testing.
     */
    static boolean isForceJniScannerEnabled(ConnectorSession session) {
        if (session == null) {
            return false;
        }
        return Boolean.parseBoolean(session.getSessionProperties().get(FORCE_JNI_SCANNER));
    }

    /**
     * Reads the {@code ignore_split_type} session variable (same {@code VariableMgr.toMap} channel as
     * {@link #isForceJniScannerEnabled}). Returns {@code "NONE"} when the session is absent (offline unit tests)
     * or the variable is unset, matching this file's null-tolerant session-read convention. Only
     * {@code IGNORE_JNI} / {@code IGNORE_NATIVE} carry behavior (skip the matching split type, legacy
     * {@code PaimonScanNode.getSplits}); every other value (incl. {@code NONE} / {@code IGNORE_PAIMON_CPP})
     * is a no-op. Package-private static for offline unit testing.
     */
    static String resolveIgnoreSplitType(ConnectorSession session) {
        if (session == null) {
            return "NONE";
        }
        return session.getSessionProperties().getOrDefault(IGNORE_SPLIT_TYPE, "NONE");
    }

    /**
     * Returns the handle's transient Paimon {@link Table}, reloading it from the catalog seam
     * when the transient reference is null (e.g. after a serialization round-trip across the
     * FE/BE boundary or plan reuse). Delegates to the single sys-aware {@link PaimonTableResolver}
     * shared with the metadata path, so a deserialized SYSTEM handle reloads its own (sys) Table
     * via the 4-arg sys {@link Identifier} instead of silently scanning the base table.
     * Package-private for direct unit testing.
     *
     * <p>NOTE: the reloaded Table may come from a different {@link org.apache.paimon.catalog.Catalog}
     * instance than the one that produced the handle. That is acceptable for this fallback safety
     * net (it is not snapshot-consistent with the handle's originating catalog).
     */
    Table resolveTable(PaimonTableHandle paimonHandle) {
        // M-11: wrap the (possibly remote) reload in executeAuthenticated (D-052) so the scan path's
        // table resolution runs under the FE-injected Kerberos UGI, matching the metadata twin. The
        // transient-table fast path issues no RPC. The FileIO split planning is wrapped separately in
        // planSplits (iceberg fourth-locus parity — the un-wrapped plan-time manifest read is exactly
        // what failed SASL on iceberg's kerberos CI). When there is no context (offline unit tests
        // via the 2-arg ctor), resolve directly — same convention as getScanNodeProperties above.
        try {
            if (context == null) {
                return PaimonTableResolver.resolve(catalogOps, paimonHandle);
            }
            return context.executeAuthenticated(() -> PaimonTableResolver.resolve(catalogOps, paimonHandle));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Paimon table: " + paimonHandle, e);
        }
    }

    /**
     * Resolves the live {@link Table} for the SCAN path and pins it to the handle's snapshot when
     * the handle carries scan options (set by {@code applySnapshot}'s time-travel / MVCC pin). The
     * pin is applied here (NOT in the metadata {@code resolveTable}) so BOTH the planned splits AND
     * the JNI serialized-table read see the same pinned version, while schema/column/partition
     * metadata reads keep resolving the latest table.
     *
     * <p>{@code Table.copy(dynamicOptions)} layers the paimon scan options (e.g.
     * {@code scan.snapshot-id}) over the resolved table — the same mechanism legacy paimon used.
     */
    Table resolveScanTable(PaimonTableHandle paimonHandle) {
        Table table = resolveTable(paimonHandle);
        Map<String, String> scanOptions = paimonHandle.getScanOptions();
        Table finalTable = table;
        if (scanOptions != null && !scanOptions.isEmpty()) {
            if (PaimonScanParams.isOptionsPin(scanOptions)) {
                // An @options pin owns the whole scan-startup state: applyOptions strips the internal
                // markers and nulls out the absent members of paimon's inherited read-state family, so a
                // scan.mode / tag persisted on the base table cannot leak into this relation's read.
                finalTable = PaimonScanParams.applyOptions(table, scanOptions);
            } else {
                // FIX-INCR-SCAN-RESET: for an @incr read, reapply legacy's null reset of
                // scan.snapshot-id/scan.mode here (the single Table.copy chokepoint shared by both the
                // native/JNI scan path and the JNI serialized-table path) so a stale persisted pin on the
                // base table cannot hijack incremental-between. Non-incremental pins pass through unchanged.
                finalTable = table.copy(PaimonIncrementalScanParams.applyResetsIfIncremental(scanOptions));
            }
        }
        finalTable = PaimonReaderOptions.runtimeSafeTable(finalTable);
        finalTable = runtimeSafeSystemTable(paimonHandle, finalTable, scanOptions);
        // This is the last common boundary before planning and serialization. Normalize and
        // validate only after relation > catalog > physical precedence is established.
        PaimonReaderOptions.validateEffectiveTable(finalTable);
        return finalTable;
    }

    private Table runtimeSafeSystemTable(
            PaimonTableHandle handle, Table systemTable, Map<String, String> scanOptions) {
        if (!handle.isSystemTable()) {
            return systemTable;
        }
        try {
            Table dataTable = PaimonTableResolver.resolveSystemSource(catalogOps, handle, context);
            // System wrappers hide the fallback pair from instanceof checks. Retain the exact source
            // resolved here so split-limit safety applies after transient handles are reloaded too.
            handle.setSystemTableSource(dataTable);
            return PaimonReaderOptions.runtimeSafeSystemTable(
                    handle.getSysTableName(), systemTable, dataTable, scanOptions);
        } catch (IllegalArgumentException e) {
            // Validation details must reach the SQL boundary so users can correct unsafe table options.
            throw new DorisConnectorException(e.getMessage(), e);
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to validate Paimon system table source", e);
        }
    }

    @Override
    public boolean supportsFileCache() {
        // paimon reads native parquet/orc sub-splits where it can (see isNativeReadRange), so the BE file cache
        // applies; this preserves the governance paimon catalogs already had.
        return true;
    }

    /**
     * Paimon is predicate-driven: {@code planScan} ignores {@code requiredPartitions} and re-plans through
     * the SDK with the pushed predicate, so a FE prune-to-zero must scan-all rather than short-circuit to
     * zero rows (required for {@code col IS NULL} parity once a genuine-null partition renders as a NON-null
     * sentinel). See the class-level partition-pruning note.
     */
    @Override
    public boolean ignorePartitionPruneShortCircuit() {
        return true;
    }

    /**
     * Which paimon SYSTEM tables honor {@code @incr}. Only views whose reader can serve a commit range
     * qualify: {@code FilesScan} enumerates the LATEST partitions before its range-aware per-partition
     * scan, so it cannot read a range whose partition has since been dropped.
     */
    @Override
    public boolean supportsSystemTableIncrementalRead(String sysTableName) {
        return PaimonScanParams.supportsIncrementalRead(sysTableName);
    }

    /**
     * Which paimon SYSTEM tables honor {@code @options}. A view qualifies only when EVERY row-producing
     * stage observes the selected snapshot; {@code $files} / {@code $buckets} still consult latest
     * metadata internally, so they decline rather than answer a historical question with current data.
     */
    @Override
    public boolean supportsSystemTableOptions(String sysTableName) {
        return PaimonScanParams.supportsOptions(sysTableName);
    }

    /**
     * The distinct scanned partitions among the just-planned ranges (FIX-L12) — restores legacy
     * {@code PaimonScanNode}'s {@code selectedPartitionNum = partitionInfoMaps.size()} (keyed by
     * {@code dataSplit.partition()}) so EXPLAIN {@code partition=N/M} and {@code sql_block_rule} reflect
     * the partitions the paimon SDK actually resolved after manifest/file-stat pruning, not the engine's
     * declared-column Nereids count. The identity is the rendered {@link PaimonScanRange#getPartitionValues()}
     * map — {@code getPartitionInfoMap(table, dataSplit.partition(), tz)}, deterministic and injective per
     * partition within a scan, so distinct maps == distinct native partitions (multiple ranges of one
     * partition de-dup). Returns empty for an unpartitioned table (every range's partition map is empty),
     * so the engine keeps its own count. A partition column whose type is unserializable makes
     * {@code getPartitionInfoMap} drop the whole map to empty too → empty here → engine keeps the (safe,
     * &ge; real) Nereids count. Only counts this provider's own {@link PaimonScanRange} instances.
     */
    @Override
    public OptionalLong scannedPartitionCount(List<ConnectorScanRange> scanRanges) {
        Set<Map<String, String>> distinctPartitions = new HashSet<>();
        for (ConnectorScanRange range : scanRanges) {
            if (range instanceof PaimonScanRange) {
                Map<String, String> partitionValues = range.getPartitionValues();
                if (partitionValues != null && !partitionValues.isEmpty()) {
                    distinctPartitions.add(partitionValues);
                }
            }
        }
        return distinctPartitions.isEmpty()
                ? OptionalLong.empty() : OptionalLong.of(distinctPartitions.size());
    }

    /**
     * Harvest the paimon SDK scan metrics recorded into {@code registry} by {@code scan.plan()} and stash
     * them keyed by the session queryId for fe-core to drain (FIX-SCAN-METRICS). No-op for a blank queryId
     * (offline/no-session) or a scan the SDK recorded no metrics for.
     */
    private void stashScanProfile(ConnectorSession session, Table table, PaimonTableHandle handle,
            PaimonMetricRegistry registry) {
        // Guard a null session (offline unit tests) — production planScan always carries one.
        if (session == null) {
            return;
        }
        String queryId = session.getQueryId();
        if (queryId == null || queryId.isEmpty()) {
            return;
        }
        String scanLabel = "Table Scan (" + handle.getDatabaseName() + "." + handle.getTableName() + ")";
        PaimonScanMetrics.harvest(registry, table.name(), scanLabel).ifPresent(profile ->
                scanProfileStash.computeIfAbsent(queryId, k -> new CopyOnWriteArrayList<>()).add(profile));
    }

    @Override
    public List<ConnectorScanProfile> collectScanProfiles(ConnectorSession session) {
        String queryId = session.getQueryId();
        if (queryId == null || queryId.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorScanProfile> profiles = scanProfileStash.remove(queryId);
        return profiles == null ? Collections.emptyList() : profiles;
    }

    @Override
    public void releaseReadTransaction(String queryId) {
        // Paimon opens no metastore read transaction (it inherits the SPI no-op); this override only reclaims
        // the scan-metrics stash for a query whose planScan threw AFTER harvesting (the normal path drains it
        // via collectScanProfiles). Same queryId fe-core registered the query-finish callback with.
        if (queryId != null && !queryId.isEmpty()) {
            scanProfileStash.remove(queryId);
        }
    }

    /**
     * The scan entry. Of everything on the request, paimon consumes the handle, the columns, the filter and
     * the row limit, and the no-grouping {@code COUNT(*)} signal (FIX-COUNT-PUSHDOWN, which lets a split
     * answer from its precomputed merged row count); the pruned partition set is not consumed by the paimon
     * read path — it is predicate-driven and re-plans through the SDK from the filter.
     */
    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) request.getTableHandle();
        if (!isExternalScanTaskReuseEnabled(session)) {
            return planScanInternal(session, request.getTableHandle(), request.getColumns(),
                    request.getFilter(), request.getLimit(), request.isCountPushdown());
        }
        if (paimonHandle.isSystemTable()) {
            // System tables resolve their snapshot on the BE and carry deferred side effects
            // (authorized file enumeration); never reuse their planned ranges.
            return planScanInternal(session, request.getTableHandle(), request.getColumns(),
                    request.getFilter(), request.getLimit(), request.isCountPushdown());
        }
        // Statement-scoped reuse: within one statement the identical scan (same table, same
        // branch/options pin, same projection, same filter, same limit, same COUNT pushdown) plans once and
        // every duplicated relation shares the result. The scope is NONE for offline planning and
        // tests, in which case the loader runs on every call. Session variables are constant within
        // a statement and deliberately absent from the key.
        String memoKey = SCAN_REUSE_NAMESPACE + ":" + session.getCatalogId() + ":" + session.getQueryId();
        Map<PaimonScanReuseKey, List<ConnectorScanRange>> scanReuse = session.getStatementScope().computeIfAbsent(
                memoKey, () -> new ConcurrentHashMap<>());
        PaimonScanReuseKey reuseKey = new PaimonScanReuseKey(paimonHandle, request);
        return scanReuse.computeIfAbsent(reuseKey,
                key -> Collections.unmodifiableList(planScanInternal(session,
                        request.getTableHandle(), request.getColumns(), request.getFilter(),
                        request.getLimit(), request.isCountPushdown())));
    }

    private static boolean isExternalScanTaskReuseEnabled(ConnectorSession session) {
        return session != null && "true".equalsIgnoreCase(
                session.getSessionProperties().get(ENABLE_EXTERNAL_SCAN_TASK_REUSE));
    }

    /**
     * Enumerate the read splits — {@code scan.plan()} reads paimon's snapshot/manifest files remotely —
     * inside the auth context when present, the scan-side twin of {@link #resolveTable}'s wrap and the
     * paimon mirror of iceberg's fourth Kerberos locus: on a Kerberos filesystem catalog the plugin bundles
     * hadoop child-first, so the planning thread's FileIO reads the PLUGIN's UserGroupInformation copy,
     * which only the plugin-side doAs ({@code TcclPinningConnectorContext}) logs in — un-wrapped, the
     * plan-time manifest read hits secured HDFS as SIMPLE (iceberg CI proof:
     * test_iceberg_hadoop_catalog_kerberos SELECT failing SASL at exactly this point). Paimon's shared
     * manifest-reader pool reuses the FileSystem paimon cached at first (authenticated) touch — paimon's
     * cache is not UGI-keyed — so the thread-level wrap carries to parallel manifest reads. No live paimon
     * kerberos e2e gates this (parity/defence, same footing as the TcclPinningConnectorContext port); a
     * {@code null} context (offline unit tests) plans directly.
     */
    private List<Split> planSplits(TableScan scan) {
        if (context == null) {
            return scan.plan().splits();
        }
        try {
            return context.executeAuthenticated(() -> scan.plan().splits());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan paimon splits, error message is:" + e.getMessage(), e);
        }
    }

    /**
     * Plans the splits of a {@code scan.file-creation-time-millis} / fallback
     * {@code scan.creation-time-millis} read. Paimon's own file-creation scanner consults LATEST lazily,
     * which would re-race the snapshot the relation was bound at; so the resolution replaced the live
     * lookup with a fixed {@code scan.snapshot-id} and carried the creation-time threshold as an internal
     * marker, and this method reads that pinned snapshot directly with the threshold as a manifest-entry
     * filter.
     *
     * <p>Reading through {@code SnapshotReader} bypasses {@code DataTableBatchScan}, so
     * {@link #preserveBatchScanFilters} re-applies the correctness filters that scan would have added.
     */
    private List<Split> planFileCreationTimeSplits(
            Table table,
            Map<String, String> pinnedOptions,
            List<org.apache.paimon.predicate.Predicate> predicates,
            long fileCreationTime) {
        if (!(table instanceof FileStoreTable)) {
            throw new DorisConnectorException("Paimon file-creation OPTIONS require a data table.");
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        String pinnedSnapshotId = table.options().get(CoreOptions.SCAN_SNAPSHOT_ID.key());
        if (pinnedSnapshotId == null) {
            throw new DorisConnectorException(
                    "Paimon file-creation OPTIONS resolved without a pinned snapshot.");
        }
        SnapshotReader snapshotReader = fileStoreTable.newSnapshotReader()
                .withMode(ScanMode.ALL)
                .withSnapshot(Long.parseLong(pinnedSnapshotId))
                .withManifestEntryFilter(entry -> entry.file().creationTimeEpochMillis() >= fileCreationTime);
        preserveBatchScanFilters(fileStoreTable, snapshotReader);
        predicates.forEach(snapshotReader::withFilter);
        if (context == null) {
            return snapshotReader.read().splits();
        }
        try {
            return context.executeAuthenticated(() -> snapshotReader.read().splits());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to plan paimon file-creation splits, error message is:" + e.getMessage(), e);
        }
    }

    /**
     * Re-applies the correctness filters {@code DataTableBatchScan} would have added, for the direct
     * {@link SnapshotReader} path above: level-0 skipping plus value filtering on deletion-vector /
     * first-row tables, and real-bucket-only reads on a postponed-bucket table. Without them the direct
     * read returns rows the ordinary batch scan would have merged away.
     */
    private void preserveBatchScanFilters(FileStoreTable table, SnapshotReader snapshotReader) {
        CoreOptions options = table.coreOptions();
        if (!table.primaryKeys().isEmpty()
                && options.batchScanSkipLevel0()
                && options.toConfiguration().get(CoreOptions.BATCH_SCAN_MODE) == CoreOptions.BatchScanMode.NONE) {
            snapshotReader.withLevelFilter(level -> level > 0).enableValueFilter();
        }
        if (options.bucket() == BucketMode.POSTPONE_BUCKET) {
            snapshotReader.onlyReadRealBuckets();
        }
    }

    /**
     * Whether this scan is an {@code @incr} read of the {@code $binlog} system table. Detected from the
     * handle (the system-table name) plus the resolved pin (the {@code incremental-between*} keys the
     * {@code @incr} resolution produced), because the connector never sees the raw scan-param clause.
     */
    private boolean isIncrementalBinlogScan(PaimonTableHandle handle, Map<String, String> scanOptions) {
        if (!handle.isSystemTable()
                || !PAIMON_BINLOG_SYSTEM_TABLE.equalsIgnoreCase(handle.getSysTableName())) {
            return false;
        }
        return scanOptions != null
                && (scanOptions.containsKey("incremental-between")
                        || scanOptions.containsKey("incremental-between-timestamp"));
    }

    private List<ConnectorScanRange> planScanInternal(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit,
            boolean countPushdown) {

        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Map<String, String> pinnedOptions = paimonHandle.getScanOptions();
        boolean optionsPin = PaimonScanParams.isOptionsPin(pinnedOptions);
        if (countPushdown && isIncrementalBinlogScan(paimonHandle, pinnedOptions)) {
            // A binlog reader packs an UPDATE_BEFORE/UPDATE_AFTER pair into ONE logical row, so a
            // DataSplit's physical merged row count is not a valid COUNT(*) for this relation. Veto the
            // engine's table-level count pushdown rather than answer with the physical count.
            countPushdown = false;
        }
        if (PaimonScanParams.isPinnedEmptyScan(pinnedOptions)) {
            // Every latest fence, including a plain relation, preserves an empty table as statement
            // state so a commit between binding and split planning cannot appear mid-statement.
            return Collections.emptyList();
        }
        Table table = resolveScanTable(paimonHandle);
        Optional<Long> fileCreationTime = optionsPin
                ? PaimonScanParams.getPinnedFileCreationTime(pinnedOptions)
                : Optional.empty();

        // Build predicates from filter expression
        RowType rowType = table.rowType();
        List<org.apache.paimon.predicate.Predicate> predicates = Collections.emptyList();
        if (filter.isPresent()) {
            PaimonPredicateConverter converter = new PaimonPredicateConverter(rowType);
            predicates = converter.convert(filter.get());
        }

        // Build column projection
        List<String> fieldNames = rowType.getFieldNames().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        int[] projected = columns.stream()
                .filter(c -> c instanceof PaimonColumnHandle)
                .mapToInt(c -> fieldNames.indexOf(
                        ((PaimonColumnHandle) c).getName().toLowerCase()))
                .filter(i -> optionsPin || i >= 0)
                .toArray();
        boolean hasVariantProjection = Arrays.stream(projected)
                .filter(index -> index >= 0)
                .anyMatch(index -> containsVariant(rowType.getTypeAt(index)));

        // FIX-L14: honor the ignore_split_type debugging escape hatch (legacy PaimonScanNode.getSplits):
        // IGNORE_JNI drops JNI splits (nonDataSplit + DataSplit-JNI arms), IGNORE_NATIVE drops native splits.
        // The COUNT(*) arm is never dropped (legacy parity); IGNORE_PAIMON_CPP stays a no-op (legacy getSplits
        // never consulted it). Read once here so discarded JNI splits bypass carrier compatibility checks.
        String ignoreSplitType = resolveIgnoreSplitType(session);
        boolean ignoreJni = IGNORE_SPLIT_TYPE_JNI.equals(ignoreSplitType);
        boolean ignoreNative = IGNORE_SPLIT_TYPE_NATIVE.equals(ignoreSplitType);

        if (hasVariantProjection && paimonHandle.isForceJni() && !ignoreJni) {
            // System-table forceJni preserves row-kind/sequence semantics that raw files cannot reproduce;
            // Variant has no JNI carrier, so failing is safer than silently changing those semantics.
            throw new DorisConnectorException(
                    "Paimon Variant columns are unsupported for force-JNI system tables");
        }
        if (optionsPin && Arrays.stream(projected).anyMatch(index -> index < 0)) {
            // Only an @options read can bind against a schema the scan table does not have: its snapshot
            // is chosen per relation, so a column bound from one version may be absent from the version
            // this scan resolves. Dropping it (what the filter above does for every other path, where the
            // two schemas agree by construction) would silently shift every later projection index by one
            // and hand BE the wrong column. Fail loud instead.
            throw new DorisConnectorException("Paimon scan schema does not contain all bound Doris columns.");
        }

        // Call Paimon SDK
        ReadBuilder readBuilder = table.newReadBuilder();
        if (!predicates.isEmpty()) {
            readBuilder.withFilter(predicates);
        }
        if (projected.length > 0) {
            readBuilder.withProjection(projected);
        }
        if (limit > 0 && limit <= Integer.MAX_VALUE
                && filter.isEmpty()
                && fileCreationTime.isEmpty()
                && hasTrustworthyLimitAccounting(table)
                && !usesFallbackRead(table, paimonHandle)
                && !ignoreJni
                && !ignoreNative) {
            // Only append-only FileStore manifests count final output rows: format tables count
            // files, while primary-key metadata may count deletes that its reader later removes.
            // Ignore routing happens after planning, so pruning first could discard every retained
            // split and hide rows from the non-ignored reader path.
            readBuilder.withLimit((int) limit);
        }
        TableScan scan = readBuilder.newScan();
        // FIX-SCAN-METRICS: attach a metric registry so scan.plan() records its ScanMetrics (manifest cache
        // hit/miss, scan durations, table files skipped/resulted), then harvest them below — restores the
        // legacy PaimonScanNode scan-metrics profile. InnerTableScan.withMetricRegistry is a real body on the
        // AbstractDataTableScan a data table returns; other scan types keep the no-op default (no metrics).
        PaimonMetricRegistry metricRegistry = new PaimonMetricRegistry();
        if (scan instanceof InnerTableScan) {
            scan = ((InnerTableScan) scan).withMetricRegistry(metricRegistry);
        }
        List<Split> paimonSplits = fileCreationTime.isPresent()
                ? planFileCreationTimeSplits(table, pinnedOptions, predicates, fileCreationTime.get())
                : planSplits(scan);
        stashScanProfile(session, table, paimonHandle, metricRegistry);

        String defaultFileFormat = table.options().getOrDefault(
                CoreOptions.FILE_FORMAT.key(), "parquet");

        // Separate DataSplit vs non-DataSplit
        List<DataSplit> dataSplits = new ArrayList<>();
        List<Split> nonDataSplits = new ArrayList<>();
        for (Split split : paimonSplits) {
            if (split instanceof DataSplit) {
                dataSplits.add((DataSplit) split);
            } else {
                nonDataSplits.add(split);
            }
        }

        List<ConnectorScanRange> ranges = new ArrayList<>();

        // FIX-REST-VENDED-URI-NORMALIZE (P9-1): extract the per-table vended token ONCE per scan
        // (validToken() may refresh; legacy computes its storage map once in doInitialize), threaded into
        // the native-path URI normalization below so REST object-store reads normalize via the vended
        // credentials (a REST catalog's static storage map is empty by design, so the static-only path
        // would throw "No storage properties found for schema: oss"). Empty for non-REST tables (FileIO
        // gate in extractVendedToken) and offline unit tests (no context) → the 2-arg normalize folds to
        // the static-map path, leaving non-REST reads byte-unchanged.
        Map<String, String> vendedToken =
                context != null ? extractVendedToken(table) : Collections.emptyMap();

        // FIX-A1: the FE FileSplit proportional-weight denominator (legacy PaimonScanNode:499, set on ALL
        // splits). Session-only, so compute once here (before any split is built). DISTINCT from the
        // file-splitting targetSplitSize below — named weightDenominator to make a positional swap impossible.
        long weightDenominator = resolveSplitWeightDenominator(session);

        // Non-DataSplit → always JNI
        for (Split split : nonDataSplits) {
            if (ignoreJni) {
                // FIX-L14: ignore_split_type=IGNORE_JNI drops JNI splits (legacy getSplits:401).
                continue;
            }
            if (hasVariantProjection) {
                throw new DorisConnectorException(
                        "Paimon Variant columns require native Parquet data files");
            }
            ranges.add(buildJniScanRange(split, defaultFileFormat,
                    Collections.emptyMap(), false, weightDenominator));
        }

        // COUNT(*) pushdown (FIX-COUNT-PUSHDOWN): collapse every split whose merged (post-merge /
        // post-deletion-vector) row count is precomputed into ONE count range carrying the summed
        // total, emitted after the loop — BE serves the count from table_level_row_count (CountReader)
        // without reading data. Mirrors legacy PaimonScanNode's count short-circuit, which is the
        // FIRST routing arm (BEFORE the native/JNI gate): a count-eligible split must NOT also emit a
        // data range, or BE would re-scan and double-count against deletion vectors / PK merge. The
        // collapse == legacy's <=10000 case (singletonList(first) + assignCountToSplits([one], sum) ->
        // one split bearing the full total); legacy's >10000 parallel-split trim needs numBackends (an
        // fe-core-only concern) and is intentionally dropped -> perf-only divergence [deviations-log].
        // Splits WITHOUT a precomputed merged count fall through to the normal native/JNI routing so
        // BE still counts them from file metadata / by reading.
        long countSum = 0;
        DataSplit countRepresentative = null;

        // FIX-NATIVE-SUBSPLIT: target file split size for native ORC/Parquet sub-splitting, computed
        // lazily ONCE on the first native split (legacy hasDeterminedTargetFileSplitSize parity).
        long targetSplitSize = -1;

        Set<Long> physicalVariantSchemaIds = hasVariantProjection
                ? physicalVariantSchemaIds(table, paimonHandle, rowType, columns, dataSplits)
                : Collections.emptySet();

        // Process DataSplits
        for (DataSplit dataSplit : dataSplits) {
            if (isCountPushdownSplit(countPushdown, dataSplit)) {
                countSum += dataSplit.mergedRowCount();
                if (countRepresentative == null) {
                    countRepresentative = dataSplit;
                }
                continue;
            }

            Map<String, String> partitionValues = getPartitionInfoMap(
                    table, dataSplit.partition(), session.getTimeZone());

            Optional<List<RawFile>> optRawFiles = dataSplit.convertToRawFiles();
            Optional<List<DeletionFile>> optDeletionFiles = dataSplit.deletionFiles();

            if (shouldUseNativeReader(paimonHandle.isForceJni(),
                    isForceJniScannerEnabled(session), hasVariantProjection,
                    physicalVariantSchemaIds, optRawFiles)) {
                if (ignoreNative) {
                    // FIX-L14: ignore_split_type=IGNORE_NATIVE drops native splits (legacy getSplits:443).
                    continue;
                }
                // Native reader path: sub-split large ORC/Parquet files for read parallelism
                // (FIX-NATIVE-SUBSPLIT), mirroring legacy fileSplitter.splitFile. Under COUNT(*) pushdown
                // legacy passes splittable=!applyCountPushdown, so a native split that reaches this arm
                // (i.e. NOT siphoned to the count arm because its merged count is not precomputed — e.g. a
                // DV with null cardinality) is kept WHOLE. We mirror that by passing target size 0, which
                // makes buildNativeRanges emit a single whole-file range; the target heuristic is then not
                // needed (and not computed) under count pushdown.
                if (!countPushdown && targetSplitSize < 0) {
                    targetSplitSize = resolveTargetSplitSize(session, dataSplits);
                }
                long effectiveSplitSize = countPushdown ? 0L : targetSplitSize;
                List<RawFile> rawFiles = optRawFiles.get();
                for (int i = 0; i < rawFiles.size(); i++) {
                    RawFile file = rawFiles.get(i);
                    DeletionFile deletionFile =
                            (optDeletionFiles.isPresent() && i < optDeletionFiles.get().size())
                                    ? optDeletionFiles.get().get(i) : null;
                    ranges.addAll(buildNativeRanges(file, deletionFile, defaultFileFormat,
                            partitionValues, vendedToken, effectiveSplitSize, weightDenominator,
                            dataSplit.bucket()));
                }
            } else {
                // JNI reader path
                if (ignoreJni) {
                    // FIX-L14: ignore_split_type=IGNORE_JNI drops JNI splits (legacy getSplits:483).
                    continue;
                }
                if (hasVariantProjection) {
                    throw new DorisConnectorException(
                            "Paimon Variant columns require native Parquet data files");
                }
                ranges.add(buildJniScanRange(dataSplit, defaultFileFormat,
                        partitionValues, true, weightDenominator));
            }
        }

        // Emit the single collapsed count range carrying the summed total (legacy's <=10000 case: one
        // split bearing the full count). Skipped when no split had a precomputed merged count.
        if (countRepresentative != null) {
            Map<String, String> partitionValues = getPartitionInfoMap(
                    table, countRepresentative.partition(), session.getTimeZone());
            ranges.add(buildCountRange(countRepresentative, defaultFileFormat,
                    partitionValues, countSum, weightDenominator));
        }

        return ranges;
    }

    private static boolean usesFallbackRead(Table scanTable, PaimonTableHandle handle) {
        return isFallbackFileStoreTable(scanTable)
                || isFallbackFileStoreTable(handle.getSystemTableSource())
                || isFallbackFileStoreTable(handle.getSysBaseTable());
    }

    private static boolean hasTrustworthyLimitAccounting(Table table) {
        return table instanceof FileStoreTable && table.primaryKeys().isEmpty();
    }

    private static boolean isFallbackFileStoreTable(Table table) {
        return table instanceof FileStoreTable
                && PaimonTableDecorators.unwrapToFallbackOrBase((FileStoreTable) table)
                        instanceof FallbackReadFileStoreTable;
    }

    /**
     * Builds the native-reader {@link PaimonScanRange} for one raw ORC/Parquet file plus its optional
     * deletion vector. BOTH the data-file path and the deletion-vector path are routed through
     * {@link #normalizeUri} so BE's scheme-dispatched S3 factory receives canonical {@code s3://}
     * URIs on OSS/COS/OBS/s3a warehouses (FIX-URI-NORMALIZE; legacy {@code PaimonScanNode} normalizes
     * both via the 2-arg {@code LocationPath.of}). The {@code vendedToken} (empty for non-REST) is the
     * per-table vended credential map, routed into normalization so REST object-store paths normalize via
     * the vended map (FIX-REST-VENDED-URI-NORMALIZE). Package-private so both normalization sites are
     * unit-testable without a live deletion-vector-bearing split.
     */
    PaimonScanRange buildNativeRange(RawFile file, DeletionFile deletionFile,
            String defaultFileFormat, Map<String, String> partitionValues,
            Map<String, String> vendedToken, long start, long length, long weightDenominator,
            int bucket) {
        String fileFormat = getFileFormatBySuffix(file.path()).orElse(defaultFileFormat);
        // FIX-A1: native sub-split FE weight = the sub-range byte length, + the deletion-vector length when
        // attached (legacy PaimonSplit(LocationPath,...).selfSplitWeight = length, setDeletionFile += DV).
        // This is FE-scheduling only; the BE-thrift paimon.self_split_weight stays gated on paimonSplit (A3)
        // so native ranges still do not emit it to BE.
        long selfSplitWeight = length + (deletionFile != null ? deletionFile.length() : 0);
        PaimonScanRange.Builder builder = new PaimonScanRange.Builder()
                .path(normalizeUri(file.path(), vendedToken))
                .start(start)
                .length(length)
                .fileSize(file.length())
                .fileFormat(fileFormat)
                .partitionValues(partitionValues)
                .selfSplitWeight(selfSplitWeight)
                .targetSplitSize(weightDenominator)
                .schemaId(file.schemaId())
                .bucket(bucket);
        if (deletionFile != null) {
            builder.deletionFile(
                    normalizeUri(deletionFile.path(), vendedToken),
                    deletionFile.offset(), deletionFile.length());
        }
        return builder.build();
    }

    /**
     * Builds the native sub-range(s) for one raw ORC/Parquet file (FIX-NATIVE-SUBSPLIT): slices it at
     * {@code targetSplitSize} via {@link #computeFileSplitOffsets} and emits one {@link PaimonScanRange}
     * per {@code [start, length)} sub-range. The SAME per-file deletion vector is attached to EVERY
     * sub-range — BE indexes the DV by GLOBAL file row position, so disjoint sub-ranges share the
     * unmodified deletion file (no offset re-basing); attaching it to only some sub-ranges would let
     * deleted rows reappear in the others (merge-on-read corruption). A non-positive
     * {@code targetSplitSize} yields a single whole-file range (used under COUNT(*) pushdown, where
     * legacy keeps the split whole via {@code splittable=!applyCountPushdown}). Package-private so the
     * DV-on-every-sub-range invariant is unit-testable without a live DV-bearing split.
     */
    List<PaimonScanRange> buildNativeRanges(RawFile file, DeletionFile deletionFile,
            String defaultFileFormat, Map<String, String> partitionValues,
            Map<String, String> vendedToken, long targetSplitSize, long weightDenominator,
            int bucket) {
        List<PaimonScanRange> result = new ArrayList<>();
        for (long[] offset : computeFileSplitOffsets(file.length(), targetSplitSize)) {
            result.add(buildNativeRange(file, deletionFile, defaultFileFormat,
                    partitionValues, vendedToken, offset[0], offset[1], weightDenominator, bucket));
        }
        return result;
    }

    /**
     * Normalizes a raw paimon-SDK storage URI (native data-file or deletion-vector path) into BE's
     * canonical scheme via the engine ({@code oss://}/{@code cos://}/{@code obs://}/{@code s3a://}
     * &rarr; {@code s3://}; OSS {@code bucket.endpoint} &rarr; {@code bucket}). Ports legacy
     * {@code PaimonScanNode}'s 2-arg {@code LocationPath.of(path, storagePropertiesMap)} — BE's S3
     * file factory only recognizes {@code s3://}, so an un-normalized OSS/COS/OBS path fails the
     * native read (data file) or silently drops the deletion vector (merge-on-read wrong rows). The
     * connector cannot import fe-core's {@code LocationPath}, so it delegates to the
     * {@link ConnectorStorageContext#normalizeStorageUri(String, Map)} seam, passing the per-table
     * {@code vendedToken} (empty for non-REST) so a REST object-store path normalizes via the vended
     * credentials — the catalog's static storage map is empty for REST, so the static-only path would
     * throw (FIX-REST-VENDED-URI-NORMALIZE). With no context (offline unit tests) the raw path is
     * preserved — same null-guard as the {@code vendStorageCredentials} overlay below.
     */
    private String normalizeUri(String rawUri, Map<String, String> vendedToken) {
        return context != null ? storage().normalizeStorageUri(rawUri, vendedToken) : rawUri;
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {

        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table = resolveScanTable(paimonHandle);

        Map<String, String> props = new LinkedHashMap<>();

        // File format type (default)
        props.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "jni");
        props.put("table_format_type", "paimon");

        // Path partition keys: declare the partition columns at the scan-node level so
        // FileQueryScanNode excludes them from the file/decode column set (num_of_columns_from_file +
        // classifyColumn -> PARTITION_KEY). Paimon physically stores partition columns IN the data
        // file, and the per-split PaimonScanRange.populateRangeParams already emits them as
        // columnsFromPath; without this declaration the BE both DECODES dt/hh from the ORC file AND
        // APPENDS them from columnsFromPath -> a row-count double-fill that trips the OrcReader DCHECK
        // (block rows != partition col rows). Case-preserved to match the Doris column names and the
        // columnsFromPath keys (getPartitionInfoMap). Restores legacy PaimonScanNode.getPathPartitionKeys
        // parity (and mirrors the hive connector). PluginDrivenScanNode.getPathPartitionKeys reads this.
        List<String> partitionKeys = table.partitionKeys();
        if (partitionKeys != null && !partitionKeys.isEmpty()) {
            props.put(ScanNodePropertyKeys.PATH_PARTITION_KEYS, String.join(",", partitionKeys));
        }

        // Serialized table for BE's JNI reader, stripped of its catalog loader (see
        // tableForBackend) so the BE never has to deserialize the Hive metastore stack.
        Table backendTable = tableForBackend(paimonHandle, table);
        String serializedTable = encodeObjectToString(backendTable);
        props.put(PROP_SERIALIZED_TABLE, serializedTable);
        props.put(PROP_SERIALIZED_TABLE_CACHE_KEY, UUID.randomUUID().toString());
        OptionalInt backendManifestCap = backendManifestParallelism(paimonHandle, table);

        // Serialized predicates for BE's JNI scanner. ALWAYS emit, even for the no-filter / empty-predicate
        // case: an empty list still serializes to a non-null base64 string, and PaimonJniScanner.getPredicates()
        // deserializes this param UNCONDITIONALLY — omitting it makes the JNI reader NPE on deserialize(null)
        // ("encodedStr is null"). Mirrors legacy PaimonScanNode.createScanRangeLocations, which always called
        // setPaimonPredicate(encodeObjectToString(predicates)) regardless of whether predicates was empty.
        List<org.apache.paimon.predicate.Predicate> predicates = Collections.emptyList();
        if (filter.isPresent()) {
            RowType rowType = table.rowType();
            PaimonPredicateConverter converter = new PaimonPredicateConverter(rowType);
            predicates = converter.convert(filter.get());
        }
        props.put("paimon.predicate", encodeObjectToString(predicates));

        // Paimon JDBC metastore options for BE (if applicable)
        Map<String, String> backendOptions = new LinkedHashMap<>(getBackendPaimonOptions());
        backendManifestCap.ifPresent(cap -> backendOptions.put(
                DORIS_MANIFEST_PARALLELISM_CAP, String.valueOf(cap)));
        if (paimonHandle.isSystemTable() && backendManifestCap.isPresent()) {
            Table source = paimonHandle.getSystemTableSource();
            if (source == null) {
                source = paimonHandle.getSysBaseTable();
            }
            Table effectiveSource = source == null ? null
                    : PaimonReaderOptions.runtimeSafeSystemSource(
                            source, paimonHandle.getScanOptions());
            if (effectiveSource instanceof FileStoreTable) {
                // A system wrapper can hide its physical option map. Ship the exact catalog-less
                // source so a smaller BE can cap it and rebuild without reopening catalog state.
                backendOptions.put(DORIS_SERIALIZED_SYSTEM_SOURCE,
                        encodeObjectToString(dropCatalogLoader((FileStoreTable) effectiveSource)));
                backendOptions.put(DORIS_SYSTEM_TABLE_TYPE, paimonHandle.getSysTableName());
            }
        }
        if (!backendOptions.isEmpty()) {
            // Encode as JSON for transport
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, String> entry : backendOptions.entrySet()) {
                if (!first) {
                    sb.append(",");
                }
                sb.append("\"").append(escapeJson(entry.getKey()))
                        .append("\":\"").append(escapeJson(entry.getValue())).append("\"");
                first = false;
            }
            sb.append("}");
            props.put("paimon.options_json", sb.toString());
        }

        // FIX-STATIC-CREDS-BE (B-9): static catalog-level storage credentials/config, normalized to
        // BE-canonical keys (AWS_* for object stores). BE's native (FILE_S3) reader understands ONLY the
        // canonical keys, so the raw catalog aliases (s3.access_key, oss.access_key, …) must be translated
        // before they leave FE — copying them verbatim gives the native reader no usable creds (403 on a
        // private bucket). Sourced from the typed fe-filesystem StorageProperties bound by fe-core and
        // handed over via storage().getStorageProperties() (P1-T04): each backend's toBackendProperties().toMap()
        // yields the canonical map (e.g. S3FileSystemProperties IS-A BackendStorageProperties → AWS_*).
        // This replaces the legacy getBackendStorageProperties() seam so the connector derives BOTH its
        // Hadoop config (P1-T03) and its BE creds from the SAME typed source (design D-003). Empty when no
        // context (offline unit tests) → no storage props emitted (never the broken raw aliases).
        //
        // HDFS (DV-004 / R-007 — CLOSED by FU-T01): fe-filesystem now has a typed HDFS BE model
        // (HdfsFileSystemProperties); HdfsFileSystemProvider.bind() yields it, so an HDFS-warehouse catalog
        // emits the hadoop/dfs/HA/kerberos keys here (→ THdfsParams) at parity with the legacy path
        // (hadoop.config.resources resolved under the operator-configured Config.hadoop_config_dir).
        // KNOWN GAP 2 (R-008): the typed OSS/COS/OBS models omit AWS_CREDENTIALS_PROVIDER_TYPE, which legacy
        // emitted as ANONYMOUS for credential-less catalogs — a fe-filesystem parity gap (out of P1 whitelist),
        // tracked as a follow-up; only affects OSS/COS/OBS catalogs with no static ak/sk.
        if (context != null) {
            Map<String, String> backendStorageProps = new HashMap<>();
            for (StorageProperties sp : storage().getStorageProperties()) {
                sp.toBackendProperties().ifPresent(b -> backendStorageProps.putAll(b.toMap()));
            }
            for (Map.Entry<String, String> e : backendStorageProps.entrySet()) {
                props.put(ScanNodePropertyKeys.LOCATION_PREFIX + e.getKey(), e.getValue());
            }
        }

        // FIX-REST-VENDED: overlay per-table vended cloud-storage credentials (REST catalogs).
        // The raw token is extracted from the live, snapshot-pinned table's RESTTokenFileIO (paimon
        // SDK only), then normalized to BE-facing AWS_* keys by the engine (the connector cannot
        // import fe-core's StorageProperties). Vended overlays static (legacy precedence). Skipped
        // when no context (offline unit tests) or the table is non-REST (empty token -> no-op).
        if (context != null) {
            Map<String, String> vendedBeProps = storage().vendStorageCredentials(extractVendedToken(table));
            for (Map.Entry<String, String> e : vendedBeProps.entrySet()) {
                props.put(ScanNodePropertyKeys.LOCATION_PREFIX + e.getKey(), e.getValue());
            }
        }

        // FIX-SCHEMA-EVOLUTION (B-1a): emit the native-reader schema dictionary so BE matches file<->table
        // columns BY FIELD ID across schema evolution (rename/reorder) instead of falling back to NAME
        // matching (which silently reads NULL/garbage for renamed columns). Only meaningful when the table
        // can take the native path: handle-level force is semantic and unconditional, while a Variant
        // projection may override only the session debugging knob because JNI has no Variant carrier.
        boolean hasVariantProjection = projectsVariant(table.rowType(), columns);
        if (!paimonHandle.isForceJni()
                && (hasVariantProjection || !isForceJniScannerEnabled(session))) {
            // The schema dict must be built from a FileStoreTable. A normal data table IS one; a $ro
            // (read-optimized) system table is a ReadOptimizedTable that WRAPS a FileStoreTable and reads
            // its data files with its field ids, so resolve the underlying base FileStoreTable here.
            Table schemaDictTable = resolveSchemaDictTable(table, paimonHandle);
            if (schemaDictTable != null) {
                buildSchemaEvolutionParam(paimonHandle, schemaDictTable, columns)
                        .ifPresent(v -> props.put(SCHEMA_EVOLUTION_PROP, v));
            }
        }

        return props;
    }

    OptionalInt backendManifestParallelism(PaimonTableHandle handle, Table scanTable) {
        Table planningTable = scanTable;
        if (handle.isSystemTable()) {
            Table source = handle.getSystemTableSource();
            if (source == null) {
                source = handle.getSysBaseTable();
            }
            if (source != null) {
                // System wrappers hide their manifest planner, so send its FE-safe value out of
                // band; a smaller BE can lower the same hidden planner after deserialization.
                planningTable = PaimonReaderOptions.runtimeSafeSystemSource(
                        source, handle.getScanOptions());
            }
        }
        return PaimonReaderOptions.backendManifestParallelismCap(planningTable);
    }

    /**
     * Build the Paimon table object that is serialized to the BE.
     *
     * <p>Every table loaded from a metastore-backed Paimon catalog (HMS / DLF) carries a Paimon
     * {@code HiveCatalogLoader} in its {@link CatalogEnvironment}. The BE only reads — via
     * FE-resolved splits and the object store — and never needs the catalog, yet deserializing that
     * loader forces the whole Hive metastore stack onto the BE classpath: {@code HiveConf}, the
     * metastore API, and, when a system table resolves its latest snapshot, even the metastore
     * client (DLF's {@code ProxyMetaStoreClient} and its REST stack). So we serialize a catalog-less
     * table to the BE:
     * <ul>
     *   <li>data table: drop the catalog loader. A {@link FileStoreTable} is fully defined by
     *       fileIO / location / schema / catalogEnvironment, and its dynamic options (time travel,
     *       incremental) are merged into the schema by {@code copy(...)} — which
     *       {@link #resolveScanTable} has already applied — so rebuilding from
     *       fileIO / location / schema preserves everything except the catalog loader.</li>
     *   <li>system table (e.g. {@code $snapshots}): rebuild it over a catalog-less data table so
     *       {@code SnapshotManager#latestSnapshotId} lists the snapshot directory on the filesystem
     *       instead of calling the metastore. The base table is the one the FE-side wrapper was
     *       built over ({@link PaimonTableHandle#getSysBaseTable()}), and for the system tables that
     *       pick their snapshot on the BE ({@link PaimonScanParams#resolvesSnapshotOnBackend}) what
     *       the catalog would have done there is done here instead: see
     *       {@link #authorizeDeferredScan} and {@link #pinCatalogSnapshot}. Every other system table
     *       reads what the FE already planned, so it is handed over untouched. The relation-scoped
     *       scan params {@link #resolveScanTable} applied to the original wrapper are re-applied to
     *       the rebuilt one by {@link #reapplyScanParams}.</li>
     * </ul>
     */
    // Package-private for direct unit testing (PaimonBackendBoundTableTest).
    Table tableForBackend(PaimonTableHandle handle, Table scanTable) {
        if (scanTable instanceof FileStoreTable) {
            // resolveScanTable's copy(...) merged the relation's dynamic options into the schema,
            // and the rebuild below goes through that schema, so this branch needs no re-application.
            return dropCatalogLoader((FileStoreTable) scanTable);
        }
        if (!handle.isSystemTable()) {
            return scanTable;
        }
        // The very same base table the FE-side wrapper was built over, so that the BE never sees a
        // different schema generation than the one this query was planned with.
        FileStoreTable dataTable = handle.getSysBaseTable();
        if (dataTable == null) {
            return scanTable;
        }
        String sysTableType = handle.getSysTableName();
        boolean resolvesOnBackend = PaimonScanParams.resolvesSnapshotOnBackend(sysTableType);
        if (PAIMON_FILES_SYSTEM_TABLE.equalsIgnoreCase(sysTableType)) {
            authorizeDeferredScan(dataTable);
        }
        FileStoreTable preparedDataTable = dataTable;
        if (resolvesOnBackend) {
            preparedDataTable = pinCatalogSnapshot(preparedDataTable, dataTable);
        }
        Map<String, String> scanOptions = handle.getScanOptions();
        boolean optionsAppliedToSource = PaimonScanParams.isOptionsPin(scanOptions);
        if (optionsAppliedToSource) {
            // Fallback snapshot translation consults each branch catalog, so options must be
            // resolved while both loaders are still present and only then made BE-safe.
            preparedDataTable = (FileStoreTable) PaimonScanParams.applyOptions(
                    preparedDataTable, scanOptions);
        }
        FileStoreTable baseForBackend = dropCatalogLoader(preparedDataTable);
        Table catalogLessSysTable = SystemTableLoader.load(sysTableType, baseForBackend);
        if (catalogLessSysTable == null) {
            return scanTable;
        }
        return reapplyScanParams(catalogLessSysTable, dataTable, resolvesOnBackend,
                optionsAppliedToSource ? Collections.emptyMap() : scanOptions);
    }

    /**
     * Re-apply the relation-scoped scan params to a rebuilt system-table wrapper.
     *
     * <p>{@link #resolveScanTable} applies {@code @incr} / {@code @options} to the wrapper the
     * handle carries, and every Paimon system table delegates {@code copy(...)} to the data table it
     * wraps. The wrapper rebuilt above is a different object, so the same copy has to be redone on
     * it, otherwise the BE would materialize its splits against the unpinned latest state. The
     * branches mirror {@link #resolveScanTable}'s exactly — one chokepoint's worth of logic applied
     * to two different objects.
     *
     * <p>This runs last on purpose: an explicit relation option outranks anything this class pins on
     * the rebuilt table, and {@code copy(...)} lets the option win. An incremental relation outranks
     * {@link #pinCatalogSnapshot} the same way but cannot inherit its bound, so it is bound to the
     * catalog's snapshot separately by {@link PaimonIncrementalScanParams#bindRangeToCatalog}.
     *
     * <p>The {@code @options} family is applied to the catalog-backed source before this method is
     * reached. That ordering is required for fallback tables because Paimon's snapshot translation
     * must consult each branch's catalog-visible pointer before the loaders are removed.
     */
    private Table reapplyScanParams(Table rebuiltSysTable, FileStoreTable dataTable,
            boolean resolvesOnBackend, Map<String, String> scanOptions) {
        if (scanOptions == null || scanOptions.isEmpty()) {
            return rebuiltSysTable;
        }
        if (PaimonScanParams.isOptionsPin(scanOptions)) {
            return PaimonScanParams.applyOptions(rebuiltSysTable, scanOptions);
        }
        Map<String, String> params = resolvesOnBackend
                ? PaimonIncrementalScanParams.bindRangeToCatalog(scanOptions, dataTable)
                : scanOptions;
        return rebuiltSysTable.copy(PaimonIncrementalScanParams.applyResetsIfIncremental(params));
    }

    /**
     * {@code $files} plans only partition-level splits on the FE and re-plans the base table on the
     * BE through {@code DataTableScan#plan()} ({@code FilesTable.FilesRead#createReader}). That
     * deferred plan normally authorizes itself through the catalog loader
     * ({@code CatalogEnvironment#tableQueryAuth} -&gt; {@code Catalog#authTableQuery}); once the
     * loader is dropped it silently allows everything. So authorize here, while the loader is still
     * around. Paimon discards the predicates the call returns (row level access control is a TODO in
     * {@code AbstractDataTableScan#authQuery}), so running it on the FE loses nothing.
     *
     * <p>Only {@code $files} may do this: {@code auth(null)} means "every column" to
     * {@code Catalog#authTableQuery}, and the system tables that keep planning on the FE
     * ({@code $ro}, {@code $row_tracking}, {@code $audit_log}, {@code $binlog}) already authorize
     * themselves through {@code DataTableBatchScan} with the slot projection the query really reads.
     * Authorizing those again for every column would reject a user allowed to read only some of the
     * base columns. {@code $partitions} never reaches {@code plan()} on either side
     * ({@code listPartitionEntries} does not authorize), so it has no authorization to transfer.
     *
     * <p>A {@code scan.fallback-branch} table has to be authorized branch by branch.
     * {@code FallbackReadFileStoreTable#newScan} builds a {@code FallbackReadScan} over both
     * branches' own scans, so each authorizes itself, and {@code FileStoreTableFactory#create} gives
     * the fallback branch a {@link CatalogEnvironment} of its own carrying a branch-qualified
     * {@code Identifier}. Since the pair delegates {@code catalogEnvironment()} to its main branch,
     * authorizing the pair would check the main branch alone and never the fallback one - and once
     * its loader is dropped that missing check turns into a permanent allow, letting a user denied on
     * the fallback branch read the fallback rows of {@code $files}.
     */
    // Package-private for direct unit testing (PaimonBackendBoundTableTest).
    static void authorizeDeferredScan(FileStoreTable dataTable) {
        FileStoreTable undecorated = PaimonTableDecorators.unwrapToFallbackOrBase(dataTable);
        if (undecorated instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable fallbackReadTable = (FallbackReadFileStoreTable) undecorated;
            authorizeBranch(fallbackReadTable.wrapped());
            authorizeBranch(fallbackReadTable.fallback());
            return;
        }
        authorizeBranch(undecorated);
    }

    private static void authorizeBranch(FileStoreTable branch) {
        CoreOptions options = branch.coreOptions();
        if (options.queryAuthEnabled()) {
            branch.catalogEnvironment().tableQueryAuth(options).auth(null);
        }
    }

    /**
     * For catalogs that manage versions themselves (Paimon REST / DLF REST) the committed snapshot
     * is the one the catalog points at, not the newest file in the snapshot directory: Paimon
     * publishes the snapshot file before the pointer moves, and a rollback leaves newer files
     * behind. Without the catalog loader {@code SnapshotManager} falls back to listing that
     * directory, so the BE could plan on a snapshot the catalog has not published while the FE
     * planned on the previous one. Pin the catalog-visible snapshot instead.
     *
     * <p>Only for {@link PaimonScanParams#resolvesSnapshotOnBackend} system tables, because only
     * those pick their snapshot inside the BE reader. Every other system table materializes the
     * splits the FE already planned, so pinning them would bind nothing that is not already bound.
     * The pin goes through {@code copyWithoutTimeTravel}, so it bounds which snapshot the BE plans
     * on without rewinding the BE's schema to that snapshot.
     *
     * <p>Known gap: {@code scan.snapshot-id} only bounds plans that go through
     * {@code DataTableScan}. {@code $snapshots} ({@code SnapshotManager#snapshotsWithinRange}) and
     * {@code $buckets} ({@code SnapshotReader#bucketEntries}) ignore it, so on the BE they observe
     * the snapshot directory rather than the catalog's pointer. Both are read-only metadata tables,
     * so this shows up only as a transient extra row inside the publication window of a
     * version-managed catalog.
     *
     * <p>Known gap: {@code $files} is planned in two phases and only the second one can be pinned.
     * The FE emits one marker split per partition of {@code SnapshotManager#latestSnapshot()} at
     * split generation time ({@code FilesTable.FilesScan#innerPlan} -&gt;
     * {@code SnapshotReader#partitions}), and that path reads {@code ManifestsReader#read(null, ..)},
     * which ignores {@code scan.snapshot-id} - so the marker set cannot be bound to the pin, and
     * {@code FilesSplit} is private to {@code FilesTable}, so Doris cannot emit a snapshot
     * independent marker either. A commit landing between initialization and split generation that
     * drops a partition therefore hides that partition's rows even though the BE stays on the older
     * snapshot. Paimon's two-phase plan has this gap regardless: before the pin the BE resolved the
     * latest snapshot at read time, i.e. across a strictly wider window, so this only moves which
     * side of the window the mismatch falls on.
     *
     * <p>A fallback pair is pinned branch by branch. Its branches may expose different committed
     * pointers, and pinning the pair through its delegated main catalog would otherwise translate
     * the fallback id against uncommitted snapshot files after that branch loader is removed.
     */
    // Package-private for direct unit testing (PaimonBackendBoundTableTest).
    static FileStoreTable pinCatalogSnapshot(FileStoreTable catalogLessTable, FileStoreTable dataTable) {
        FileStoreTable source = PaimonTableDecorators.unwrapToFallbackOrBase(dataTable);
        FileStoreTable target = PaimonTableDecorators.unwrapToFallbackOrBase(catalogLessTable);
        if (source instanceof FallbackReadFileStoreTable) {
            if (!(target instanceof FallbackReadFileStoreTable)) {
                throw new IllegalStateException("Catalog-less Paimon table lost its fallback branch");
            }
            FallbackReadFileStoreTable sourcePair = (FallbackReadFileStoreTable) source;
            FallbackReadFileStoreTable targetPair = (FallbackReadFileStoreTable) target;
            return new FallbackReadFileStoreTable(
                    pinCatalogSnapshotBranch(targetPair.wrapped(), sourcePair.wrapped()),
                    pinCatalogSnapshotBranch(targetPair.fallback(), sourcePair.fallback()));
        }
        return pinCatalogSnapshotBranch(target, source);
    }

    private static FileStoreTable pinCatalogSnapshotBranch(
            FileStoreTable target, FileStoreTable source) {
        if (!source.catalogEnvironment().supportsVersionManagement()) {
            return target;
        }
        Long snapshotId = source.snapshotManager().latestSnapshotId();
        if (snapshotId == null) {
            return target;
        }
        // Without time travel: pin which snapshot the BE plans on, leave schema resolution alone.
        return target.copyWithoutTimeTravel(
                Collections.singletonMap(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId)));
    }

    /**
     * Return an equivalent {@link FileStoreTable} without the catalog loader, so the BE never
     * deserializes a {@code HiveCatalogLoader} (and snapshot loading uses the filesystem instead of
     * the catalog's metastore). fileIO / location / schema (and the schema's options) are preserved.
     *
     * <p>A {@code scan.fallback-branch} table must be rebuilt branch by branch.
     * {@link FallbackReadFileStoreTable} exposes only its main branch through {@code schema()}, and
     * the plain {@code FileStoreTableFactory#create} re-expands the fallback branch from
     * {@code SchemaManager(fallbackBranch).latest()} instead of the object the FE captured. That
     * would ship a main/fallback pair from two different generations: after external DDL publishes
     * a new schema on both branches, the FE keeps planning on the cached M1/F1 while the BE gets
     * M1/F2 and fails in {@code FallbackReadFileStoreTable#validateSchema}. So rebuild each branch
     * from the schema the FE really planned with, and re-wrap.
     */
    // Package-private for direct unit testing (PaimonBackendBoundTableTest).
    static FileStoreTable dropCatalogLoader(FileStoreTable dataTable) {
        if (dataTable.catalogEnvironment().catalogLoader() == null) {
            return dataTable;
        }
        FileStoreTable undecorated = PaimonTableDecorators.unwrapToFallbackOrBase(dataTable);
        if (undecorated instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable fallbackReadTable = (FallbackReadFileStoreTable) undecorated;
            return new FallbackReadFileStoreTable(
                    rebuildWithoutCatalogLoader(fallbackReadTable.wrapped()),
                    rebuildWithoutCatalogLoader(fallbackReadTable.fallback()));
        }
        return rebuildWithoutCatalogLoader(undecorated);
    }

    private static FileStoreTable rebuildWithoutCatalogLoader(FileStoreTable branch) {
        return FileStoreTableFactory.createWithoutFallbackBranch(
                branch.fileIO(), branch.location(), branch.schema(), new Options(),
                CatalogEnvironment.empty());
    }

    /**
     * Resolves the {@link FileStoreTable} whose schema dictionary BE needs to field-id-match the native
     * data files for {@code table}. A normal data table IS the FileStoreTable. A read-optimized system
     * table ({@code $ro} &rarr; {@link ReadOptimizedTable}) is NOT a {@code FileStoreTable} (it wraps one)
     * but reads the BASE table's data files with the BASE field ids, so its dict must come from the base
     * FileStoreTable, reloaded here via the 2-arg base {@link Identifier}.
     *
     * <p>Restores legacy {@code PaimonScanNode} parity: legacy set {@code history_schema_info} for ANY
     * paimon table (incl. {@code $ro}) in {@code doInitialize}, so BE always took the field-id path. The
     * SPI connector had gated the dict on {@code instanceof FileStoreTable} and so emitted nothing for
     * {@code $ro}; with no {@code history_schema_info} BE's {@code gen_table_info_node_by_field_id} fell
     * into the legacy name-matching branch {@code by_parquet_name(tuple_descriptor, ...)} and dereferenced
     * a still-null tuple descriptor ({@code table_schema_change_helper.cpp:94}) &rarr; a SIGSEGV that
     * aborted the whole BE.
     *
     * <p>Returns {@code null} for a table with no native data files (metadata system tables take the JNI
     * path and never consult the dict), preserving the prior "emit nothing" behavior for those.
     */
    private Table resolveSchemaDictTable(Table table, PaimonTableHandle handle) {
        if (table instanceof FileStoreTable) {
            return table;
        }
        if (table instanceof ReadOptimizedTable) {
            FileStoreTable pinnedSource = handle.getSysBaseTable();
            if (pinnedSource != null) {
                // $ro reads the field ids of its embedded source; a catalog reload here can observe
                // schema generation B while the wrapper still plans generation A's files. Relation scan
                // options must also select this source, or historical splits get the latest dictionary.
                return reapplyScanParams(
                        pinnedSource, pinnedSource, false, handle.getScanOptions());
            }
            return reloadBaseTable(handle);
        }
        return null;
    }

    /**
     * Reloads the BASE data table for a system handle via the 2-arg base {@link Identifier}, under the
     * FE-injected authenticator (D-052) when a context is present — mirroring {@link #resolveTable}'s
     * reload. Used to obtain the underlying {@link FileStoreTable} of a {@code $ro} read so its schema
     * dictionary can be emitted.
     */
    private Table reloadBaseTable(PaimonTableHandle handle) {
        Identifier baseId = Identifier.create(handle.getDatabaseName(), handle.getTableName());
        try {
            if (context == null) {
                return catalogOps.getTable(baseId);
            }
            return context.executeAuthenticated(() -> catalogOps.getTable(baseId));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Paimon base table for schema dict: " + baseId, e);
        }
    }

    /**
     * Extracts the raw per-table vended credential token from a REST catalog table's
     * {@link RESTTokenFileIO} (port of legacy {@code PaimonVendedCredentialsProvider
     * .extractRawVendedCredentials}, paimon SDK only). Returns empty for a non-REST table (different
     * FileIO) or when no valid token is available — the gate is the table's FileIO type, equivalent
     * to legacy's "metastore is REST" check for the read path.
     */
    static Map<String, String> extractVendedToken(Table table) {
        if (table == null) {
            return Collections.emptyMap();
        }
        FileIO fileIO = table.fileIO();
        if (!(fileIO instanceof RESTTokenFileIO)) {
            return Collections.emptyMap();
        }
        RESTToken token = ((RESTTokenFileIO) fileIO).validToken();
        Map<String, String> raw = token == null ? null : token.token();
        return raw == null ? Collections.emptyMap() : new HashMap<>(raw);
    }

    private PaimonScanRange buildJniScanRange(Split split, String defaultFileFormat,
            Map<String, String> partitionValues, boolean isDataSplit, long weightDenominator) {
        long splitWeight = 0;
        if (isDataSplit) {
            splitWeight = computeSplitWeight((DataSplit) split);
        } else {
            splitWeight = split.rowCount();
        }

        String serializedSplit = encodeSplit(split);

        // FIX-JNI-FILE-FORMAT (P7-1) + FIX-L11: emit the real data-file format (orc/parquet/avro), NOT "jni".
        // JNI routing is gated by the paimon.split property (PaimonScanRange.populateRangeParams), so this
        // string only feeds fileDesc.file_format, which BE's paimon_cpp_reader backfills into
        // FILE_FORMAT/MANIFEST_FORMAT (an invalid "jni" breaks the manifest read). Mirrors legacy
        // PaimonScanNode.setPaimonParams's fileDesc.setFileFormat(getFileFormat(getPathString())): for a
        // DataSplit the format is the FIRST data-file suffix (falling back to the table default); a
        // non-DataSplit has no data file and falls back to the table default (legacy DUMMY_PATH -> orElse).
        String fileFormat = isDataSplit
                ? dataSplitFileFormat((DataSplit) split, defaultFileFormat)
                : defaultFileFormat;
        PaimonScanRange.Builder builder = new PaimonScanRange.Builder()
                .fileFormat(fileFormat)
                .paimonSplit(serializedSplit)
                .partitionValues(partitionValues)
                .selfSplitWeight(splitWeight)
                .targetSplitSize(weightDenominator);
        if (isDataSplit) {
            // Same bucket property as the native arm: which reader BE ends up using must not change
            // what a sibling connector can learn about the split (see PaimonScanRange's props).
            builder.bucket(((DataSplit) split).bucket());
        }
        return builder.build();
    }

    /**
     * Whether a {@link DataSplit} contributes a precomputed COUNT(*)-pushdown row count: true iff count
     * pushdown is active for this scan AND the split's merged (post-merge / post-deletion-vector) row
     * count is precomputed by the paimon SDK. Mirrors legacy {@code PaimonScanNode}'s count gate
     * ({@code applyCountPushdown && dataSplit.mergedRowCountAvailable()}, the FIRST routing arm).
     * Extracted as a pure static so the correctness-critical count routing decision is unit-testable
     * with a real {@link DataSplit}, like {@link #shouldUseNativeReader}.
     */
    static boolean isCountPushdownSplit(boolean countPushdown, DataSplit dataSplit) {
        return countPushdown && dataSplit.mergedRowCountAvailable();
    }

    /**
     * Builds the single collapsed COUNT(*)-pushdown range: a JNI-serialized {@link DataSplit} (legacy
     * {@code new PaimonSplit(dataSplit)}) carrying the summed merged row count via {@code paimon.row_count}
     * &rarr; BE's {@code table_level_row_count} &rarr; {@code CountReader}, so BE emits the count without
     * reading data. Uses the same Java-object split serialization as {@link #buildJniScanRange}.
     */
    private PaimonScanRange buildCountRange(DataSplit dataSplit, String defaultFileFormat,
            Map<String, String> partitionValues, long rowCount, long weightDenominator) {
        String serializedSplit = encodeSplit(dataSplit);
        // FIX-JNI-FILE-FORMAT (P7-1) + FIX-L11: real data-file format from the first data-file suffix, not
        // "jni" and not the bare table default (see buildJniScanRange / dataSplitFileFormat).
        return new PaimonScanRange.Builder()
                .fileFormat(dataSplitFileFormat(dataSplit, defaultFileFormat))
                .paimonSplit(serializedSplit)
                .partitionValues(partitionValues)
                .selfSplitWeight(computeSplitWeight(dataSplit))
                .targetSplitSize(weightDenominator)
                .rowCount(rowCount)
                .build();
    }

    /**
     * Slices a native data file into {@code [start, length]} sub-ranges for read parallelism
     * (FIX-NATIVE-SUBSPLIT), porting the specified-size branch of legacy {@code FileSplitter.splitFile}
     * (the connector has no block locations, so the block-based branch is never reached). Byte-identical
     * to {@code FileSplitter.java:129-144}, including the
     * <b>{@code > 1.1D} tail guard</b> — the LAST range absorbs a remainder of up to 1.1&times; the
     * target instead of emitting a tiny tail split (a naive {@code ceilDiv} would differ). The ranges
     * tile {@code [0, fileLength)} contiguously with no gap/overlap. A zero/negative file length yields
     * no range (legacy skips empty files); a non-positive target yields a single whole-file range —
     * used under COUNT(*) pushdown (see {@link #buildNativeRanges}, where legacy keeps the split whole
     * via {@code splittable=!applyCountPushdown}); {@link #determineTargetSplitSize} otherwise never
     * returns &le; 0. Pure static so the offset math is unit-testable against the fe-core source it ports.
     */
    static List<long[]> computeFileSplitOffsets(long fileLength, long targetSplitSize) {
        List<long[]> result = new ArrayList<>();
        if (fileLength <= 0) {
            return result;
        }
        if (targetSplitSize <= 0) {
            result.add(new long[] {0L, fileLength});
            return result;
        }
        long bytesRemaining;
        for (bytesRemaining = fileLength;
                (double) bytesRemaining / (double) targetSplitSize > 1.1D;
                bytesRemaining -= targetSplitSize) {
            result.add(new long[] {fileLength - bytesRemaining, targetSplitSize});
        }
        if (bytesRemaining != 0L) {
            result.add(new long[] {fileLength - bytesRemaining, bytesRemaining});
        }
        return result;
    }

    /**
     * Computes the native target file split size, porting legacy
     * {@code PaimonScanNode.determineTargetFileSplitSize} + {@code FileQueryScanNode.applyMaxFileSplitNumLimit}
     * with plain longs (the connector cannot import {@code SessionVariable}). The legacy
     * {@code isBatchMode -> 0} branch is omitted: paimon is never batch-mode on the plugin path. Pure
     * static so the heuristic is unit-testable.
     */
    static long determineTargetSplitSize(long fileSplitSize, long maxInitialSplitSize, long maxSplitSize,
            long maxInitialSplitNum, long maxFileSplitNum, long totalNativeFileSize) {
        if (fileSplitSize > 0) {
            return fileSplitSize;
        }
        long result = (totalNativeFileSize >= maxSplitSize * maxInitialSplitNum)
                ? maxSplitSize : maxInitialSplitSize;
        if (maxFileSplitNum > 0 && totalNativeFileSize > 0) {
            long minSplitSizeForMaxNum = (totalNativeFileSize + maxFileSplitNum - 1L) / maxFileSplitNum;
            result = Math.max(result, minSplitSizeForMaxNum);
        }
        return result;
    }

    /**
     * Reads the 5 file-split session vars (VariableMgr.toMap channel) and sums the native-eligible
     * file sizes across {@code dataSplits}, then delegates to the pure-static
     * {@link #determineTargetSplitSize}. Mirrors legacy {@code determineTargetFileSplitSize}'s
     * once-per-scan computation (summing every {@code supportNativeReader}-eligible RawFile, like
     * {@code PaimonScanNode.java:552-564}).
     */
    private long resolveTargetSplitSize(ConnectorSession session, List<DataSplit> dataSplits) {
        long totalNativeFileSize = 0;
        for (DataSplit dataSplit : dataSplits) {
            Optional<List<RawFile>> rawFiles = dataSplit.convertToRawFiles();
            if (!supportNativeReader(rawFiles)) {
                continue;
            }
            for (RawFile file : rawFiles.get()) {
                totalNativeFileSize += file.fileSize();
            }
        }
        return determineTargetSplitSize(
                sessionLong(session, FILE_SPLIT_SIZE, 0L),
                sessionLong(session, MAX_INITIAL_FILE_SPLIT_SIZE, DEFAULT_MAX_INITIAL_FILE_SPLIT_SIZE),
                sessionLong(session, MAX_FILE_SPLIT_SIZE, DEFAULT_MAX_FILE_SPLIT_SIZE),
                sessionLong(session, MAX_INITIAL_FILE_SPLIT_NUM, DEFAULT_MAX_INITIAL_FILE_SPLIT_NUM),
                sessionLong(session, MAX_FILE_SPLIT_NUM, DEFAULT_MAX_FILE_SPLIT_NUM),
                totalNativeFileSize);
    }

    /**
     * The proportional-weight denominator (FIX-A1) = legacy scan-level {@code targetSplitSize}
     * ({@code PaimonScanNode:497-500}): {@code file_split_size} when set ({@code > 0}), else
     * {@code max_file_split_size} (default 64 MB). Exact parity with legacy
     * {@code getFileSplitSize() > 0 ? getFileSplitSize() : getMaxSplitSize()}. This is DISTINCT from
     * {@link #resolveTargetSplitSize} (the native file-splitting granularity); it is the divisor for the FE
     * {@code FileSplit} proportional split weight and is applied to EVERY split type (native / JNI / count),
     * even under COUNT(*) pushdown where the file-splitting size is 0.
     */
    static long resolveSplitWeightDenominator(ConnectorSession session) {
        long fileSplitSize = sessionLong(session, FILE_SPLIT_SIZE, 0L);
        return fileSplitSize > 0
                ? fileSplitSize
                : sessionLong(session, MAX_FILE_SPLIT_SIZE, DEFAULT_MAX_FILE_SPLIT_SIZE);
    }

    /**
     * Reads a long session var from the SPI session properties (VariableMgr.toMap channel), falling
     * back to {@code defaultValue} when absent/blank/unparseable. Mirrors the null-tolerant
     * {@link #isForceJniScannerEnabled} pattern.
     */
    private static long sessionLong(ConnectorSession session, String key, long defaultValue) {
        if (session == null) {
            return defaultValue;
        }
        String value = session.getSessionProperties().get(key);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long computeSplitWeight(DataSplit dataSplit) {
        List<DataFileMeta> metas = dataSplit.dataFiles();
        if (metas != null && !metas.isEmpty()) {
            return metas.stream().mapToLong(DataFileMeta::fileSize).sum();
        }
        return dataSplit.rowCount();
    }

    /**
     * Decides whether a {@link DataSplit} may take the native (ORC/Parquet) reader path.
     *
     * <p>The split is native-eligible iff (a) it is NOT name-forced to JNI by the handle, AND (b) it is
     * NOT session-forced to JNI via {@code force_jni_scanner}, AND (c) its raw files all support the
     * native reader (see {@link #supportNativeReader}). Mirrors legacy's three-boolean gate
     * {@code !forceJniScanner && !forceJniForSystemTable && supportNativeReader} (PaimonScanNode.getSplits).
     *
     * <p>{@code forceJni} is the T19 name-force: {@code binlog} / {@code audit_log} system tables are
     * paimon {@code DataTable}s whose {@code DataSplit.convertToRawFiles()} may succeed, but the native
     * reader cannot reproduce their read semantics (binlog pack/merge + array materialization;
     * audit_log rowkind/sequence-number projection), so they would silently return wrong rows. Legacy
     * forces them to JNI ({@code PaimonScanNode.shouldForceJniForSystemTable}, captured by
     * {@link PaimonTableHandle#isForceJni()}). It must NOT over-force: metadata sys tables already go
     * JNI via the non-DataSplit path, and a non-forced {@code DataTable} like "ro" (forceJni=false)
     * must still be allowed native.
     *
     * <p>{@code forceJniScanner} is the user/session escape hatch ({@code SET force_jni_scanner=true},
     * read via {@link #isForceJniScannerEnabled}): when set, every native-eligible non-Variant split is
     * routed to JNI to dodge native-reader bugs. Variant projections on ordinary tables stay native because
     * JNI cannot carry Variant columns, but the semantic handle-level force remains unconditional. Default
     * false, so normal reads are unaffected.
     *
     * <p>Extracted as a pure static so the correctness-critical routing decision is unit-testable
     * with real {@link RawFile}s, without driving a full Paimon {@code ReadBuilder}/{@code TableScan}.
     */
    static boolean shouldUseNativeReader(boolean forceJni, boolean forceJniScanner,
            Optional<List<RawFile>> optRawFiles) {
        return shouldUseNativeReader(forceJni, forceJniScanner, false, optRawFiles);
    }

    static boolean shouldUseNativeReader(boolean forceJni, boolean forceJniScanner,
            boolean hasVariantProjection, Optional<List<RawFile>> optRawFiles) {
        Set<Long> physicalVariantSchemaIds = hasVariantProjection && optRawFiles.isPresent()
                ? optRawFiles.get().stream().map(RawFile::schemaId).collect(Collectors.toSet())
                : Collections.emptySet();
        return shouldUseNativeReader(forceJni, forceJniScanner, hasVariantProjection,
                physicalVariantSchemaIds, optRawFiles);
    }

    static boolean shouldUseNativeReader(boolean forceJni, boolean forceJniScanner,
            boolean hasVariantProjection, Set<Long> physicalVariantSchemaIds,
            Optional<List<RawFile>> optRawFiles) {
        // Handle-level force marks system-table semantics, while only the session debugging knob may be
        // overridden for Variant. An ORC file is safe only when its historical physical schema predates
        // the projected Variant field; BE never needs to install a Variant schema override for that file.
        return !forceJni && (hasVariantProjection
                ? supportNativeVariantReader(optRawFiles, physicalVariantSchemaIds)
                : !forceJniScanner && supportNativeReader(optRawFiles));
    }

    private Set<Long> physicalVariantSchemaIds(Table table, PaimonTableHandle handle,
            RowType currentRowType, List<ConnectorColumnHandle> columns, List<DataSplit> dataSplits) {
        Set<Integer> projectedVariantFieldIds = columns.stream()
                .filter(PaimonColumnHandle.class::isInstance)
                .map(PaimonColumnHandle.class::cast)
                .map(column -> currentRowType.getFields().stream()
                        .filter(field -> field.name().equalsIgnoreCase(column.getName()))
                        .findFirst().orElse(null))
                .filter(Objects::nonNull)
                .filter(field -> containsVariant(field.type()))
                .map(DataField::id)
                .collect(Collectors.toSet());
        if (projectedVariantFieldIds.isEmpty()) {
            return Collections.emptySet();
        }

        Set<Long> rawSchemaIds = new HashSet<>();
        for (DataSplit split : dataSplits) {
            split.convertToRawFiles()
                    .ifPresent(files -> files.forEach(file -> rawSchemaIds.add(file.schemaId())));
        }
        // $ro wraps the pinned base FileStoreTable but reads that base table's schema ids. Using the
        // wrapper here would conservatively label every historical ORC schema as physical Variant.
        Table physicalSchemaTable = resolveSchemaDictTable(table, handle);
        if (!(physicalSchemaTable instanceof FileStoreTable)) {
            return rawSchemaIds;
        }

        Set<Long> physicalVariantSchemaIds = new HashSet<>();
        SchemaManager schemaManager = ((FileStoreTable) physicalSchemaTable).schemaManager();
        for (long schemaId : rawSchemaIds) {
            TableSchema physicalSchema = schemaManager.schema(schemaId);
            if (physicalSchema == null || physicalSchema.fields().stream()
                    .anyMatch(field -> projectedVariantFieldIds.contains(field.id())
                            && containsVariant(field.type()))) {
                physicalVariantSchemaIds.add(schemaId);
            }
        }
        return physicalVariantSchemaIds;
    }

    private static boolean containsVariant(DataType type) {
        switch (type.getTypeRoot()) {
            case VARIANT:
                return true;
            case ARRAY:
                return containsVariant(((ArrayType) type).getElementType());
            case MAP:
                MapType map = (MapType) type;
                return containsVariant(map.getKeyType()) || containsVariant(map.getValueType());
            case ROW:
                return ((RowType) type).getFields().stream()
                        .anyMatch(field -> containsVariant(field.type()));
            default:
                return false;
        }
    }

    private static boolean projectsVariant(
            RowType rowType, List<ConnectorColumnHandle> columns) {
        List<String> fieldNames = rowType.getFieldNames().stream()
                .map(String::toLowerCase).collect(Collectors.toList());
        return columns.stream().filter(PaimonColumnHandle.class::isInstance)
                .map(PaimonColumnHandle.class::cast)
                .mapToInt(column -> fieldNames.indexOf(column.getName().toLowerCase()))
                .filter(index -> index >= 0)
                .anyMatch(index -> containsVariant(rowType.getTypeAt(index)));
    }

    private static boolean supportNativeReader(Optional<List<RawFile>> optRawFiles) {
        if (!optRawFiles.isPresent() || optRawFiles.get().isEmpty()) {
            return false;
        }
        for (RawFile file : optRawFiles.get()) {
            String path = file.path().toLowerCase();
            if (!path.endsWith(".orc") && !path.endsWith(".parquet")) {
                return false;
            }
        }
        return true;
    }

    private static boolean supportNativeVariantReader(Optional<List<RawFile>> optRawFiles,
            Set<Long> physicalVariantSchemaIds) {
        return optRawFiles.isPresent() && !optRawFiles.get().isEmpty()
                && optRawFiles.get().stream()
                        .allMatch(file -> file.path().toLowerCase().endsWith(".parquet")
                                || (file.path().toLowerCase().endsWith(".orc")
                                && !physicalVariantSchemaIds.contains(file.schemaId())));
    }

    private Map<String, String> getPartitionInfoMap(Table table, BinaryRow partitionValue, String timeZone) {
        List<String> partitionKeys = table.partitionKeys();
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.emptyMap();
        }
        RowType partitionType = table.rowType().project(partitionKeys);
        RowDataToObjectArrayConverter converter =
                new RowDataToObjectArrayConverter(partitionType);
        Object[] values = converter.convert(partitionValue);

        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            try {
                String value = serializePartitionValue(
                        partitionType.getFields().get(i).type(), values[i], timeZone);
                result.put(partitionKeys.get(i), value);
            } catch (UnsupportedOperationException e) {
                // Legacy parity (PaimonUtil.getPartitionInfoMap): an unsupported partition column
                // type (e.g. binary/varbinary) drops the ENTIRE map — BE then materializes no
                // columnsFromPath for this split, rather than emitting non-deterministic [B@hash
                // garbage. Legacy returned null; the connector returns an empty map, which
                // PaimonScanRange.populateRangeParams treats identically (no columnsFromPath emitted).
                LOG.warn("Failed to serialize partition value for key {} of table {}: {}",
                        partitionKeys.get(i), table.name(), e.getMessage());
                return Collections.emptyMap();
            }
        }
        return result;
    }

    /**
     * Renders one Paimon partition value to the canonical string BE expects in columnsFromPath.
     * Byte-faithful port of legacy PaimonUtil.serializePartitionValue. Pure static (no Table /
     * ReadBuilder needed) so the correctness-critical per-type rendering is unit-testable offline.
     * Only TIMESTAMP_WITH_LOCAL_TIME_ZONE consumes {@code timeZone} (session zone, UTC-&gt;session
     * shift); all other cases ignore it.
     *
     * <p>For native ORC/Parquet reads, partition columns are NOT stored in the data files — BE
     * materializes them from this string. A raw {@code Object.toString()} corrupts several types:
     * DATE renders as epoch-days ("19723"), LTZ keeps the un-shifted UTC wall clock, BINARY becomes
     * a JVM-identity {@code [B@hash}. This per-type switch restores legacy correctness.
     */
    static String serializePartitionValue(DataType type, Object value, String timeZone) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case INTEGER:
            case BIGINT:
            case SMALLINT:
            case TINYINT:
            case DECIMAL:
            case VARCHAR:
            case CHAR:
                return value == null ? null : value.toString();
            case FLOAT:
                return value == null ? null : Float.toString((Float) value);
            case DOUBLE:
                return value == null ? null : Double.toString((Double) value);
            // BINARY / VARBINARY intentionally unsupported (falls to default -> throws -> map
            // dropped): a utf8 string render can corrupt the bytes (legacy comment).
            case DATE:
                return value == null ? null
                        : LocalDate.ofEpochDay((Integer) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
            case TIME_WITHOUT_TIME_ZONE:
                if (value == null) {
                    return null;
                }
                return LocalTime.ofNanoOfDay(((Long) value) * 1000)
                        .format(DateTimeFormatter.ISO_LOCAL_TIME);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return value == null ? null
                        : ((Timestamp) value).toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (value == null) {
                    return null;
                }
                return ((Timestamp) value).toLocalDateTime()
                        .atZone(ZoneId.of("UTC"))
                        .withZoneSameInstant(ZoneId.of(timeZone))
                        .toLocalDateTime()
                        .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type for serializePartitionValue: " + type);
        }
    }

    // #65332: JNI IOManager backend options. Paimon primary-key merge reads (the most common
    // filesystem/hive-metastore case) need withIOManager to spill through the Paimon IOManager;
    // BE's PaimonJniScanner enables it only when FE ships these keys. Catalog properties carry the
    // connector "paimon." prefix (e.g. properties.get("paimon.catalog.type")); the prefix is stripped
    // so BE receives jni.enable_jni_io_manager etc. (BE re-adds the paimon. prefix).
    // #65955 moved this namespace from paimon.doris.* to paimon.jni.*, on BOTH sides at once
    // (paimon_jni_reader.cpp x2 + PaimonJniScanner), and dropped jni.enable_file_reader_async along
    // with the JNI scanner's table.copy(buildTableOptions(..)) that consumed it -- the equivalent knob
    // is now the catalog-level "paimon.table-option.file-reader-async-threshold" (PaimonTableOptions).
    private static final String PAIMON_PROPERTY_PREFIX = "paimon.";
    private static final String PAIMON_BINLOG_SYSTEM_TABLE = "binlog";

    // The one system table whose deferred BE-side plan must inherit the catalog's authorization —
    // see authorizeDeferredScan.
    private static final String PAIMON_FILES_SYSTEM_TABLE = "files";

    private static final List<String> BACKEND_PAIMON_JNI_OPTIONS = Arrays.asList(
            "jni.enable_jni_io_manager",
            "jni.io_manager.tmp_dir",
            "jni.io_manager.impl_class");

    // Package-private for direct unit testing (PaimonScanPlanProviderTest).
    Map<String, String> getBackendPaimonOptions() {
        Map<String, String> options = new HashMap<>();
        // Two wildcard namespaces the holder does not model, forwarded verbatim: the paimon.jni.* knobs
        // below and the jdbc.* / warehouse / uri / metastore / catalog-key set further down.
        Map<String, String> properties = catalogProps.getRaw();
        // #65332: forward the JNI IOManager options for ALL metastore flavors (mirrors upstream
        // PaimonScanNode.getBackendPaimonOptions returning them before the jdbc-only branch), so
        // non-jdbc catalogs are no longer silently stripped of the enable flag.
        for (String option : BACKEND_PAIMON_JNI_OPTIONS) {
            String prefixed = PAIMON_PROPERTY_PREFIX + option;
            if (properties.containsKey(prefixed)) {
                options.put(option, properties.get(prefixed));
            }
        }
        if (!PaimonCatalogProperties.JDBC.equals(catalogProps.getFlavor())) {
            return options;
        }
        // Forward relevant JDBC catalog properties for BE's paimon-cpp reader
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("jdbc.") || key.equals("warehouse")
                    || key.equals("uri") || key.equals("metastore")
                    || key.equals("catalog-key")) {
                options.put(key, entry.getValue());
            }
        }
        // FIX-JDBC-DRIVER-URL (B-8a): the loop above forwards driver_url RAW and only matches the
        // "jdbc.*" form, so a bare "jdbc.driver_url=mysql.jar" reaches BE unresolved (BE does
        // new URL(value) -> MalformedURLException, JdbcDriverUtils.registerDriver) and a
        // "paimon.jdbc.driver_url" alias is dropped entirely. Emit the canonical, RESOLVED keys the
        // BE reader accepts (PaimonJdbcDriverUtils reads both aliases): honor either alias and resolve
        // a bare jar name to a full file:// URL. Mirrors legacy
        // PaimonJdbcMetaStoreProperties.getBackendPaimonOptions (getFullDriverUrl + driver_class).
        PaimonJdbcMetaStoreProperties jdbc = PaimonJdbcMetaStoreProperties.of(catalogProps.getRaw());
        if (StringUtils.isNotBlank(jdbc.getDriverUrl())) {
            options.put("jdbc.driver_url", JdbcDriverSupport.resolveDriverUrl(jdbc.getDriverUrl(),
                    PaimonConf.driversDir(context),
                    PaimonConf.dorisHome(context)));
            if (StringUtils.isNotBlank(jdbc.getDriverClass())) {
                options.put("jdbc.driver_class", jdbc.getDriverClass());
            }
        }
        return options;
    }

    /**
     * The real data-file format of a {@link DataSplit}: the suffix of its FIRST data file (legacy
     * {@code PaimonSplit} path = {@code "/" + dataFiles().get(0).fileName()}), falling back to the table
     * default when the suffix is unrecognized or the split carries no data file. Ports legacy
     * {@code PaimonScanNode.getFileFormat(getPathString())} for the JNI/COUNT arms, where HEAD had regressed
     * to the bare table-level {@code file.format} default (wrong when the option differs from the on-disk
     * files, e.g. an altered/mixed-format table). Package-private static so the suffix-over-default decision
     * is unit-testable, like {@link #isCountPushdownSplit} / {@link #computeFileSplitOffsets}.
     */
    static String dataSplitFileFormat(DataSplit dataSplit, String defaultFileFormat) {
        List<DataFileMeta> files = dataSplit.dataFiles();
        if (files == null || files.isEmpty()) {
            return defaultFileFormat;
        }
        return getFileFormatBySuffix("/" + files.get(0).fileName()).orElse(defaultFileFormat);
    }

    private static Optional<String> getFileFormatBySuffix(String path) {
        if (path == null) {
            return Optional.empty();
        }
        String lower = path.toLowerCase();
        if (lower.endsWith(".avro")) {
            return Optional.of("avro");
        } else if (lower.endsWith(".orc")) {
            return Optional.of("orc");
        } else if (lower.endsWith(".parquet") || lower.endsWith(".parq")) {
            return Optional.of("parquet");
        }
        return Optional.empty();
    }

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params,
            Map<String, String> properties) {
        // The paimon Table the BE JNI reader deserializes. Set here rather than through a dedicated SPI
        // method: this hook already receives the very TFileScanRangeParams the engine sends, and it runs
        // after the generic scan-range construction, so a plain set is enough. BE fails the scan outright
        // when the field is missing ("missing serialized_table"), so it must be emitted for every scan.
        String serializedTable = properties.get(PROP_SERIALIZED_TABLE);
        if (serializedTable != null) {
            params.setSerializedTable(serializedTable);
            params.setSerializedTableCacheKey(properties.get(PROP_SERIALIZED_TABLE_CACHE_KEY));
        }

        String predicate = properties.get("paimon.predicate");
        if (predicate != null) {
            params.setPaimonPredicate(predicate);
        }

        String optionsJson = properties.get("paimon.options_json");
        if (optionsJson != null && !optionsJson.isEmpty()) {
            try {
                Map<String, String> options = OBJECT_MAPPER
                        .readValue(optionsJson, MAP_TYPE_REF);
                params.setPaimonOptions(options);
            } catch (Exception e) {
                LOG.warn("Failed to parse paimon.options_json", e);
            }
        }

        // FIX-SCHEMA-EVOLUTION (B-1a): apply the schema dictionary built in getScanNodeProperties. Fail
        // loud on a decode error — this prop is produced by us, so a failure is a real bug, and silently
        // dropping it would re-introduce the silent wrong-rows BLOCKER on schema-evolved native reads.
        String schemaEvolution = properties.get(SCHEMA_EVOLUTION_PROP);
        if (schemaEvolution != null && !schemaEvolution.isEmpty()) {
            applySchemaEvolutionParam(params, schemaEvolution);
        }
    }

    /**
     * FIX-E (explain gap): re-emits the legacy {@code PaimonScanNode} EXPLAIN line
     * {@code paimonNativeReadSplits=<raw>/<total>} (native ORC/Parquet sub-splits over all splits).
     * The generic {@code PluginDrivenScanNode} accumulates the counts from
     * {@link ConnectorScanRange#isNativeReadRange()} in {@code getSplits} and injects them into the
     * props map via the {@link ScanNodePropertyKeys#SYNTHETIC_NATIVE_READ_SPLITS} /
     * {@link ScanNodePropertyKeys#SYNTHETIC_TOTAL_READ_SPLITS} synthetic keys,
     * so this connector owns the paimon-specific string without an SPI signature change. Skipped when
     * the keys are absent (e.g. EXPLAIN rendered before any split accounting, or another connector's
     * props map) so the line never prints {@code 0/0} spuriously.
     */
    @Override
    public void appendExplainInfo(StringBuilder output, String prefix,
            Map<String, String> nodeProperties) {
        String nativeSplits = nodeProperties.get(ScanNodePropertyKeys.SYNTHETIC_NATIVE_READ_SPLITS);
        String totalSplits = nodeProperties.get(ScanNodePropertyKeys.SYNTHETIC_TOTAL_READ_SPLITS);
        if (nativeSplits != null && totalSplits != null) {
            output.append(prefix).append("paimonNativeReadSplits=")
                    .append(nativeSplits).append("/").append(totalSplits).append("\n");
            // FIX-A2 (explain gap): re-emit the legacy predicatesFromPaimon: block (the Paimon Predicate
            // objects actually pushed to the SDK, or NONE) BETWEEN paimonNativeReadSplits= and the VERBOSE
            // PaimonSplitStats block -- legacy order PaimonScanNode:657-671. It logically depends only on
            // paimon.predicate and is nested in this native-splits block SOLELY so the legacy ordering
            // holds (in a real EXPLAIN the synthetic split keys are always injected, so the gate always
            // passes). The pushed list is already serialized into paimon.predicate (getScanNodeProperties:
            // 579, always emitted), so deserialize+render it rather than re-converting (the filter is not
            // in the seam).
            String encodedPredicates = nodeProperties.get("paimon.predicate");
            if (encodedPredicates != null) {
                appendPredicatesFromPaimon(output, prefix, encodedPredicates);
            }
            if (nodeProperties.containsKey(ScanNodePropertyKeys.SYNTHETIC_EXPLAIN_VERBOSE)) {
                appendSplitStats(output, prefix,
                        Integer.parseInt(nativeSplits), Integer.parseInt(totalSplits));
            }
        }
    }

    /**
     * FIX-A2 (explain gap): renders the legacy {@code predicatesFromPaimon:} EXPLAIN block from the
     * {@code paimon.predicate} prop (the base64 {@link InstantiationUtil}-serialized
     * {@code List<Predicate>} pushed to the SDK by {@link #getScanNodeProperties}). Lists each pushed
     * predicate (double-prefix indented) or {@code  NONE} when the list is empty, byte-faithful to
     * {@code PaimonScanNode.java:660-668}. Diagnostic-only: surfaces a conjunct that
     * {@link PaimonPredicateConverter} silently dropped (LTZ / FLOAT / unsupported CAST), so this can list
     * fewer entries than the generic {@code PREDICATES:} line. A decode failure is logged and the line
     * skipped -- it must never break EXPLAIN.
     */
    @SuppressWarnings("unchecked")
    private static void appendPredicatesFromPaimon(StringBuilder output, String prefix, String encoded) {
        List<org.apache.paimon.predicate.Predicate> predicates;
        try {
            // paimon.predicate is standard-Base64 by construction (encodeObjectToString -> BASE64_ENCODER
            // = Base64.getEncoder()), so a standard decoder is the exact inverse. Decode with the paimon
            // SDK's own classloader (the plugin CL that loaded Predicate), independent of the TCCL.
            byte[] bytes = Base64.getDecoder().decode(encoded);
            predicates = InstantiationUtil.deserializeObject(
                    bytes, org.apache.paimon.predicate.Predicate.class.getClassLoader());
        } catch (Exception e) {
            // Diagnostic line only -- never break EXPLAIN. The prop is produced by us, so a decode failure
            // is a real bug; log + skip rather than render a misleading NONE.
            LOG.warn("Failed to decode paimon.predicate for EXPLAIN predicatesFromPaimon", e);
            return;
        }
        if (predicates == null) {
            // unexpected payload -- skip (do not render a misleading NONE), consistent with the catch path.
            return;
        }
        output.append(prefix).append("predicatesFromPaimon:");
        if (predicates.isEmpty()) {
            output.append(" NONE\n");
        } else {
            output.append("\n");
            for (org.apache.paimon.predicate.Predicate predicate : predicates) {
                output.append(prefix).append(prefix).append(predicate).append("\n");
            }
        }
    }

    /**
     * FIX-E (explain gap): re-emits the legacy {@code PaimonScanNode} VERBOSE {@code PaimonSplitStats:}
     * block — one {@code SplitStat [type=NATIVE|JNI]} line per split. The generic
     * {@code PluginDrivenScanNode} retains only the native/total counts (not the per-split objects), and
     * native files are re-split into multiple ranges on the SPI path, so exact per-{@code DataSplit}
     * parity (rowCount/mergedRowCount/hasDeletionVector) is not reconstructible; the split TYPE is, which
     * is what {@code paimon_data_system_table}'s assertNativePath/assertJniPath check. Lines are grouped
     * NATIVE-first ({@code [0, native)} NATIVE, {@code [native, total)} JNI). Truncates beyond 4 splits
     * exactly like legacy (first 3 + "... other N ..." + last) so VERBOSE output stays bounded.
     */
    private void appendSplitStats(StringBuilder output, String prefix, int nativeCount, int total) {
        output.append(prefix).append("PaimonSplitStats: \n");
        if (total <= 4) {
            for (int i = 0; i < total; i++) {
                output.append(prefix).append("  ").append(splitStatLine(i, nativeCount)).append("\n");
            }
        } else {
            for (int i = 0; i < 3; i++) {
                output.append(prefix).append("  ").append(splitStatLine(i, nativeCount)).append("\n");
            }
            output.append(prefix).append("  ... other ").append(total - 4)
                    .append(" paimon split stats ...\n");
            output.append(prefix).append("  ").append(splitStatLine(total - 1, nativeCount)).append("\n");
        }
    }

    private static String splitStatLine(int index, int nativeCount) {
        return "SplitStat [type=" + (index < nativeCount ? "NATIVE" : "JNI") + "]";
    }

    /**
     * FIX-E (explain gap): reads the deletion-vector file path carried by one scan range's
     * {@link TPaimonFileDesc}, for the VERBOSE per-backend EXPLAIN block
     * ({@code deleteFileNum}/{@code deleteSplitNum}). Verbatim port of legacy
     * {@code PaimonScanNode.getDeleteFiles} (reading {@code getPaimonParams().getDeletionFile()
     * .getPath()}); the generic {@code PluginDrivenScanNode.getDeleteFiles(TFileRangeDesc)} delegates
     * here. Returns empty when the range carries no paimon params or no deletion file.
     */
    @Override
    public List<String> getDeleteFiles(TTableFormatFileDesc tableFormatParams) {
        List<String> deleteFiles = new ArrayList<>();
        if (tableFormatParams == null || !tableFormatParams.isSetPaimonParams()) {
            return deleteFiles;
        }
        TPaimonFileDesc paimonParams = tableFormatParams.getPaimonParams();
        if (paimonParams == null || !paimonParams.isSetDeletionFile()) {
            return deleteFiles;
        }
        TPaimonDeletionFileDesc deletionFile = paimonParams.getDeletionFile();
        if (deletionFile != null && deletionFile.isSetPath()) {
            deleteFiles.add(deletionFile.getPath());
        }
        return deleteFiles;
    }

    /**
     * FIX-SCHEMA-EVOLUTION (B-1a): builds the native-reader schema dictionary
     * ({@code current_schema_id} + {@code history_schema_info}) for {@code table} and serializes it for
     * transport via the scan-node props (see {@link #SCHEMA_EVOLUTION_PROP}).
     *
     * <p>Returns empty for non-{@link FileStoreTable}s (paimon system tables such as {@code audit_log} /
     * {@code binlog} read via JNI and never consult {@code history_schema_info}). The carrier is a
     * throwaway {@link TFileScanRangeParams} (the exact thrift target), so
     * {@link #applySchemaEvolutionParam} only has to copy the two fields back.</p>
     *
     * <p>Parity with legacy {@code PaimonScanNode}: {@code current_schema_id = -1} and the current/target
     * schema is pushed under that sentinel. Crucially the -1 entry's top-level field set is built from the
     * REQUESTED {@code columns} — the authoritative Doris slot list fe-core also turns into BE's
     * {@code base_ctx->column_names} — NOT from an independent paimon-SDK schema read. This restores the
     * legacy invariant ({@code PaimonScanNode.doInitialize} -> {@code ExternalUtil.initSchemaInfo(-1,
     * getTargetTable().getColumns())}): the -1 entry's names == the scan-slot names BY CONSTRUCTION, so
     * BE's {@code by_table_field_id} / {@code children_column_exists} lookup
     * ({@code table_schema_change_helper.h:166}) can never miss when the FE-cached schema and the
     * scan-time paimon schema skew. (CI 969249: a column added after the last snapshot was present in the
     * FE slots but absent from the resolved {@code table.schema()} read, so the old "build the -1 entry
     * from {@code table.schema()}" tripped the BE DCHECK and aborted the whole BE.) Each column's field id
     * and nested type are matched BY NAME against the resolved (snapshot-pinned for time-travel, latest
     * for plain) schema, with the fresh latest schema as a fallback (see
     * {@link #resolveCurrentSchemaFields}). Per-schema historical entries are added for every committed
     * schema id ({@link SchemaManager#listAllIds()}) so any native file's {@code schema_id} is covered (BE
     * fails loud — {@code "miss table/file schema info"} — if a referenced id is absent). Schema reads
     * that throw are allowed to propagate (fail loud, mirroring legacy {@code putHistorySchemaInfo}).</p>
     */
    private Optional<String> buildSchemaEvolutionParam(PaimonTableHandle handle, Table table,
            List<ConnectorColumnHandle> columns) {
        if (!(table instanceof FileStoreTable)) {
            return Optional.empty();
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        SchemaManager schemaManager = fileStoreTable.schemaManager();

        List<TSchema> history = new ArrayList<>();
        // Current/target schema under the -1 sentinel, keyed off the REQUESTED columns (see javadoc). Its
        // top-level names are case-preserved (paimon-cased): BE keys the table-side StructNode by these names
        // VERBATIM and the native reader looks them up by the case-preserved Doris slot name (#65094 read-path
        // alignment — the slots from getColumnHandles now keep their paimon case). Nested + historical names
        // stay paimon-cased (legacy PaimonUtil.getSchemaInfo). NOT memoized: it reads the LIVE
        // table.schema()/latest() and is keyed off the requested columns, not a committed schema id.
        history.add(buildSchemaInfo(CURRENT_SCHEMA_ID,
                resolveCurrentSchemaFields(fileStoreTable, schemaManager, columns), false));
        // One entry per committed schema id so every native file's schema_id resolves. The EMISSION is
        // unchanged (still every listAllIds() id -> the dict always covers any file's schema_id -> no
        // BE-crash risk); only the per-id field READ is memoized (FIX-B-R2-be). A committed schemaId's
        // schema-<id> file is write-once, so the (handle, schemaId) cache value is immutable; the loader
        // keeps the DIRECT read (not catalogOps.schemaAt) and a read that throws propagates uncached
        // (fail-loud, mirroring legacy putHistorySchemaInfo).
        for (Long schemaId : schemaManager.listAllIds()) {
            List<DataField> fields = schemaAtMemo.getOrLoad(handle, schemaId, () -> {
                TableSchema ts = schemaManager.schema(schemaId);
                return new PaimonCatalogOps.PaimonSchemaSnapshot(
                        ts.fields(), ts.partitionKeys(), ts.primaryKeys());
            }).fields();
            history.add(buildSchemaInfo(schemaId, fields, false));
        }
        return Optional.of(encodeSchemaEvolution(CURRENT_SCHEMA_ID, history));
    }

    /**
     * Resolves the current/target (-1 entry) field list from the requested {@code columns}, matching each
     * to a paimon {@link DataField} BY NAME (case-insensitive). The resolved (snapshot-pinned) schema wins
     * on a name collision so a time-travel read keys the pinned column names (and a renamed column resolves
     * its pinned id before ever reaching the fallback); the fresh latest schema is consulted as a fallback
     * so a column added after the last snapshot — present in the FE slots but lagging the resolved table
     * instance (CI 969249) — is still carried with its real field id (an add-only column is then absent
     * from older files and BE fills it NULL, the correct result). Keying off the requested columns rather
     * than a paimon schema read is what guarantees the -1 entry's names equal BE's scan-slot names, the
     * legacy invariant the field-id matcher relies on. When {@code columns} is empty (e.g. a count-only
     * scan with no projected slots) there is nothing to mismatch, so it falls back to the resolved
     * schema's fields. Fails loud if a requested column is in neither schema (a genuine FE/connector
     * inconsistency) rather than silently dropping it.
     */
    private static List<DataField> resolveCurrentSchemaFields(FileStoreTable table,
            SchemaManager schemaManager, List<ConnectorColumnHandle> columns) {
        List<String> columnNames = new ArrayList<>(columns == null ? 0 : columns.size());
        if (columns != null) {
            for (ConnectorColumnHandle handle : columns) {
                columnNames.add(((PaimonColumnHandle) handle).getName());
            }
        }
        List<DataField> latestFields = schemaManager.latest()
                .map(TableSchema::fields).orElse(Collections.emptyList());
        return selectCurrentSchemaFields(table.schema().fields(), latestFields, columnNames);
    }

    /**
     * Pure field-selection core of {@link #resolveCurrentSchemaFields} (package-private for unit testing).
     * Returns one {@link DataField} per requested {@code columnNames}, matched case-insensitively against
     * {@code resolvedFields} first (so the snapshot-pinned schema wins, keeping time-travel + rename
     * correct) then {@code latestFields} (so an add-column-after-snapshot column the resolved instance lags
     * is still carried with its real field id). Empty {@code columnNames} (count-only scan) -> the resolved
     * fields unchanged. Throws if a requested column is in neither schema (fail loud, not silent drop).
     */
    static List<DataField> selectCurrentSchemaFields(List<DataField> resolvedFields,
            List<DataField> latestFields, List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            return resolvedFields;
        }
        Map<String, DataField> byName = new HashMap<>();
        // Latest first, resolved second so the resolved (snapshot-pinned) field wins on a name collision.
        for (DataField f : latestFields) {
            byName.put(f.name().toLowerCase(Locale.ROOT), f);
        }
        for (DataField f : resolvedFields) {
            byName.put(f.name().toLowerCase(Locale.ROOT), f);
        }
        List<DataField> currentFields = new ArrayList<>(columnNames.size());
        for (String name : columnNames) {
            DataField field = byName.get(name.toLowerCase(Locale.ROOT));
            if (field == null) {
                throw new RuntimeException("paimon schema-evolution: requested column '" + name
                        + "' not found in the resolved or latest schema");
            }
            currentFields.add(field);
        }
        return currentFields;
    }

    /**
     * Serializes the schema dictionary into a base64 TBinaryProtocol blob, carried by a throwaway
     * {@link TFileScanRangeParams} (the exact thrift target so {@link #applySchemaEvolutionParam} only
     * copies the two fields back). Package-private static for round-trip unit testing.
     */
    static String encodeSchemaEvolution(long currentSchemaId, List<TSchema> history) {
        TFileScanRangeParams carrier = new TFileScanRangeParams();
        carrier.setCurrentSchemaId(currentSchemaId);
        carrier.setHistorySchemaInfo(history);
        try {
            byte[] bytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(carrier);
            return BASE64_ENCODER.encodeToString(bytes);
        } catch (Exception | LinkageError e) {
            // Catch LinkageError (e.g. IncompatibleClassChangeError from a thrift classloader split) too:
            // wrapped as a RuntimeException it surfaces as a clean per-query failure instead of escaping
            // the connection handler as an uncaught Error and killing the whole mysql session.
            throw new RuntimeException("Failed to serialize paimon schema-evolution info", e);
        }
    }

    static void applySchemaEvolutionParam(TFileScanRangeParams params, String encoded) {
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            TFileScanRangeParams carrier = new TFileScanRangeParams();
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(carrier, bytes);
            if (carrier.isSetCurrentSchemaId()) {
                params.setCurrentSchemaId(carrier.getCurrentSchemaId());
            }
            if (carrier.isSetHistorySchemaInfo()) {
                params.setHistorySchemaInfo(carrier.getHistorySchemaInfo());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply paimon schema-evolution info to scan params", e);
        }
    }

    /**
     * Builds one {@link TSchema} (schema id + root struct) from a paimon schema's top-level fields.
     * Port of legacy {@code PaimonUtil.getSchemaInfo(TableSchema)} that emits only what BE's field-id
     * matcher consumes ({@code TField.id} / {@code name} / a nested-vs-scalar {@code type.type} tag) —
     * no Doris {@code Type} / {@code toColumnTypeThrift} needed (verified against
     * {@code be/src/format/table/table_schema_change_helper.cpp}).
     *
     * <p>{@code lowercaseTopLevelNames} lowercases ONLY the top-level field names (not nested struct
     * fields) when set. Post-#65094 (read-path case alignment) both the current/target (-1) and historical
     * entries pass {@code false}: top-level names stay case-preserved (paimon-cased) to byte-match the
     * case-preserving Doris slot names BE keys by (the {@code getColumnHandles} slots + {@code parseSchema}
     * now keep their remote case), while nested struct field names are always paimon-cased
     * ({@code PaimonUtil.paimonTypeToDorisType} keeps them).</p>
     */
    static TSchema buildSchemaInfo(long schemaId, List<DataField> fields, boolean lowercaseTopLevelNames) {
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(schemaId);
        tSchema.setRootField(buildStructField(fields, lowercaseTopLevelNames));
        return tSchema;
    }

    private static TStructField buildStructField(List<DataField> fields, boolean lowercaseNames) {
        TStructField structField = new TStructField();
        for (DataField field : fields) {
            // Field id + name are the join keys BE uses to match file<->table columns (rename-safe).
            // Nested structs are always built paimon-cased (legacy parity) — only this level's names are
            // optionally lowercased.
            TField tField = buildField(field.type());
            // When lowercaseNames is set, lowercase the top-level name with the DEFAULT locale (NOT
            // Locale.ROOT — that would diverge from the slot names under a non-ROOT JVM default locale).
            // Post-#65094 production passes false, so the name is emitted case-preserved to byte-match the
            // Doris slot names BE looks up (same casing PaimonConnectorMetadata column mapping produces).
            tField.setName(lowercaseNames ? field.name().toLowerCase() : field.name());
            tField.setId(field.id());
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(tField);
            structField.addToFields(fieldPtr);
        }
        return structField;
    }

    private static TField buildField(DataType dataType) {
        TField field = new TField();
        field.setIsOptional(dataType.isNullable());
        // Paimon uses the same unannotated INT96 encoding for high-precision TIMESTAMP and
        // TIMESTAMP_LTZ, so the table schema is the only reliable discriminator.
        switch (dataType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                field.setTimestampIsAdjustedToUtc(false);
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                field.setTimestampIsAdjustedToUtc(true);
                break;
            default:
                break;
        }
        TColumnType columnType = new TColumnType();
        TNestedField nestedField = new TNestedField();
        switch (dataType.getTypeRoot()) {
            case ARRAY: {
                columnType.setType(TPrimitiveType.ARRAY);
                TArrayField arrayField = new TArrayField();
                TFieldPtr itemPtr = new TFieldPtr();
                itemPtr.setFieldPtr(buildField(((ArrayType) dataType).getElementType()));
                arrayField.setItemField(itemPtr);
                nestedField.setArrayField(arrayField);
                field.setNestedField(nestedField);
                break;
            }
            case MAP: {
                columnType.setType(TPrimitiveType.MAP);
                MapType mapType = (MapType) dataType;
                TMapField mapField = new TMapField();
                TFieldPtr keyPtr = new TFieldPtr();
                keyPtr.setFieldPtr(buildField(mapType.getKeyType()));
                mapField.setKeyField(keyPtr);
                TFieldPtr valuePtr = new TFieldPtr();
                valuePtr.setFieldPtr(buildField(mapType.getValueType()));
                mapField.setValueField(valuePtr);
                nestedField.setMapField(mapField);
                field.setNestedField(nestedField);
                break;
            }
            case ROW: {
                columnType.setType(TPrimitiveType.STRUCT);
                // Nested struct field names stay paimon-cased (legacy PaimonUtil.paimonTypeToDorisType).
                nestedField.setStructField(buildStructField(((RowType) dataType).getFields(), false));
                field.setNestedField(nestedField);
                break;
            }
            default:
                // Scalar: BE reads type.type only as a nested-vs-scalar discriminator (it never inspects
                // the specific scalar tag in the field-id path), so a single placeholder is sufficient and
                // avoids replicating the full paimon->Doris primitive mapping.
                columnType.setType(TPrimitiveType.STRING);
                break;
        }
        field.setType(columnType);
        return field;
    }

    /**
     * Serializes a paimon {@link Split} for the BE JNI reader: ALWAYS Java object serialization, which is
     * what BE's PaimonJniScanner deserializes. Mirrors upstream {@code PaimonScanNode.setPaimonParams} +
     * {@code PaimonUtil.encodeObjectToString} after #66008 removed the paimon-cpp arm — a logical
     * {@link DataSplit} may span several files, and file-scanner-v2 has no split-aware paimon-cpp adapter,
     * so the native-binary ({@code DataSplit.serialize} / {@code paimon::Split::Deserialize}) encoding is
     * never emitted and {@code enable_paimon_cpp_reader} no longer influences the wire format.
     */
    static String encodeSplit(Split split) {
        return encodeObjectToString(split);
    }

    @SuppressWarnings("unchecked")
    private static <T> String encodeObjectToString(T obj) {
        try {
            byte[] bytes = InstantiationUtil.serializeObject(obj);
            return new String(BASE64_ENCODER.encode(bytes), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize object: " + e.getMessage(), e);
        }
    }

    private static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** This catalog's engine-owned storage services (see {@link ConnectorContext#getStorageContext()}). */
    private ConnectorStorageContext storage() {
        return context.getStorageContext();
    }

    /**
     * Statement-scoped cache key for one Paimon scan.
     *
     * <p>Includes every input that changes the planned split list: table identity, the branch pin,
     * the whole scan-options map (snapshot / tag / incremental / options pins), the projected
     * columns in order, the pushed filter, the limit and the COUNT pushdown flag. System tables are excluded
     * upstream, and session variables are statement-constant, so both stay out of the key.
     */
    private static final class PaimonScanReuseKey {
        private final String databaseName;
        private final String tableName;
        private final String branchName;
        private final Map<String, String> scanOptions;
        private final List<String> columnNames;
        private final Optional<ConnectorExpression> filter;
        private final long limit;
        private final boolean countPushdown;

        private PaimonScanReuseKey(PaimonTableHandle handle, ConnectorScanRequest request) {
            // Catalog and query isolation are provided by the statement-scope memo key. System
            // tables are bypassed in planScan before this key is built, so sysTableName is always
            // null here; if the system-table bypass is ever relaxed, add it back.
            this.databaseName = handle.getDatabaseName();
            this.tableName = handle.getTableName();
            this.branchName = handle.getBranchName();
            this.scanOptions = handle.getScanOptions() == null
                    ? Collections.emptyMap()
                    : Collections.unmodifiableMap(new HashMap<>(handle.getScanOptions()));
            this.columnNames = request.getColumns().stream()
                    .map(PaimonScanReuseKey::toPaimonColumnName)
                    .collect(Collectors.toList());
            this.filter = request.getFilter();
            this.limit = request.getLimit();
            this.countPushdown = request.isCountPushdown();
        }

        private static String toPaimonColumnName(ConnectorColumnHandle column) {
            if (!(column instanceof PaimonColumnHandle)) {
                throw new IllegalArgumentException(
                        "Paimon scan reuse key requires PaimonColumnHandle, got: " + column.getClass().getName());
            }
            return ((PaimonColumnHandle) column).getName().toLowerCase(Locale.ROOT);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof PaimonScanReuseKey)) {
                return false;
            }
            PaimonScanReuseKey that = (PaimonScanReuseKey) object;
            return limit == that.limit
                    && countPushdown == that.countPushdown
                    && Objects.equals(databaseName, that.databaseName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(branchName, that.branchName)
                    && Objects.equals(scanOptions, that.scanOptions)
                    && Objects.equals(columnNames, that.columnNames)
                    && Objects.equals(filter, that.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(databaseName, tableName, branchName,
                    scanOptions, columnNames, filter, limit, countPushdown);
        }

        @Override
        public String toString() {
            return "PaimonScanReuseKey{table=" + databaseName + "." + tableName + "}";
        }
    }
}
