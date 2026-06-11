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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
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
 * the Nereids-pruned set feeds FE EXPLAIN ({@code partition=N/M}) and the generic scan node's
 * pruned-empty short-circuit only &mdash; never {@code planScan}. For an explicit time-travel pin the
 * connector deliberately reports an empty partition-item map and defers pruning to this predicate
 * pushdown; {@code PluginDrivenScanNode.resolveRequiredPartitions} is guarded so that empty-universe
 * pin scans-all rather than short-circuiting to zero splits. None of this affects read-row correctness.
 */
public class PaimonScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(PaimonScanPlanProvider.class);

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE_REF =
            new TypeReference<Map<String, String>>() {};

    // Session variable name (byte-identical to SessionVariable.ENABLE_PAIMON_CPP_READER) surfaced
    // through ConnectorSession.getSessionProperties() (VariableMgr.toMap). When true, BE routes the
    // JNI-format paimon split to PaimonCppReader, which deserializes the NATIVE paimon binary format
    // (paimon::Split::Deserialize), so FE must serialize a DataSplit with that format, not Java serde.
    private static final String ENABLE_PAIMON_CPP_READER = "enable_paimon_cpp_reader";

    // FIX-SCHEMA-EVOLUTION (B-1a): scan-level prop carrying the base64 TBinaryProtocol-serialized
    // schema dictionary (a throwaway TFileScanRangeParams holding current_schema_id +
    // history_schema_info). getScanNodeProperties builds it from the live table; populateScanLevelParams
    // applies it to the real params. Transport via the props map because getScanPlanProvider() returns a
    // fresh provider per call (no shared instance state between the two SPI methods).
    private static final String SCHEMA_EVOLUTION_PROP = "paimon.schema_evolution";
    // Legacy parity: current_schema_id is the -1 sentinel ("latest"); the current/target schema is
    // also pushed into history_schema_info under this key (PaimonScanNode.doInitialize -> -1L).
    private static final long CURRENT_SCHEMA_ID = -1L;

    private final Map<String, String> properties;
    private final PaimonCatalogOps catalogOps;
    private final ConnectorContext context;

    public PaimonScanPlanProvider(Map<String, String> properties, PaimonCatalogOps catalogOps) {
        this(properties, catalogOps, null);
    }

    public PaimonScanPlanProvider(Map<String, String> properties, PaimonCatalogOps catalogOps,
            ConnectorContext context) {
        this.properties = properties;
        this.catalogOps = catalogOps;
        this.context = context;
    }

    /**
     * Reads the {@code enable_paimon_cpp_reader} session flag from the SPI session properties
     * (forwarded by the engine via {@code VariableMgr.toMap}). Default false (legacy default), so
     * normal reads are unaffected. Package-private static for offline unit testing.
     */
    static boolean isCppReaderEnabled(ConnectorSession session) {
        if (session == null) {
            return false;
        }
        return Boolean.parseBoolean(session.getSessionProperties().get(ENABLE_PAIMON_CPP_READER));
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
        try {
            return PaimonTableResolver.resolve(catalogOps, paimonHandle);
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
        if (scanOptions != null && !scanOptions.isEmpty()) {
            return table.copy(scanOptions);
        }
        return table;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {

        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table = resolveScanTable(paimonHandle);

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
                .filter(i -> i >= 0)
                .toArray();

        // Call Paimon SDK
        ReadBuilder readBuilder = table.newReadBuilder();
        if (!predicates.isEmpty()) {
            readBuilder.withFilter(predicates);
        }
        if (projected.length > 0) {
            readBuilder.withProjection(projected);
        }
        TableScan scan = readBuilder.newScan();
        List<Split> paimonSplits = scan.plan().splits();

        // Determine table location
        String tableLocation = getTableLocation(table);
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

        // Read the cpp-reader flag once: it selects the JNI split serialization format (see encodeSplit).
        boolean cppReader = isCppReaderEnabled(session);

        // Non-DataSplit → always JNI
        for (Split split : nonDataSplits) {
            ranges.add(buildJniScanRange(split, tableLocation, defaultFileFormat,
                    Collections.emptyMap(), false, cppReader));
        }

        // Process DataSplits
        for (DataSplit dataSplit : dataSplits) {
            Map<String, String> partitionValues = getPartitionInfoMap(
                    table, dataSplit.partition(), session.getTimeZone());

            Optional<List<RawFile>> optRawFiles = dataSplit.convertToRawFiles();
            Optional<List<DeletionFile>> optDeletionFiles = dataSplit.deletionFiles();

            if (shouldUseNativeReader(paimonHandle.isForceJni(), optRawFiles)) {
                // Native reader path
                List<RawFile> rawFiles = optRawFiles.get();
                for (int i = 0; i < rawFiles.size(); i++) {
                    DeletionFile deletionFile =
                            (optDeletionFiles.isPresent() && i < optDeletionFiles.get().size())
                                    ? optDeletionFiles.get().get(i) : null;
                    ranges.add(buildNativeRange(
                            rawFiles.get(i), deletionFile, defaultFileFormat, partitionValues));
                }
            } else {
                // JNI reader path
                ranges.add(buildJniScanRange(
                        dataSplit, tableLocation, defaultFileFormat,
                        partitionValues, true, cppReader));
            }
        }

        return ranges;
    }

    /**
     * Builds the native-reader {@link PaimonScanRange} for one raw ORC/Parquet file plus its optional
     * deletion vector. BOTH the data-file path and the deletion-vector path are routed through
     * {@link #normalizeUri} so BE's scheme-dispatched S3 factory receives canonical {@code s3://}
     * URIs on OSS/COS/OBS/s3a warehouses (FIX-URI-NORMALIZE; legacy {@code PaimonScanNode} normalizes
     * both via the 2-arg {@code LocationPath.of}). Package-private so both normalization sites are
     * unit-testable without a live deletion-vector-bearing split.
     */
    PaimonScanRange buildNativeRange(RawFile file, DeletionFile deletionFile,
            String defaultFileFormat, Map<String, String> partitionValues) {
        String fileFormat = getFileFormatBySuffix(file.path()).orElse(defaultFileFormat);
        PaimonScanRange.Builder builder = new PaimonScanRange.Builder()
                .path(normalizeUri(file.path()))
                .start(0)
                .length(file.length())
                .fileSize(file.length())
                .fileFormat(fileFormat)
                .partitionValues(partitionValues)
                .schemaId(file.schemaId());
        if (deletionFile != null) {
            builder.deletionFile(
                    normalizeUri(deletionFile.path()), deletionFile.offset(), deletionFile.length());
        }
        return builder.build();
    }

    /**
     * Normalizes a raw paimon-SDK storage URI (native data-file or deletion-vector path) into BE's
     * canonical scheme via the engine ({@code oss://}/{@code cos://}/{@code obs://}/{@code s3a://}
     * &rarr; {@code s3://}; OSS {@code bucket.endpoint} &rarr; {@code bucket}). Ports legacy
     * {@code PaimonScanNode}'s 2-arg {@code LocationPath.of(path, storagePropertiesMap)} — BE's S3
     * file factory only recognizes {@code s3://}, so an un-normalized OSS/COS/OBS path fails the
     * native read (data file) or silently drops the deletion vector (merge-on-read wrong rows). The
     * connector cannot import fe-core's {@code LocationPath}, so it delegates to the
     * {@link ConnectorContext#normalizeStorageUri} seam. With no context (offline unit tests) the raw
     * path is preserved — same null-guard as the {@code vendStorageCredentials} overlay below.
     */
    private String normalizeUri(String rawUri) {
        return context != null ? context.normalizeStorageUri(rawUri) : rawUri;
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
        props.put("file_format_type", "jni");
        props.put("table_format_type", "paimon");

        // Serialized table for BE's JNI reader
        String serializedTable = encodeObjectToString(table);
        props.put("paimon.serialized_table", serializedTable);

        // Serialized predicates for BE's JNI scanner
        if (filter.isPresent()) {
            RowType rowType = table.rowType();
            PaimonPredicateConverter converter = new PaimonPredicateConverter(rowType);
            List<org.apache.paimon.predicate.Predicate> predicates = converter.convert(filter.get());
            if (!predicates.isEmpty()) {
                String serializedPredicate = encodeObjectToString(predicates);
                props.put("paimon.predicate", serializedPredicate);
            }
        }

        // Paimon JDBC metastore options for BE (if applicable)
        Map<String, String> backendOptions = getBackendPaimonOptions();
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
        // BE-canonical keys (AWS_* for object stores, hadoop/dfs for HDFS). Ports legacy
        // PaimonScanNode.getLocationProperties() = getBackendPropertiesFromStorageMap(storagePropertiesMap):
        // BE's native (FILE_S3) reader understands ONLY the canonical keys, so the raw catalog aliases
        // (s3.access_key, oss.access_key, …) must be translated before they leave FE — copying them
        // verbatim gives the native reader no usable creds (403 on a private bucket). The connector
        // cannot import fe-core StorageProperties -> it delegates to the ConnectorContext seam. Empty
        // when no context (offline unit tests) -> no storage props emitted (never the broken raw aliases).
        if (context != null) {
            for (Map.Entry<String, String> e : context.getBackendStorageProperties().entrySet()) {
                props.put("location." + e.getKey(), e.getValue());
            }
        }

        // FIX-REST-VENDED: overlay per-table vended cloud-storage credentials (REST catalogs).
        // The raw token is extracted from the live, snapshot-pinned table's RESTTokenFileIO (paimon
        // SDK only), then normalized to BE-facing AWS_* keys by the engine (the connector cannot
        // import fe-core's StorageProperties). Vended overlays static (legacy precedence). Skipped
        // when no context (offline unit tests) or the table is non-REST (empty token -> no-op).
        if (context != null) {
            Map<String, String> vendedBeProps = context.vendStorageCredentials(extractVendedToken(table));
            for (Map.Entry<String, String> e : vendedBeProps.entrySet()) {
                props.put("location." + e.getKey(), e.getValue());
            }
        }

        // FIX-SCHEMA-EVOLUTION (B-1a): emit the native-reader schema dictionary so BE matches file<->table
        // columns BY FIELD ID across schema evolution (rename/reorder) instead of falling back to NAME
        // matching (which silently reads NULL/garbage for renamed columns). Only meaningful when the table
        // can take the native path (a DataTable read without force_jni_scanner); JNI splits never consult it.
        if (!paimonHandle.isForceJni()) {
            buildSchemaEvolutionParam(table).ifPresent(v -> props.put(SCHEMA_EVOLUTION_PROP, v));
        }

        return props;
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

    private PaimonScanRange buildJniScanRange(Split split, String tableLocation,
            String defaultFileFormat, Map<String, String> partitionValues,
            boolean isDataSplit, boolean cppReader) {
        long splitWeight = 0;
        if (isDataSplit) {
            splitWeight = computeSplitWeight((DataSplit) split);
        } else {
            splitWeight = split.rowCount();
        }

        String serializedSplit = encodeSplit(split, cppReader);

        return new PaimonScanRange.Builder()
                .fileFormat("jni")
                .paimonSplit(serializedSplit)
                .tableLocation(tableLocation)
                .partitionValues(partitionValues)
                .selfSplitWeight(splitWeight)
                .build();
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
     * <p>The split is native-eligible iff (a) it is NOT name-forced to JNI by the handle, AND (b) its
     * raw files all support the native reader (see {@link #supportNativeReader}). Gating on
     * {@code forceJni} is the T19 fix: {@code binlog} / {@code audit_log} system tables are paimon
     * {@code DataTable}s whose {@code DataSplit.convertToRawFiles()} may succeed, but the native
     * reader cannot reproduce their read semantics (binlog pack/merge + array materialization;
     * audit_log rowkind/sequence-number projection), so they would silently return wrong rows. Legacy
     * forces them to JNI ({@code PaimonScanNode.shouldForceJniForSystemTable}, captured by
     * {@link PaimonTableHandle#isForceJni()}). ONLY the {@code forceJni} flag gates this: metadata sys
     * tables already go JNI via the non-DataSplit path, and a non-forced {@code DataTable} like "ro"
     * (forceJni=false) must still be allowed native — so this must not over-force.
     *
     * <p>Extracted as a pure static so the correctness-critical routing decision is unit-testable
     * with real {@link RawFile}s, without driving a full Paimon {@code ReadBuilder}/{@code TableScan}.
     */
    static boolean shouldUseNativeReader(boolean forceJni, Optional<List<RawFile>> optRawFiles) {
        return !forceJni && supportNativeReader(optRawFiles);
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
                result.put(partitionKeys.get(i).toLowerCase(Locale.ROOT), value);
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

    private String getTableLocation(Table table) {
        if (table instanceof FileStoreTable) {
            return ((FileStoreTable) table).location().toString();
        }
        return table.options().get("path");
    }

    private Map<String, String> getBackendPaimonOptions() {
        String metastoreType = properties.get("paimon.catalog.type");
        if (!"jdbc".equalsIgnoreCase(metastoreType)) {
            return Collections.emptyMap();
        }
        Map<String, String> options = new HashMap<>();
        // Forward relevant JDBC catalog properties for BE's paimon-cpp reader
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("jdbc.") || key.equals("warehouse")
                    || key.equals("uri") || key.equals("metastore")
                    || key.equals("catalog-key")) {
                options.put(key, entry.getValue());
            }
        }
        return options;
    }

    private static Optional<String> getFileFormatBySuffix(String path) {
        if (path == null) {
            return Optional.empty();
        }
        String lower = path.toLowerCase();
        if (lower.endsWith(".orc")) {
            return Optional.of("orc");
        } else if (lower.endsWith(".parquet") || lower.endsWith(".parq")) {
            return Optional.of("parquet");
        }
        return Optional.empty();
    }

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params,
            Map<String, String> properties) {
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
     * schema is pushed under that sentinel. Crucially it is built from {@code table.schema()} — the
     * resolved (snapshot-PINNED) schema, the SAME schema the query's tuple slots use — so a time-travel
     * read keys BE's table-side {@code StructNode} by the pinned column names (legacy built the -1 entry
     * from {@code getTargetTable().getColumns()}, also snapshot-aware; {@code schemaManager().latest()}
     * would wrongly use the absolute latest schema). Per-schema historical entries are added for every
     * committed schema id ({@link SchemaManager#listAllIds()}) so any native file's {@code schema_id} is
     * covered (BE fails loud — {@code "miss table/file schema info"} — if a referenced id is absent).
     * Schema reads that throw are allowed to propagate (fail loud, mirroring legacy
     * {@code putHistorySchemaInfo}).</p>
     */
    private Optional<String> buildSchemaEvolutionParam(Table table) {
        if (!(table instanceof FileStoreTable)) {
            return Optional.empty();
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        SchemaManager schemaManager = fileStoreTable.schemaManager();

        List<TSchema> history = new ArrayList<>();
        // Current/target schema under the -1 sentinel, from the resolved (snapshot-pinned) schema. Its
        // top-level names are lowercased: BE keys the table-side StructNode by these names VERBATIM and the
        // native reader looks them up by the lowercase Doris slot name (legacy ExternalUtil/parseSchema
        // parity). Nested + historical names stay paimon-cased (legacy PaimonUtil.getSchemaInfo).
        history.add(buildSchemaInfo(CURRENT_SCHEMA_ID, fileStoreTable.schema().fields(), true));
        // One entry per committed schema id so every native file's schema_id resolves.
        for (Long schemaId : schemaManager.listAllIds()) {
            history.add(buildSchemaInfo(schemaId, schemaManager.schema(schemaId).fields(), false));
        }
        return Optional.of(encodeSchemaEvolution(CURRENT_SCHEMA_ID, history));
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
        } catch (Exception e) {
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
     * fields) — the legacy-asymmetric casing: the current/target (-1) entry needs lowercase top-level
     * names to match the lowercase Doris slot names BE keys by ({@code parseSchema} lowercases top-level),
     * while nested struct field names stay paimon-cased ({@code PaimonUtil.paimonTypeToDorisType} keeps
     * them) and historical entries are fully paimon-cased.</p>
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
            // Default-locale toLowerCase to byte-match the Doris slot names BE looks up — produced the
            // same way by PaimonConnectorMetadata column mapping and legacy PaimonUtil.parseSchema (NOT
            // Locale.ROOT — that would diverge from the slot names under a non-ROOT JVM default locale).
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

    @Override
    public String getSerializedTable(Map<String, String> properties) {
        return properties.get("paimon.serialized_table");
    }

    /**
     * Selects the split serialization that matches the BE reader the engine will use.
     * When the paimon-cpp reader is enabled AND the split is a {@link DataSplit}, serialize with
     * Paimon's NATIVE binary format ({@code DataSplit.serialize}) so BE's PaimonCppReader
     * ({@code paimon::Split::Deserialize}) can decode it. Otherwise (flag off, or a non-DataSplit
     * system split / no-raw-file fallback that has no native format) fall back to Java object
     * serialization for the Java JNI reader. Mirrors legacy PaimonScanNode.setPaimonParams +
     * PaimonUtil.encodeDataSplitToString; the {@code instanceof DataSplit} guard is load-bearing
     * parity (non-DataSplit splits MUST stay Java-serialized even when the flag is on).
     */
    static String encodeSplit(Split split, boolean cppReader) {
        if (cppReader && split instanceof DataSplit) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ((DataSplit) split).serialize(new DataOutputViewStreamWrapper(baos));
                return new String(BASE64_ENCODER.encode(baos.toByteArray()), StandardCharsets.UTF_8);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize Paimon DataSplit (native format): "
                        + e.getMessage(), e);
            }
        }
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
}
