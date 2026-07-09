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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StoragePath;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Scan plan provider for Hudi tables.
 *
 * <p>Implements the core scan planning pipeline:
 * <ol>
 *   <li>Build {@link HoodieTableMetaClient} from the table's base path</li>
 *   <li>Resolve the query instant from the completed timeline</li>
 *   <li>Resolve partitions (pruned via applyFilter or all partitions)</li>
 *   <li>For each partition:
 *     <ul>
 *       <li>COW: list latest base files → native reader splits (Parquet/ORC)</li>
 *       <li>MOR: list latest merged file slices → JNI splits (or native if no logs)</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>Scope: snapshot reads, {@code FOR TIME AS OF} time travel (a pinned {@code queryInstant}), and {@code @incr}
 * incremental FILE selection (a resolved {@code (begin, end]} window). Incremental row-level filtering to the
 * window (a synthetic {@code _hoodie_commit_time} predicate) and schema evolution are deferred to later steps.</p>
 */
public class HudiScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(HudiScanPlanProvider.class);

    // The force_jni_scanner session flag (VariableMgr.toMap channel, read via
    // ConnectorSession.getSessionProperties()). When true, the JNI escape hatch is engaged: a native-eligible
    // slice is routed to the JNI reader (dodging native-reader bugs), matching legacy
    // HudiScanNode.canUseNativeReader() / setScanParams (sessionVariable.isForceJniScanner()). Same key + read
    // path as the paimon connector's FORCE_JNI_SCANNER. Default false, so normal reads are unaffected.
    private static final String FORCE_JNI_SCANNER = "force_jni_scanner";

    private final Map<String, String> properties;
    private final ConnectorContext context;

    public HudiScanPlanProvider(Map<String, String> properties, ConnectorContext context) {
        this.properties = properties;
        this.context = context;
    }

    /**
     * Reads the {@code force_jni_scanner} session flag from the SPI session properties. Package-private static
     * for offline unit testing. Default false (legacy default) when unset or the session is null.
     */
    static boolean isForceJniScannerEnabled(ConnectorSession session) {
        if (session == null) {
            return false;
        }
        return Boolean.parseBoolean(session.getSessionProperties().get(FORCE_JNI_SCANNER));
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        String basePath = hudiHandle.getBasePath();

        Configuration conf = buildHadoopConf();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(new org.apache.hudi.storage.hadoop.HadoopStorageConfiguration(conf))
                .setBasePath(basePath)
                .build();

        // Determine COW vs MOR from the Hudi table config (authoritative), NOT the substring-detected handle
        // type: an UNKNOWN detection must not silently pick the wrong read path for a COW table (detection
        // hardening).
        boolean isCow = metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE;
        // force_jni_scanner routes even a native-eligible read to the JNI reader (legacy
        // HudiScanNode.canUseNativeReader() = !isForceJniScanner() && isCowTable), so a COW table under
        // force_jni takes the merged-file-slice (JNI) path too.
        boolean forceJni = isForceJniScannerEnabled(session);
        boolean useNativeCowPath = isCow && !forceJni;

        HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline()
                .filterCompletedInstants();

        Optional<HoodieInstant> lastInstant = timeline.lastInstant().toJavaOptional();
        if (!lastInstant.isPresent()) {
            LOG.info("No completed instants on timeline for {}, returning empty splits", basePath);
            return Collections.emptyList();
        }
        // FOR TIME AS OF pins an explicit instant (applySnapshot stamped it on the handle); a plain read has
        // none and reads the latest completed instant (byte-identical to before this step). This single local
        // drives every downstream instant use: COW/MOR getLatest*BeforeOrOn file selection AND the MOR-JNI
        // THudiFileDesc.instantTime, so FE file selection and the BE merge instant stay consistent.
        String queryInstant = hudiHandle.getQueryInstant() != null
                ? hudiHandle.getQueryInstant()
                : lastInstant.get().requestedTime();

        // Resolve column names and types for JNI reader
        List<String> columnNames;
        List<String> columnTypes;
        // The mode-aware table InternalSchema (+ evolution flag), resolved ONCE, drives the per-file
        // THudiFileDesc.schema_id stamped on native slices for BE's field-id path. Null when unresolved ->
        // no schema_id -> BE BY_NAME (the safe baseline). schema_id is dormant/inert until the dict is emitted.
        HudiSchemaUtils.ResolvedInternalSchema resolvedSchema = null;
        try {
            TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
            // include the 5 `_hoodie_*` meta columns (explicit `true` = legacy parity, in lockstep with
            // HudiConnectorMetadata.getSchemaFromMetaClient) so the JNI reader's column list matches the
            // exposed schema; byte-identical for the common populate.meta.fields=true table.
            Schema avroSchema = schemaResolver.getTableAvroSchema(true);
            columnNames = avroSchema.getFields().stream()
                    .map(Schema.Field::name).collect(Collectors.toList());
            columnTypes = avroSchema.getFields().stream()
                    .map(f -> HudiTypeMapping.toHiveTypeString(f.schema()))
                    .collect(Collectors.toList());
            resolvedSchema = HudiSchemaUtils.resolveTableInternalSchema(schemaResolver, avroSchema);
        } catch (Exception e) {
            LOG.warn("Failed to resolve Hudi schema for JNI reader, JNI splits may fail: {}",
                    e.getMessage());
            columnNames = Collections.emptyList();
            columnTypes = Collections.emptyList();
        }

        // Per-file native-reader schema_id resolver (base-file path -> version), or null to skip stamping
        // (base schema unresolved -> BY_NAME). A per-file resolution failure logs and returns null for that file
        // (BY_NAME) rather than failing the whole scan. Runs on this TCCL-pinned scan thread.
        final HudiSchemaUtils.ResolvedInternalSchema baseSchema = resolvedSchema;
        Function<String, Long> schemaIdResolver = baseSchema == null ? null
                : filePath -> {
                    try {
                        return HudiSchemaUtils.resolveFileInternalSchema(filePath,
                                baseSchema.enableSchemaEvolution, baseSchema.internalSchema, metaClient).schemaId();
                    } catch (Exception e) {
                        LOG.warn("Failed to resolve Hudi per-file schema_id for {}: {}", filePath, e.getMessage());
                        return null;
                    }
                };

        String inputFormat = hudiHandle.getInputFormat();
        String serdeLib = hudiHandle.getSerdeLib();

        // @incr incremental read: a non-null beginInstant is the marker (INC-1 stamped the resolved (begin, end]
        // window onto the handle). Select the files the window touches via the ported IncrementalRelation family
        // instead of the latest-snapshot partition scan below. NOTE: this selects FILES only — row-level
        // filtering to (begin, end] is a LATER step (an FE-side synthetic _hoodie_commit_time predicate), so a
        // bare @incr read would over-read until that lands (harmless while the connector is dormant). The COW-vs-
        // MOR relation choice is driven by the SAME isCow the snapshot path uses (metaClient.getTableType(),
        // hoodie.properties-authoritative): this SUBSUMES the legacy RO-as-RT flip (LogicalHudiScan:251-260 /
        // HudiScanNode:187-199), which existed only because legacy classifies from the hive inputFormat (a MOR
        // _ro facade uses HoodieParquetInputFormat, so legacy misreads it as COW and the flip re-routes it to
        // MOR). metaClient.getTableType() reads MERGE_ON_READ for that facade directly, so no flip — and no serde
        // params are needed on the handle. Do NOT restore the flip.
        if (hudiHandle.getBeginInstant() != null) {
            IncrementalRelation relation = buildIncrementalRelation(metaClient, conf, hudiHandle, isCow);
            Optional<List<ConnectorScanRange>> incrementalRanges = incrementalRanges(relation, isCow, forceJni,
                    basePath, inputFormat, serdeLib, columnNames, columnTypes, partitionFieldNames(metaClient));
            if (incrementalRanges.isPresent()) {
                LOG.info("Hudi incremental scan planning: {}.{} window=({}, {}] splits={}",
                        hudiHandle.getDbName(), hudiHandle.getTableName(),
                        hudiHandle.getBeginInstant(), hudiHandle.getEndInstant(), incrementalRanges.get().size());
                return incrementalRanges.get();
            }
            // relation.fallbackFullTableScan() (archived instant / missing file) → degrade to the normal
            // latest-snapshot partition scan below (legacy HudiScanNode.getSplits:470), reading the latest instant.
        }

        // Build file system view via FileSystemViewManager (Hudi 1.0.2 API)
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient))
                .build();
        HoodieLocalEngineContext engineCtx = new HoodieLocalEngineContext(metaClient.getStorageConf());
        HoodieTableFileSystemView fsView = FileSystemViewManager.createInMemoryFileSystemView(
                engineCtx, metaClient, metadataConfig);

        // Resolve partitions
        List<String> partitionPaths = resolvePartitions(hudiHandle, metaClient);

        List<ConnectorScanRange> ranges = new ArrayList<>();
        for (String partitionPath : partitionPaths) {
            Map<String, String> partValues = parsePartitionValues(
                    partitionPath, hudiHandle.getPartitionKeyNames());

            if (useNativeCowPath) {
                collectCowSplits(fsView, partitionPath, queryInstant,
                        basePath, partValues, ranges, schemaIdResolver);
            } else {
                collectMorSplits(fsView, partitionPath, queryInstant,
                        basePath, inputFormat, serdeLib,
                        columnNames, columnTypes, partValues, forceJni, ranges, schemaIdResolver);
            }
        }

        LOG.info("Hudi scan planning: {}.{} type={} partitions={} splits={}",
                hudiHandle.getDbName(), hudiHandle.getTableName(),
                hudiHandle.getHudiTableType(), partitionPaths.size(), ranges.size());

        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        boolean isCow = "COPY_ON_WRITE".equals(hudiHandle.getHudiTableType());

        Map<String, String> props = new LinkedHashMap<>();
        // For COW tables, we default to parquet (may be overridden per-split).
        // For MOR tables, default is JNI.
        props.put("file_format_type", isCow ? "parquet" : "jni");
        props.put("table_format_type", "hudi");

        // Partition keys
        List<String> partKeys = hudiHandle.getPartitionKeyNames();
        if (partKeys != null && !partKeys.isEmpty()) {
            props.put("path_partition_keys", String.join(",", partKeys));
        }

        // BE-facing storage for the native + JNI readers, mirroring legacy getLocationProperties' dual merge.
        //  (1) BE-canonical static credentials (AWS_* for object stores, resolved hadoop.*/dfs.* for HDFS): BE's
        //      native (FILE_S3) reader understands ONLY these canonical keys, so without them a private bucket
        //      403s (the raw catalog aliases s3.access_key/... are useless to it). Sourced from the context's
        //      single normalization hook. Empty for no context (offline tests) or a credential-less warehouse.
        if (context != null) {
            context.getBackendStorageProperties().forEach((k, v) -> props.put("location." + k, v));
        }
        //  (2) Hadoop-format passthrough for the Hudi JNI reader (its own Hadoop FileSystem: fs.s3a.* etc).
        //      Emitted AFTER the canonical set so an overlapping hadoop key resolves to the catalog's explicit
        //      value (legacy putAll order: backendStorageProperties then hadoopProperties). The s3./oss./cos./obs.
        //      Doris aliases are harmless to BE (ignored by both readers) but kept so no configured key is dropped.
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("hadoop.") || key.startsWith("fs.")
                    || key.startsWith("dfs.") || key.startsWith("hive.")
                    || key.startsWith("s3.") || key.startsWith("cos.")
                    || key.startsWith("oss.") || key.startsWith("obs.")) {
                props.put("location." + key, entry.getValue());
            }
        }

        return props;
    }

    /**
     * Collect splits for COW (Copy on Write) tables.
     * COW tables only have base files — use native Parquet/ORC reader.
     */
    private void collectCowSplits(
            HoodieTableFileSystemView fsView,
            String partitionPath, String queryInstant,
            String basePath,
            Map<String, String> partValues,
            List<ConnectorScanRange> ranges,
            Function<String, Long> schemaIdResolver) {
        fsView.getLatestBaseFilesBeforeOrOn(partitionPath, queryInstant)
                .forEach(baseFile -> {
                    String filePath = baseFile.getPath();
                    long fileSize = baseFile.getFileSize();
                    String format = detectFileFormat(filePath);

                    HudiScanRange.Builder builder = new HudiScanRange.Builder()
                            .path(filePath)
                            .start(0)
                            .length(fileSize)
                            .fileSize(fileSize)
                            .fileFormat(format)
                            .partitionValues(partValues);
                    // COW base files always read native -> stamp the per-file schema version for BE's field-id path.
                    Long schemaId = schemaIdResolver == null ? null : schemaIdResolver.apply(filePath);
                    if (schemaId != null) {
                        builder.schemaId(schemaId);
                    }
                    ranges.add(builder.build());
                });
    }

    /**
     * Collect splits for MOR (Merge on Read) tables.
     * MOR tables may have base files + delta log files. If a file slice
     * has no delta logs, we can use the native reader; otherwise JNI.
     */
    private void collectMorSplits(
            HoodieTableFileSystemView fsView,
            String partitionPath, String queryInstant,
            String basePath, String inputFormat, String serdeLib,
            List<String> columnNames, List<String> columnTypes,
            Map<String, String> partValues, boolean forceJni,
            List<ConnectorScanRange> ranges,
            Function<String, Long> schemaIdResolver) {
        fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, queryInstant)
                .forEach(fileSlice -> ranges.add(buildMorRange(fileSlice, partValues, queryInstant,
                        forceJni, basePath, inputFormat, serdeLib, columnNames, columnTypes, schemaIdResolver)));
    }

    /**
     * Builds one MOR {@link HudiScanRange} from a merged {@link FileSlice}, shared by the snapshot MOR path
     * ({@link #collectMorSplits}) and the {@code @incr} MOR path ({@link #incrementalRanges}). Byte-faithful port
     * of legacy {@code HudiScanNode.generateHudiSplit}: a slice with no delta logs reads natively (parquet/orc)
     * UNLESS {@code force_jni} keeps it on JNI, a log-only slice uses its first log as the agency path, and a JNI
     * slice carries the full merge metadata. The {@code jniInstant} is the merge instant BE reads: the snapshot
     * path passes its {@code queryInstant}, the incremental path passes the resolved window END
     * ({@code relation.getEndTs()}). Package-private static so the mapping is unit-testable with a hand-built
     * {@link FileSlice} and reused by both paths without duplication.
     *
     * <p>{@code schemaIdResolver} (base-file path -&gt; native schema version) is applied ONLY to a native
     * (no-log, non-force-jni) slice — the JNI merge reader consumes no schema_id. {@code null} skips stamping
     * (the {@code @incr} path passes null: {@code @incr} lists the latest schema, no per-file dict).</p>
     */
    static HudiScanRange buildMorRange(FileSlice fileSlice, Map<String, String> partValues, String jniInstant,
            boolean forceJni, String basePath, String inputFormat, String serdeLib,
            List<String> columnNames, List<String> columnTypes,
            Function<String, Long> schemaIdResolver) {
        Optional<HoodieBaseFile> baseFileOpt = fileSlice.getBaseFile().toJavaOptional();
        String filePath = baseFileOpt.map(BaseFile::getPath).orElse("");
        long fileSize = baseFileOpt.map(BaseFile::getFileSize).orElse(0L);

        List<String> logs = fileSlice.getLogFiles()
                .map(HoodieLogFile::getPath)
                .map(StoragePath::toString)
                .collect(Collectors.toList());

        // Dynamic format decision: no logs → native reader, UNLESS force_jni keeps it on JNI
        // (legacy HudiScanNode.setScanParams' !isForceJniScanner() guard on the no-log downgrade).
        boolean useNative = logs.isEmpty() && !filePath.isEmpty() && !forceJni;
        String format = useNative ? detectFileFormat(filePath) : "jni";

        // For log-only slices, use first log as agency path
        String agencyPath = filePath.isEmpty() && !logs.isEmpty()
                ? logs.get(0) : filePath;

        HudiScanRange.Builder builder = new HudiScanRange.Builder()
                .path(agencyPath)
                .start(0)
                .length(fileSize)
                .fileSize(fileSize)
                .fileFormat(format)
                .partitionValues(partValues)
                // Bake force_jni so populateRangeParams (no session) keeps this slice on JNI too.
                .forceJni(forceJni);

        if (!useNative) {
            // JNI reader needs full metadata
            builder.instantTime(jniInstant)
                    .serde(serdeLib)
                    .inputFormat(inputFormat)
                    .basePath(basePath)
                    .dataFilePath(filePath)
                    .dataFileLength(fileSize)
                    .deltaLogs(logs)
                    .columnNames(columnNames)
                    .columnTypes(columnTypes);
        } else if (schemaIdResolver != null) {
            // Native no-log slice reads via the field-id native reader -> stamp its base file's schema version.
            Long schemaId = schemaIdResolver.apply(filePath);
            if (schemaId != null) {
                builder.schemaId(schemaId);
            }
        }

        return builder.build();
    }

    /**
     * Builds the ported {@link IncrementalRelation} for a resolved {@code @incr} window: a COW relation when
     * {@code isCow} (metaClient-authoritative), else a MOR relation. The relation consumes the ALREADY-RESOLVED
     * {@code begin/endInstant} from the handle (INC-1) and the raw {@code @incr} option params (glob / fallback /
     * hollow-commit policy) threaded onto the handle, and does file selection only. Runs inline in {@code
     * planScan}, reusing its metaClient + Hadoop conf, so the relation's filesystem/timeline I/O inherits the
     * scan thread's plugin classloader pin (the same context the snapshot path's metaClient I/O runs in). The
     * ctor's {@link IOException} is re-typed to {@link DorisConnectorException} (parity with {@link
     * #resolvePartitions}).
     */
    private static IncrementalRelation buildIncrementalRelation(HoodieTableMetaClient metaClient,
            Configuration conf, HudiTableHandle handle, boolean isCow) {
        Map<String, String> optParams = handle.getIncrementalParams();
        HollowCommitHandling policy = IncrementalRelation.hollowCommitHandling(optParams);
        try {
            return isCow
                    ? new COWIncrementalRelation(metaClient, conf, handle.getBeginInstant(),
                            handle.getEndInstant(), policy, optParams)
                    : new MORIncrementalRelation(metaClient, conf, handle.getBeginInstant(),
                            handle.getEndInstant(), policy, optParams);
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to build incremental relation for " + handle.getBasePath(), e);
        }
    }

    /**
     * The incremental split set for a resolved {@code @incr} window, or {@link Optional#empty()} to signal the
     * caller must DEGRADE to the normal latest-snapshot scan. Byte-parity with legacy {@code
     * HudiScanNode.getSplits:470} + {@code getIncrementalSplits}, but routing on the relation TYPE
     * ({@code isCow}) rather than legacy's {@code canUseNativeReader()}:
     * <ul>
     *   <li>{@code relation.fallbackFullTableScan()} (an archived instant / missing file) &rarr;
     *       {@link Optional#empty()} = degrade to the latest-snapshot scan (NOT an error), legacy {@code :470}.</li>
     *   <li>COW &rarr; {@link IncrementalRelation#collectSplits()} yields native ranges directly.
     *       <b>{@code force_jni} is intentionally IGNORED for a COW incremental read</b> (it always reads native)
     *       &mdash; a signed, deliberate deviation from legacy, which routes {@code force_jni}+COW to the MOR-style
     *       branch and calls {@code collectFileSlices()} on a COW relation &rarr; {@code UnsupportedOperationException}
     *       (a latent legacy crash). Routing on the relation type never calls the unsupported shape.</li>
     *   <li>MOR &rarr; {@link IncrementalRelation#collectFileSlices()} (a FLAT cross-partition slice list) turned
     *       into JNI ranges at the resolved window END ({@code relation.getEndTs()}), with per-slice partition
     *       values parsed from the slice's own partition path against the Hudi table-config partition fields
     *       (the same non-handle source the COW relation uses). {@code force_jni} still keeps a no-log MOR slice
     *       on JNI via {@link #buildMorRange}.</li>
     * </ul>
     * Package-private static, pure over the {@link IncrementalRelation} contract, so file-selection routing +
     * the degrade decision are unit-testable with a fake relation (no live metaClient).
     */
    static Optional<List<ConnectorScanRange>> incrementalRanges(IncrementalRelation relation, boolean isCow,
            boolean forceJni, String basePath, String inputFormat, String serdeLib,
            List<String> columnNames, List<String> columnTypes, List<String> partitionFieldNames) {
        if (relation.fallbackFullTableScan()) {
            return Optional.empty();
        }
        List<ConnectorScanRange> ranges = new ArrayList<>();
        if (isCow) {
            ranges.addAll(relation.collectSplits());
            return Optional.of(ranges);
        }
        String endTs = relation.getEndTs();
        for (FileSlice fileSlice : relation.collectFileSlices()) {
            Map<String, String> partValues = parsePartitionValues(fileSlice.getPartitionPath(), partitionFieldNames);
            // @incr lists the LATEST schema (no per-file schema_id dict on the incremental path) -> null resolver.
            ranges.add(buildMorRange(fileSlice, partValues, endTs, forceJni,
                    basePath, inputFormat, serdeLib, columnNames, columnTypes, null));
        }
        return Optional.of(ranges);
    }

    /**
     * The Hudi table-config partition-field names (byte-faithful to legacy {@code HudiScanNode:391-393}), the
     * source the incremental MOR path parses per-slice partition values against &mdash; NOT the HMS-sourced
     * handle partition keys the snapshot path uses (the two coincide only for hive-synced tables).
     */
    private static List<String> partitionFieldNames(HoodieTableMetaClient metaClient) {
        Option<String[]> fields = metaClient.getTableConfig().getPartitionFields();
        return fields.isPresent() ? Arrays.asList(fields.get()) : Collections.emptyList();
    }

    /**
     * Resolve partition paths from handle or by listing all partitions.
     */
    private List<String> resolvePartitions(
            HudiTableHandle handle, HoodieTableMetaClient metaClient) {
        // Check if partitions were pruned via applyFilter
        List<String> prunedPaths = handle.getPrunedPartitionPaths();
        if (prunedPaths != null) {
            return prunedPaths;
        }

        // No pruning — list all partitions
        List<String> partKeyNames = handle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            // Unpartitioned table
            return Collections.singletonList("");
        }

        try {
            return listAllPartitionPaths(metaClient);
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to list partitions for " + handle.getBasePath(), e);
        }
    }

    /**
     * Builds a {@link HoodieTableMetaClient} from a Hadoop {@link Configuration} and base path. Package-private
     * static so the metadata path ({@link HudiConnectorMetadata}) builds the metaClient the same way the scan
     * does, from inside the plugin-auth + TCCL pin its execute-wrapper supplies.
     */
    static HoodieTableMetaClient buildMetaClient(Configuration conf, String basePath) {
        return HoodieTableMetaClient.builder()
                .setConf(new org.apache.hudi.storage.hadoop.HadoopStorageConfiguration(conf))
                .setBasePath(basePath)
                .build();
    }

    /**
     * Returns the LATEST completed instant as its raw {@code requestedTime} String (e.g.
     * {@code yyyyMMddHHmmssSSS}, compared lexicographically), or {@code Optional.empty()} when the timeline has
     * no completed instants. Reads the same {@code getCommitsAndCompactionTimeline().filterCompletedInstants()}
     * as {@link #latestCompletedInstant} / {@link #planScan}.
     *
     * <p>This ONE shared helper is byte-parity with legacy COW/MOR incremental {@code latestTime} for BOTH table
     * types, because {@code metaClient.getCommitsAndCompactionTimeline()} resolves per table type to exactly the
     * timeline legacy uses per type (verified against hudi-common 1.0.2 bytecode):
     * <ul>
     *   <li>COW &rarr; {@code getActiveTimeline().getCommitAndReplaceTimeline()} = {@code {commit, replacecommit,
     *       clustering}} &mdash; identical to what legacy COW's {@code metaClient.getCommitTimeline()} returns
     *       (that metaClient method ALSO delegates to {@code getCommitAndReplaceTimeline()}, so it is NOT
     *       commit-only; it includes the replacecommit/clustering instants COW produces via INSERT OVERWRITE /
     *       clustering).</li>
     *   <li>MOR &rarr; {@code getActiveTimeline().getWriteTimeline()} &mdash; identical to legacy MOR's
     *       {@code metaClient.getCommitsAndCompactionTimeline()}.</li>
     * </ul>
     * Both legacy and this helper take {@code lastInstant().requestedTime()} under the default hollow-commit
     * policy; the {@code USE_TRANSITION_TIME} completion-time variant is served by the overload below.
     */
    static Optional<String> latestCompletedInstantTime(HoodieTableMetaClient metaClient) {
        return latestCompletedInstantTime(metaClient, false);
    }

    /**
     * The LATEST completed instant on the requested-time axis (default) or the completion-time axis when
     * {@code useCompletionTime} is true. The completion-time axis is legacy's {@code USE_TRANSITION_TIME}
     * hollow-commit policy path: legacy COW/MOR derive the default / {@code "latest"} end via
     * {@code lastInstant().getCompletionTime()} rather than {@code requestedTime()} under that policy
     * ({@code COWIncrementalRelation:94-96} / {@code MORIncrementalRelation:88-90}), and both their file
     * selection ({@code findInstantsInRangeByCompletionTime}) and the row filter consume that completion-time
     * end. {@link HudiConnectorMetadata#resolveTimeTravel} resolves the end on the SAME axis so the ONE
     * handle-stamped end is correct for both — the connector never lets the file set and the row filter diverge
     * on the window. Completion time {@code >=} requested time for any instant, so a requested-time end fed into
     * completion-time selection would silently drop the final in-window commit (under-read).
     */
    static Optional<String> latestCompletedInstantTime(HoodieTableMetaClient metaClient, boolean useCompletionTime) {
        return metaClient.getCommitsAndCompactionTimeline()
                .filterCompletedInstants().lastInstant().toJavaOptional()
                .map(instant -> useCompletionTime ? instant.getCompletionTime() : instant.requestedTime());
    }

    /**
     * Returns the LATEST completed instant as a numeric long ({@code yyyyMMddHHmmssSSS}), or {@code 0L} when
     * the timeline has none. Byte-faithful port of legacy {@code HudiUtils.getLastTimeStamp} and the same
     * timeline {@link #planScan} reads at query time — so the MVCC pin and the scan take the identical instant.
     */
    static long latestCompletedInstant(HoodieTableMetaClient metaClient) {
        return requestedTimeToInstant(latestCompletedInstantTime(metaClient));
    }

    /**
     * Pure numeric mapping backing {@link #latestCompletedInstant}: a present {@code requestedTime} parses to a
     * long ({@code Long.parseLong}, fail-loud on malformed = legacy parity); absent &rarr; {@code 0L} (legacy
     * empty-timeline sentinel, {@code >= 0} so it survives the dictionary-refresh filter). Extracted so the
     * empty/value semantics are unit-testable without a live metaClient.
     */
    static long requestedTimeToInstant(Optional<String> requestedTime) {
        return requestedTime.map(Long::parseLong).orElse(0L);
    }

    /**
     * Lists ALL partition relative paths from the Hudi metadata table (COW/MOR agnostic). Byte-faithful port of
     * legacy {@code HudiPartitionUtils.getAllPartitionNames}; extracted so both {@link #resolvePartitions} and
     * the metadata partition-listing path share one copy of the {@code HoodieTableMetadata.create(...)} dance.
     */
    static List<String> listAllPartitionPaths(HoodieTableMetaClient metaClient) throws Exception {
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(HoodieTableMetadataUtil.isFilesPartitionAvailable(metaClient))
                .build();
        HoodieLocalEngineContext engineCtx = new HoodieLocalEngineContext(metaClient.getStorageConf());
        HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(
                engineCtx, metaClient.getStorage(), metadataConfig,
                metaClient.getBasePath().toString(), true);
        return tableMetadata.getAllPartitionPaths();
    }

    /**
     * Parse a Hudi partition's relative path into a column&rarr;value map, byte-faithful to legacy
     * {@code HudiPartitionUtils.parsePartitionValues}. Handles BOTH hive-style ("year=2024/month=01") and Hudi's
     * DEFAULT non-hive-style POSITIONAL ("2024/01") layouts, and URL-unescapes every value:
     * <ul>
     *   <li>A fragment carrying the "col=" prefix contributes the suffix; a fragment WITHOUT it is mapped
     *       POSITIONALLY to the i-th partition column. The old split-on-'=' logic silently DROPPED a
     *       prefix-less fragment, so a non-hive-style partitioned table read NULL partition columns on a plain
     *       snapshot read — this is the regression this fix closes.</li>
     *   <li>Single partition column with a mismatched fragment count: the whole path (minus an optional "col="
     *       prefix) is that column's value (legacy single-column-whole-path fallback).</li>
     *   <li>Fragment count != column count with &gt; 1 column: fail loud, exactly like legacy.</li>
     *   <li>Every value is unescaped via {@link #unescapePathName} (e.g. "%20" &rarr; space) — legacy delegated
     *       to Hive's {@code FileUtils.unescapePathName}; inlined here so the connector needs no hive-common
     *       dependency (mirrors the fe-connector-hive inlined copy).</li>
     * </ul>
     *
     * <p>Static + package-private for direct unit testing (no live HoodieTableMetaClient needed).
     *
     * <p>NOTE: this derives values from the partition path fed to the FileSystemView. On the UNPRUNED path that
     * path is Hudi's own relative path (getAllPartitionPaths) = the shape the FileSystemView uses, so values are
     * consistent. The PRUNED path (applyFilter) currently feeds HMS hive-style partition NAMES, which match the
     * FileSystemView only for hive-sync'd tables; making the pruned partition SOURCE useHiveSyncPartition-aware
     * for non-hive-style tables belongs to the partition-listing step (which ports that source once), and is
     * likewise closed before the catalog flip.
     */
    static Map<String, String> parsePartitionValues(
            String partitionPath, List<String> partKeyNames) {
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            // Non-partitioned table (legacy returns an empty value list). The unpartitioned scan path always
            // reaches here with an empty key list, so an empty partitionPath needs no separate guard.
            return Collections.emptyMap();
        }
        Map<String, String> values = new LinkedHashMap<>();
        String[] fragments = partitionPath.split("/");
        if (fragments.length != partKeyNames.size()) {
            if (partKeyNames.size() == 1) {
                String prefix = partKeyNames.get(0) + "=";
                String value = partitionPath.startsWith(prefix)
                        ? partitionPath.substring(prefix.length()) : partitionPath;
                values.put(partKeyNames.get(0), unescapePathName(value));
                return values;
            }
            throw new DorisConnectorException(
                    "Failed to parse partition values of path: " + partitionPath);
        }
        for (int i = 0; i < fragments.length; i++) {
            String prefix = partKeyNames.get(i) + "=";
            String raw = fragments[i].startsWith(prefix)
                    ? fragments[i].substring(prefix.length()) : fragments[i];
            values.put(partKeyNames.get(i), unescapePathName(raw));
        }
        return values;
    }

    /**
     * Renders a Hive-style partition name ({@code "col0=val0/col1=val1/..."}) from a column&rarr;value map, in
     * partition-key order, ESCAPING each value with {@link #escapePathName} (the canonical Hive {@code
     * makePartName}).
     *
     * <p><b>MANDATORY for the generic MVCC model:</b> fe-core rebuilds the partition item by re-parsing this
     * name via {@code HiveUtil.toPartitionValues} under a {@code checkState(values.size()==types.size())}. A raw
     * positional path ({@code "2024/01"}) would yield the wrong value count &rarr; the partition is skipped
     * &rarr; silent UNPARTITIONED degrade, so a hive-style name is required. Escaping is MANDATORY too:
     * {@code HiveUtil.toPartitionValues} splits on {@code '/'}, so a value that itself spans {@code '/'} (e.g. a
     * single partition column with a {@code yyyy/MM/dd} output format &rarr; path {@code "2024/01/02"}) must be
     * escaped ({@code "%2F"}) or the re-parse would truncate/collide it. Since {@code escapePathName} is the
     * exact inverse of {@link #escapePathName}'s unescape (the same set {@code HiveUtil.toPartitionValues} uses),
     * the re-parse recovers EXACTLY the values {@link #parsePartitionValues} produced. Static + package-private
     * for direct unit testing.
     */
    static String renderHiveStylePartitionName(List<String> partKeyNames, Map<String, String> values) {
        StringBuilder sb = new StringBuilder();
        for (String col : partKeyNames) {
            if (sb.length() > 0) {
                sb.append('/');
            }
            sb.append(col).append('=').append(escapePathName(values.get(col)));
        }
        return sb.toString();
    }

    // Hive FileUtils.charToEscape minus the control range: escaped so a partition VALUE containing one of these
    // survives the round-trip through HiveUtil.toPartitionValues (which url-unescapes). '/' and '=' are the
    // load-bearing ones (structural to the re-parse); the rest mirror Hive for name faithfulness.
    private static final String CHARS_TO_ESCAPE = "\"#%'*/:=?\\{[]^";

    /**
     * URL-encodes a partition value into a Hive-escaped path component (e.g. {@code "a/b"} &rarr; {@code
     * "a%2Fb"}). Byte-faithful port of Hive's {@code org.apache.hadoop.hive.common.FileUtils.escapePathName} and
     * the exact inverse of {@link #unescapePathName}, so a rendered hive-style name re-parses (unescapes) back
     * to the original value. Inlined so the connector needs no hive-common dependency.
     */
    private static String escapePathName(String value) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x20 || c == 0x7F || CHARS_TO_ESCAPE.indexOf(c) >= 0) {
                sb.append('%').append(String.format("%02X", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * URL-decodes a Hive-escaped path component (e.g. "a%2Fb" &rarr; "a/b"). Byte-faithful port of Hive's
     * {@code org.apache.hadoop.hive.common.FileUtils.unescapePathName} (identical to the fe-connector-hive
     * inlined copy in {@code HiveWriteUtils}), so the connector needs no hive-common dependency.
     */
    private static String unescapePathName(String path) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code = -1;
                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception e) {
                    code = -1;
                }
                if (code >= 0) {
                    sb.append((char) code);
                    i += 2;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Detect file format from file path suffix. Package-private static so the ported incremental relations
     * ({@code COWIncrementalRelation}) stamp the required explicit {@code fileFormat} on their native
     * {@link HudiScanRange}s the same way the snapshot COW path does.
     */
    static String detectFileFormat(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return "parquet";
        }
        String lower = filePath.toLowerCase();
        if (lower.endsWith(".parquet")) {
            return "parquet";
        } else if (lower.endsWith(".orc")) {
            return "orc";
        }
        return "parquet";
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
}
