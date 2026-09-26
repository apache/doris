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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalScanTaskCacheKey;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonMvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonReaderOptions;
import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.PaimonUtils;
import org.apache.doris.datasource.paimon.profile.PaimonMetricRegistry;
import org.apache.doris.datasource.paimon.profile.PaimonScanMetricsReporter;
import org.apache.doris.datasource.property.metastore.PaimonJdbcMetaStoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TPaimonDeletionFileDesc;
import org.apache.doris.thrift.TPaimonFileDesc;
import org.apache.doris.thrift.TPaimonReaderType;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.stream.Collectors;

public class PaimonScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(PaimonScanNode.class);

    private static final long COUNT_WITH_PARALLEL_SPLITS = 10000;
    private static final long MAX_RETAINED_SERIALIZED_TASK_BYTES = 16L * 1024 * 1024;
    private long maxRetainedSerializedTaskBytes = MAX_RETAINED_SERIALIZED_TASK_BYTES;
    // The keys of incremental read params for Paimon SDK
    private static final String PAIMON_INCREMENTAL_BETWEEN = "incremental-between";
    private static final String PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE = "incremental-between-scan-mode";
    private static final String PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP = "incremental-between-timestamp";
    // The keys of incremental read params for Doris Statement
    private static final String DORIS_START_SNAPSHOT_ID = "startSnapshotId";
    private static final String DORIS_END_SNAPSHOT_ID = "endSnapshotId";
    private static final String DORIS_START_TIMESTAMP = "startTimestamp";
    private static final String DORIS_END_TIMESTAMP = "endTimestamp";
    private static final String DORIS_INCREMENTAL_BETWEEN_SCAN_MODE = "incrementalBetweenScanMode";
    private static final String PAIMON_PROPERTY_PREFIX = "paimon.";
    private static final String DORIS_ENABLE_FILE_READER_ASYNC = "jni.enable_file_reader_async";
    private static final String DORIS_ENABLE_JNI_IO_MANAGER = "jni.enable_jni_io_manager";
    private static final String DORIS_JNI_IO_MANAGER_TMP_DIR = "jni.io_manager.tmp_dir";
    private static final String DORIS_JNI_IO_MANAGER_IMPL_CLASS = "jni.io_manager.impl_class";
    private static final String DORIS_MANIFEST_PARALLELISM_CAP =
            "doris.scan.manifest.parallelism-cap";
    private static final String DORIS_SERIALIZED_SYSTEM_SOURCE = "doris.serialized-system-source";
    private static final String DORIS_SYSTEM_TABLE_TYPE = "doris.system-table-type";
    // Same key as the rust CoreOptions DELETION_VECTORS_MERGE_ON_READ_OPTION: paimon
    // 1.4 exposes no Java accessor for it, so the raw TableSchema option is read.
    private static final String DELETION_VECTORS_MERGE_ON_READ = "deletion-vectors.merge-on-read";
    private static final List<String> BACKEND_PAIMON_OPTIONS = Arrays.asList(
            DORIS_ENABLE_JNI_IO_MANAGER,
            DORIS_JNI_IO_MANAGER_TMP_DIR,
            DORIS_JNI_IO_MANAGER_IMPL_CLASS,
            DORIS_ENABLE_FILE_READER_ASYNC);
    // The table-location URI schemes whose property translation into the
    // paimon-rust FileIO key families is implemented (BE bridge) and open
    // tested: s3 / s3a -> the s3.* family, oss -> the fs.oss.* family, plus
    // the credential-free hdfs and local-filesystem parsers (hadoop conf and
    // local paths pass through untouched). See isRustVerifiedLocationScheme.
    // An hdfs:// location additionally requires the credential-free backend
    // shape of isRustVerifiedHdfsBackend: the same storage properties carry a
    // Kerberized catalog's principal / keytab, its proxy user and its HA
    // nameservice config, none of which the rust HDFS parser can honor.
    private static final Set<String> RUST_VERIFIED_LOCATION_SCHEMES =
            new HashSet<>(Arrays.asList("s3", "s3a", "oss", "hdfs", "file"));

    // The dfs./hadoop./fs. keys a credential-free HDFS catalog may transport
    // without changing what the pinned rust reader does: the warehouse path
    // identity (fs.defaultFS, always shipped from the location), the
    // authentication-type markers HdfsProperties always writes (validated to
    // simple above), and the always-written fallback flag whose semantics only
    // matter under Kerberos (already rejected above). Every other client
    // setting is dropped by the rust parser and would diverge from JNI.
    private static final Set<String> RUST_VERIFIED_HDFS_OPTION_KEYS =
            new HashSet<>(Arrays.asList(
                    "fs.defaultFS",
                    "hadoop.security.authentication",
                    "hdfs.security.authentication",
                    "ipc.client.fallback-to-simple-auth-allowed"));

    private enum SplitReadType {
        JNI,
        NATIVE,
    }

    private class SplitStat {
        SplitReadType type = SplitReadType.JNI;
        private long rowCount = 0;
        private Optional<Long> mergedRowCount = Optional.empty();
        private boolean rawFileConvertable = false;
        private boolean hasDeletionVector = false;

        public void setType(SplitReadType type) {
            this.type = type;
        }

        public void setRowCount(long rowCount) {
            this.rowCount = rowCount;
        }

        public void setMergedRowCount(long mergedRowCount) {
            this.mergedRowCount = Optional.of(mergedRowCount);
        }

        public void setRawFileConvertable(boolean rawFileConvertable) {
            this.rawFileConvertable = rawFileConvertable;
        }

        public void setHasDeletionVector(boolean hasDeletionVector) {
            this.hasDeletionVector = hasDeletionVector;
        }

        @Override
        public String toString() {
            return "SplitStat [type=" + type
                    + ", rowCount=" + rowCount
                    + ", mergedRowCount=" + (mergedRowCount.isPresent() ? mergedRowCount.get() : "NONE")
                    + ", rawFileConvertable=" + rawFileConvertable
                    + ", hasDeletionVector=" + hasDeletionVector + "]";
        }
    }

    private PaimonSource source = null;
    private List<Predicate> predicates;
    private int rawFileSplitNum = 0;
    private int paimonSplitNum = 0;
    private List<SplitStat> splitStats = new ArrayList<>();
    private String serializedTable;
    private final String serializedTableCacheKey = UUID.randomUUID().toString();
    // Store PropertiesMap, including vended credentials or static credentials
    // get them in doInitialize() to ensure internal consistency of ScanNode
    private Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;
    private Map<String, String> backendStorageProperties;
    private Map<String, String> backendPaimonOptions = Collections.emptyMap();
    private Table processedTable;

    // The schema information involved in the current query process (including historical schema).
    protected ConcurrentHashMap<Long, Boolean> currentQuerySchema = new ConcurrentHashMap<>();

    public PaimonScanNode(PlanNodeId id,
                          TupleDescriptor desc,
                          boolean needCheckColumnPriv,
                          SessionVariable sv,
                          ScanContext scanContext) {
        super(id, desc, "PAIMON_SCAN_NODE", StatisticalType.PAIMON_SCAN_NODE,
                scanContext, needCheckColumnPriv, sv);
        // Some branch-4.1 callers construct the scan node before attaching a table descriptor;
        // defer source creation for them while preserving eager setup for normal planned scans.
        if (desc.getTable() != null) {
            source = new PaimonSource(desc);
        }
    }

    @Override
    protected void doInitialize() throws UserException {
        checkVariantV2Enabled(desc);
        Optional<MvccSnapshot> relationSnapshot = getRelationSnapshot();
        if (desc.getTable() instanceof PaimonExternalTable
                || desc.getTable() instanceof PaimonSysExternalTable) {
            // Rebuild from the logical scan's snapshot: system-table snapshots are keyed by their
            // source table and cannot be recovered by looking up the synthetic descriptor table.
            source = new PaimonSource(desc, relationSnapshot);
        } else if (source == null) {
            source = new PaimonSource(desc);
        }
        processedTable = getProcessedTable();
        super.doInitialize();
        long startTime = System.currentTimeMillis();
        serializeProcessedTable();
        params.setNumOfColumnsFromFile(processedTable.rowType().getFieldCount() - getPathPartitionKeys().size());
        List<Column> queryColumns = desc.getSlots().stream()
                .map(slot -> slot.getColumn())
                .collect(Collectors.toList());
        // Todo: Get the current schema id of the table, instead of using -1.
        ExternalUtil.initSchemaInfo(params, -1L, queryColumns);
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) source.getCatalog();
        storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                catalog.getCatalogProperty().getMetastoreProperties(),
                catalog.getCatalogProperty().getStoragePropertiesMap(),
                source.getPaimonTable()
        );
        backendStorageProperties = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);
        backendPaimonOptions = new HashMap<>(getBackendPaimonOptions());
        OptionalInt manifestCap;
        if (source.getExternalTable() instanceof PaimonSysExternalTable) {
            manifestCap = source.runtimeSafeManifestParallelism(getScanParams());
        } else {
            manifestCap = PaimonReaderOptions.backendManifestParallelismCap(processedTable);
        }
        // The hidden planner's option is not visible on a serialized system wrapper.
        manifestCap.ifPresent(cap -> backendPaimonOptions.put(
                DORIS_MANIFEST_PARALLELISM_CAP, String.valueOf(cap)));
        if (source.getExternalTable() instanceof PaimonSysExternalTable && manifestCap.isPresent()) {
            PaimonSysExternalTable systemTable = (PaimonSysExternalTable) source.getExternalTable();
            TableScanParams scanParams = getScanParams();
            Map<String, String> incrementalOptions = scanParams != null && scanParams.incrementalRead()
                    ? getIncrReadParams() : Collections.emptyMap();
            FileStoreTable effectiveSource = source.runtimeSafeSystemDataTable(
                    scanParams, incrementalOptions);
            // A system wrapper can hide its physical option map. Ship the exact source so a smaller
            // BE can cap it and rebuild without guessing through the wrapper.
            backendPaimonOptions.put(DORIS_SERIALIZED_SYSTEM_SOURCE,
                    PaimonUtil.encodeObjectToString(effectiveSource));
            backendPaimonOptions.put(DORIS_SYSTEM_TABLE_TYPE, systemTable.getSysTableType());
        }
        if (getSummaryProfile() != null) {
            getSummaryProfile().addExternalTableGetTableMetaTime(System.currentTimeMillis() - startTime);
        }
    }

    @VisibleForTesting
    static void checkVariantV2Enabled(TupleDescriptor tuple) throws UserException {
        // Enforce the global switch here instead of during Paimon schema conversion. The cached
        // external schema is shared, while this tuple contains only the slots projected by the
        // current query and therefore does not reject scans of unrelated non-VARIANT columns.
        if (!Config.enable_variant_v2
                && tuple.getSlots().stream().anyMatch(slot -> PaimonUtil.containsVariant(slot.getType()))) {
            throw new UserException(
                    "Paimon VARIANT columns require FE config enable_variant_v2=true");
        }
    }

    private void serializeProcessedTable() throws UserException {
        // System-table splits are materialized by the BE JNI reader, so it must receive the same
        // option-bearing table copy that FE uses to plan the split.
        serializedTable = PaimonUtil.encodeObjectToString(getProcessedTable());
    }

    @VisibleForTesting
    public void setSource(PaimonSource source) {
        this.source = source;
    }

    @Override
    protected void convertPredicate() {
        PaimonPredicateConverter paimonPredicateConverter = new PaimonPredicateConverter(
                processedTable.rowType());
        predicates = paimonPredicateConverter.convertToPaimonExpr(conjuncts);
    }

    @Override
    protected List<String> getFileColumnNames() {
        if (scanParams != null && scanParams.isOptions()) {
            // Relation-scoped options may select a historical schema, so its slots must be
            // positioned against the same processed table that is serialized to the reader.
            return processedTable.rowType().getFieldNames();
        }
        // Normal scans must retain the refreshable descriptor schema; the cached Paimon table
        // handle can still expose pre-refresh column names after an external schema change.
        return super.getFileColumnNames();
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof PaimonSplit) {
            setPaimonParams(rangeDesc, (PaimonSplit) split);
        }
    }

    @Override
    protected Optional<String> getSerializedTable() {
        return Optional.of(serializedTable);
    }

    @Override
    protected Optional<String> getSerializedTableCacheKey() {
        return Optional.of(serializedTableCacheKey);
    }

    @Override
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        // Set paimon_predicate at ScanNode level to avoid redundant serialization in each split
        String serializedPredicate = PaimonUtil.encodeObjectToString(predicates);
        params.setPaimonPredicate(serializedPredicate);
        setScanLevelPaimonOptions();
    }

    private void setScanLevelPaimonOptions() {
        if (!backendPaimonOptions.isEmpty()) {
            params.setPaimonOptions(backendPaimonOptions);
        }
    }

    /**
     * Whether the table location's URI scheme is served by a paimon-rust FileIO parser whose
     * property translation the FE/BE bridge implements (the s3.* / fs.oss.* key families for
     * s3 / s3a / oss) or that needs no credentials at all (hdfs hadoop conf and local
     * filesystem paths). Every other scheme the pinned crate dispatches to its own parser
     * (cosn / obs / gs / abfs and friends) must fall back to the JNI reader because Doris
     * delivers those credentials only as AWS_* aliases that those parsers do not read.
     * A null location cannot be verified (and cannot ship paimon_table either), so it is
     * not rust-eligible.
     *
     * <p>The scheme must also appear in the exact lowercase form the pinned crate consumes.
     * URI schemes are case-insensitive, but the crate lowercases only its storage
     * dispatch: its object-store path extraction strips a lowercase {@code s3://} prefix
     * from the original string, and the hdfs / file helpers likewise match only lowercase
     * prefixes. A {@code S3://} or {@code Hdfs://} warehouse also produces DataSplit file
     * paths in that original casing (serialized by the paimon SDK before the FE sees
     * them), so the mixed-case shape fails the rust open beyond the transported location.
     * Those valid URI variants must therefore route to JNI, whose Java stack is
     * case-insensitive everywhere.
     */
    @VisibleForTesting
    static boolean isRustVerifiedLocationScheme(String location) {
        if (location == null) {
            return false;
        }
        int sep = location.indexOf("://");
        if (sep <= 0) {
            // No URI scheme: a plain local path reads through the crate's local-filesystem
            // parser, which needs no credentials.
            return true;
        }
        return RUST_VERIFIED_LOCATION_SCHEMES.contains(location.substring(0, sep));
    }

    // Whether the location is an hdfs:// table (the only HDFS-family scheme in
    // RUST_VERIFIED_LOCATION_SCHEMES; viewfs / jfs never pass it).
    private static boolean isHdfsLocationScheme(String location) {
        if (location == null) {
            return false;
        }
        int sep = location.indexOf("://");
        return sep > 0 && "hdfs".equalsIgnoreCase(location.substring(0, sep));
    }

    // Whether any member file of the split is ORC. Paimon allows per-level
    // file.format, so one DataSplit can mix Parquet and ORC files; the split
    // path's suffix (the first file) cannot speak for the whole split, and the
    // shifted ORC LTZ decode applies to whichever ORC members rust reads.
    @VisibleForTesting
    static boolean splitHasOrcFile(DataSplit dataSplit) {
        if (dataSplit == null) {
            return false;
        }
        for (DataFileMeta fileMeta : dataSplit.dataFiles()) {
            String format = fileMeta.fileFormat();
            if (format != null && "orc".equalsIgnoreCase(format)) {
                return true;
            }
        }
        return false;
    }

    // Mirrors the pinned paimon-rust read-mode option matrix
    // (PartialUpdateConfig::read_unsupported_option_keys and
    // AggregationConfig's runtime-unsupported keys). Both validate option-key
    // PRESENCE, not values, so a table carrying the key with an off value is
    // still rejected by the rust merge construction while Java reads it — the
    // gate must mirror presence exactly.
    //
    // Partial-update reads support basic mode, sequence groups and field
    // aggregation; unsupported keys are the remove-record-on-delete family,
    // per-field ignore-delete / ignore-retract / distinct / nested-key /
    // count-limit options.
    private static boolean isRustUnsupportedPartialUpdateReadOption(String key) {
        return (key.endsWith(".ignore-delete")
                && !"ignore-delete".equals(key)
                && !"partial-update.ignore-delete".equals(key))
                || "partial-update.remove-record-on-delete".equals(key)
                || "partial-update.remove-record-on-sequence-group".equals(key)
                || hasFieldOptionSuffix(key, ".ignore-retract")
                || hasFieldOptionSuffix(key, ".distinct")
                || hasFieldOptionSuffix(key, ".nested-key")
                || hasFieldOptionSuffix(key, ".count-limit");
    }

    // Aggregation reads support the per-field aggregate-function /
    // list-agg-delimiter / default-aggregate-function matrix; unsupported keys
    // are the remove-record-on-delete family, every ignore-delete spelling
    // (including the bare one), and per-field sequence-group / ignore-retract /
    // distinct / nested-key / count-limit options.
    private static boolean isRustUnsupportedAggregationRuntimeOption(String key) {
        return "ignore-delete".equals(key)
                || key.endsWith(".ignore-delete")
                || "aggregation.remove-record-on-delete".equals(key)
                || hasFieldOptionSuffix(key, ".sequence-group")
                || hasFieldOptionSuffix(key, ".ignore-retract")
                || hasFieldOptionSuffix(key, ".distinct")
                || hasFieldOptionSuffix(key, ".nested-key")
                || hasFieldOptionSuffix(key, ".count-limit");
    }

    private static boolean hasFieldOptionSuffix(String key, String suffix) {
        return key.startsWith("fields.") && key.endsWith(suffix);
    }

    // Whether the schema options carry any option key the pinned paimon-rust
    // read rejects for this merge engine.
    @VisibleForTesting
    static boolean hasRustUnsupportedMergeOption(Map<String, String> options,
            CoreOptions.MergeEngine mergeEngine) {
        boolean partialUpdate = mergeEngine == CoreOptions.MergeEngine.PARTIAL_UPDATE;
        for (String key : options.keySet()) {
            if (key == null) {
                continue;
            }
            if (partialUpdate ? isRustUnsupportedPartialUpdateReadOption(key)
                    : isRustUnsupportedAggregationRuntimeOption(key)) {
                return true;
            }
        }
        return false;
    }

    // Whether any member file of the split carries a data-file.external-paths
    // location. Paimon can store an absolute location in each DataFileMeta, and
    // both Java and the serialized rust split prefer it over the bucket path —
    // but the pinned rust table builds ONE FileIO from paimon_table, whose
    // storage enum parses every file with that warehouse-selected backend: an
    // admitted hdfs table with an s3:// external file (or an s3 table with an
    // oss:// file) reaches the wrong parser and fails the open, while JNI
    // reads it. The shipped options describe only the warehouse, so any
    // external file keeps the split on JNI.
    @VisibleForTesting
    static boolean splitHasExternalFiles(DataSplit dataSplit) {
        if (dataSplit == null) {
            return false;
        }
        for (DataFileMeta fileMeta : dataSplit.dataFiles()) {
            if (fileMeta.externalPath().isPresent()) {
                return true;
            }
        }
        return false;
    }

    // Recursively whether this paimon type, or any member of it, is
    // TIMESTAMP_WITH_LOCAL_TIME_ZONE: an LTZ nested under MAP/ARRAY/ROW
    // reaches the same shifted ORC decode through the container's field
    // materialization.
    @VisibleForTesting
    static boolean containsTimestampLtz(DataType type) {
        if (type == null) {
            return false;
        }
        if (type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return true;
        }
        if (type instanceof ArrayType) {
            return containsTimestampLtz(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return containsTimestampLtz(mapType.getKeyType())
                    || containsTimestampLtz(mapType.getValueType());
        }
        if (type instanceof RowType) {
            return ((RowType) type).getFields().stream()
                    .anyMatch(field -> containsTimestampLtz(field.type()));
        }
        return false;
    }

    /**
     * Whether an HDFS catalog's shipped backend properties describe a shape the
     * pinned paimon-rust HDFS reader can serve identically to JNI. The rust
     * storage_hdfs parser reads only the hdfs.name-node / hdfs.enable-append
     * keys — no kerberos, no proxy user, no HA resolution, and no Hadoop client
     * option map (HdfsNativeConfig.options stays empty) — so a catalog whose
     * settings need any of those would open as the BE process's ambient
     * identity, resolve the wrong DataNode, or miss the catalog's configured
     * access while the same query works through JNI. Only the open-tested
     * credential-free shape stays rust-eligible: simple (or unset)
     * authentication, no principal / keytab / proxy user, no HA nameservice
     * resolution, and no client option beyond the always-shipped inert keys of
     * {@link #RUST_VERIFIED_HDFS_OPTION_KEYS} — everything else in the
     * dfs./hadoop./fs. namespaces is dropped by the rust parser and keeps the
     * catalog on JNI (dfs.client.use.datanode.hostname=true is the canonical
     * simple-auth example: hdfs-native defaults to false and connects to the
     * DataNode's advertised IP instead of its hostname, which commonly fails
     * behind containers/NAT).
     */
    @VisibleForTesting
    static boolean isRustVerifiedHdfsBackend(Map<String, String> backendStorageProperties) {
        if (backendStorageProperties == null) {
            return true;
        }
        // Authentication type: "simple" (or unset) authenticates as the same
        // ambient OS user on both readers; kerberos (or anything else) needs the
        // channel the rust parser does not read.
        for (String authKey : new String[] {"hadoop.security.authentication",
                "hdfs.security.authentication"}) {
            String value = backendStorageProperties.get(authKey);
            if (value != null && !"simple".equalsIgnoreCase(value.trim())) {
                return false;
            }
        }
        // Kerberos identity and the proxy user: any of these configured means
        // the open must carry a specific identity, which only JNI can honor.
        for (String identityKey : new String[] {"hadoop.kerberos.principal",
                "hadoop.kerberos.keytab", "hadoop.username"}) {
            String value = backendStorageProperties.get(identityKey);
            if (value != null && !value.trim().isEmpty()) {
                return false;
            }
        }
        // HA nameservice resolution: the rust parser receives no dfs.* config,
        // so a nameservice-authority location (dfs.nameservices / dfs.ha.*)
        // cannot be resolved; the proven shape is a single name-node URI.
        String nameServices = backendStorageProperties.get("dfs.nameservices");
        if (nameServices != null && !nameServices.trim().isEmpty()) {
            return false;
        }
        for (Map.Entry<String, String> entry : backendStorageProperties.entrySet()) {
            if (entry.getKey() != null && entry.getKey().startsWith("dfs.ha.")
                    && entry.getValue() != null && !entry.getValue().trim().isEmpty()) {
                return false;
            }
        }
        // Client options: the pinned rust storage_hdfs parser reads only the
        // hdfs.name-node / hdfs.enable-append keys and leaves
        // HdfsNativeConfig.options empty, so any other transported dfs./hadoop./fs.
        // setting is silently dropped and hdfs-native runs with its own
        // defaults. dfs.client.use.datanode.hostname=true is the canonical
        // simple-auth example: valid without Kerberos, but hdfs-native
        // defaults to false and connects to the DataNode's advertised IP
        // instead of its hostname, which commonly fails behind containers/NAT
        // while JNI succeeds. Only the keys HdfsProperties always writes for a
        // credential-free catalog (or already validated above) are proven
        // inert; anything else in these namespaces keeps the catalog on JNI.
        for (Map.Entry<String, String> entry : backendStorageProperties.entrySet()) {
            String key = entry.getKey();
            // A blank value is the unset case (the producer null-filters; a
            // blank site-config entry is inert), matching the identity and HA
            // checks above.
            if (key == null || entry.getValue() == null || entry.getValue().trim().isEmpty()) {
                continue;
            }
            if ((key.startsWith("dfs.") || key.startsWith("hadoop.") || key.startsWith("fs."))
                    && !RUST_VERIFIED_HDFS_OPTION_KEYS.contains(key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Mirrors the pinned paimon-rust {@code DataSplit::is_fully_materialized_pk_dv}: a
     * primary-key split is safe to read raw under deletion vectors only when scan planning
     * marked it raw convertible AND every data file is compacted (level != 0) and known to
     * carry no retract rows (delete_row_count == Some(0)). The rust read_pk path requires
     * this for partial-update / aggregation DV tables; anything weaker must stay on JNI.
     */
    @VisibleForTesting
    static boolean isFullyMaterializedPkDvSplit(DataSplit dataSplit) {
        if (!dataSplit.rawConvertible()) {
            return false;
        }
        for (DataFileMeta fileMeta : dataSplit.dataFiles()) {
            Optional<Long> deleteRowCount = fileMeta.deleteRowCount();
            if (fileMeta.level() == 0 || !deleteRowCount.isPresent() || deleteRowCount.get() != 0L) {
                return false;
            }
        }
        return true;
    }

    private List<String> getOrderedPathPartitionKeys() {
        if (source == null) {
            return Collections.emptyList();
        }
        ExternalTable externalTable = source.getExternalTable();
        if (externalTable instanceof PaimonSysExternalTable
                && !((PaimonSysExternalTable) externalTable).isDataTable()) {
            return Collections.emptyList();
        }
        Table paimonTable = source.getPaimonTable();
        return paimonTable == null ? Collections.emptyList() : paimonTable.partitionKeys();
    }

    private void putHistorySchemaInfo(Long schemaId) {
        if (currentQuerySchema.putIfAbsent(schemaId, Boolean.TRUE) == null) {
            ExternalTable targetTable = source.getExternalTable();
            if (targetTable instanceof PaimonSysExternalTable) {
                PaimonSysExternalTable sysTable = (PaimonSysExternalTable) targetTable;
                if (!sysTable.isDataTable()) {
                    return;
                }
            }

            TableSchema tableSchema;
            if (targetTable instanceof PaimonExternalTable) {
                // Schema IDs are scoped to the resolved relation table, so a branch ID must
                // never be looked up through the base table's schema cache namespace.
                tableSchema = ((DataTable) source.getPaimonTable()).schemaManager().schema(schemaId);
            } else {
                tableSchema = PaimonUtils.getSchemaCacheValue(targetTable, schemaId).getTableSchema();
            }
            params.addToHistorySchemaInfo(PaimonUtil.getHistorySchemaInfo(targetTable, tableSchema,
                    source.getCatalog().getEnableMappingVarbinary(),
                    source.getCatalog().getEnableMappingTimestampTz()));
        }
    }

    @VisibleForTesting
    void setPartitionValues(TFileRangeDesc rangeDesc, Map<String, String> partitionValues) {
        rangeDesc.unsetColumnsFromPathKeys();
        rangeDesc.unsetColumnsFromPath();
        rangeDesc.unsetColumnsFromPathIsNull();

        List<String> orderedPartitionKeys = getOrderedPathPartitionKeys();
        if (orderedPartitionKeys.isEmpty()) {
            return;
        }
        Preconditions.checkState(partitionValues != null,
                "Missing partition values for Paimon partitioned table");

        Map<String, String> normalizedPartitionValues = new HashMap<>();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            normalizedPartitionValues.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
        }

        List<String> fromPathValues = new ArrayList<>(orderedPartitionKeys.size());
        List<Boolean> fromPathIsNull = new ArrayList<>(orderedPartitionKeys.size());
        for (String partitionKey : orderedPartitionKeys) {
            String normalizedPartitionKey = partitionKey.toLowerCase(Locale.ROOT);
            Preconditions.checkState(normalizedPartitionValues.containsKey(normalizedPartitionKey),
                    "Missing partition value for Paimon partition key: %s", partitionKey);
            String partitionValue = normalizedPartitionValues.get(normalizedPartitionKey);
            fromPathValues.add(partitionValue == null ? "" : partitionValue);
            fromPathIsNull.add(partitionValue == null);
        }
        rangeDesc.setColumnsFromPathKeys(orderedPartitionKeys);
        rangeDesc.setColumnsFromPath(fromPathValues);
        rangeDesc.setColumnsFromPathIsNull(fromPathIsNull);
    }

    private void setPaimonParams(TFileRangeDesc rangeDesc, PaimonSplit paimonSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(paimonSplit.getTableFormatType().value());
        TPaimonFileDesc fileDesc = new TPaimonFileDesc();
        org.apache.paimon.table.source.Split split = paimonSplit.getSplit();

        String fileFormat = getFileFormat(paimonSplit.getPathString());
        if (split != null) {
            // use jni reader / paimon-cpp reader / paimon-rust reader
            rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
            // paimon-cpp and paimon-rust both consume Paimon native binary serialization,
            // which only supports DataSplit. Any other split type falls back to JNI.
            boolean nativeSplit = split instanceof DataSplit;
            // Fallback-read splits stay on JNI: FallbackDataSplit extends
            // DataSplit, so the instanceof above passes, but its serializer
            // appends an isFallback byte after the ordinary split that the
            // pinned rust decoder rejects outright ("trailing bytes after
            // DataSplit" — it requires full-buffer consumption), and even a
            // permissive decode would still lack the second table identity
            // needed to honor the fallback-side discriminator. Both sides of a
            // FallbackReadFileStoreTable wrap their splits, so the table
            // wrapper is gated as a whole (any split from it routes to JNI)
            // until the rust ABI represents both sides; the FallbackSplit
            // interface also catches a wrapper split regardless of how the
            // table was resolved here.
            boolean fallbackRead = split instanceof FallbackReadFileStoreTable.FallbackSplit
                    || processedTable instanceof FallbackReadFileStoreTable;
            // Serialize the same effective table that planning and the JNI reader use.
            // Relation options such as t@options('read.batch-size'='1') are applied by
            // getProcessedTable() (doInitialize caches it in processedTable), and the
            // rust reader derives its read batch size from the schema options — the raw
            // cached table would silently drop the override. Copies, delegates and
            // fallback wrappers of getProcessedTable() are still FileStoreTable, so the
            // instanceof gate keeps its semantics.
            Table paimonTable = processedTable;
            FileStoreTable paimonFileStoreTable =
                    paimonTable instanceof FileStoreTable ? (FileStoreTable) paimonTable : null;
            // query-auth.enabled tables stay on JNI: when catalog authorization
            // succeeds with no row filter or column mask, Paimon still leaves an
            // ordinary DataSplit (restricted results use QueryAuthSplit and are
            // already handled by the nativeSplit gate above), so this table shape
            // passes the compound gate — but the shipped schema keeps
            // query-auth.enabled=true and the pinned rust ReadBuilder rejects
            // every such table (its CoreOptions::ensure_read_authorized fails
            // closed because the client cannot enforce the row filter / column
            // masking), turning a valid authorized scan into a BE-open failure.
            // Until the authorization result can be transported and enforced by
            // the rust ABI, these tables route to JNI.
            boolean queryAuthTable = false;
            // REST-token tables stay on JNI: doInitialize snapshots
            // RESTTokenFileIO.validToken().token() into the backend storage
            // properties, discarding expireAtMillis and the REST refresh
            // context, so the shipped credentials look static — but the
            // pinned rust table reuses one option map with no refresh
            // callback, while paimon 1.4.2's JNI RESTTokenFileIO checks
            // expiry before each file operation and obtains a replacement
            // token. A queued or long scan that crosses the token TTL would
            // start on rust and later fail authentication. Gate until the
            // rust ABI can refresh and atomically update credentials.
            boolean restTokenTable = false;
            // Partial-update / aggregation tables with deletion vectors only pass
            // the rust reader in the fully materialized shape: the pinned rust
            // read_pk rejects merge-engine=partial-update/aggregation with
            // deletion-vectors.merge-on-read=true outright, and otherwise requires
            // every split to be compacted and known free of retract rows
            // (DataSplit::is_fully_materialized_pk_dv). Their ordinary DataSplits
            // sail through the compound gate above, so without this check a valid
            // Java/JNI scan reaches BE and the rust open fails. Deduplicate stays
            // rust-eligible: its read_pk routes uncompacted splits to the KV
            // reader, which applies the attached per-file DVs. merge-on-read=true
            // is a table option, so the whole table routes to JNI;
            // non-materialized splits are gated per split below.
            boolean puAggDeletionVectors = false;
            boolean dvMergeOnRead = false;
            boolean deduplicateIgnoreDelete = false;
            boolean rustUnsupportedMergeOption = false;
            if (paimonFileStoreTable != null) {
                // A renewable REST token reached the shipped properties as a
                // plain value; only the table's FileIO type reveals it expires.
                // Null-safe: a table handle whose FileIO is not resolved stays
                // rust-eligible, mirroring the CoreOptions null-safety below.
                restTokenTable = paimonFileStoreTable.fileIO() instanceof RESTTokenFileIO;
                CoreOptions resolvedCoreOptions = paimonFileStoreTable.coreOptions();
                // Null-safe: a table handle whose CoreOptions is not resolved
                // (e.g. some wrapper shapes) stays rust-eligible rather than
                // failing the scan here — the rust open itself rejects such a
                // table if the option is really set.
                if (resolvedCoreOptions != null) {
                    queryAuthTable = resolvedCoreOptions.queryAuthEnabled();
                    CoreOptions.MergeEngine mergeEngine = resolvedCoreOptions.mergeEngine();
                    if (resolvedCoreOptions.deletionVectorsEnabled()
                            && (mergeEngine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                                    || mergeEngine == CoreOptions.MergeEngine.AGGREGATE)) {
                        puAggDeletionVectors = true;
                        // The merge-engine and deletion-vectors.enabled checks
                        // above resolve through the Java CoreOptions accessors,
                        // which the table builds from this same schema options
                        // map — the one the BE rust reader deserializes from
                        // the shipped schema JSON — so they cannot diverge from
                        // what BE sees. merge-on-read has no Java accessor in
                        // paimon 1.4, so it is read raw from the map, with the
                        // rust parsing semantics (any case-insensitive "true"
                        // is on, default false).
                        TableSchema dvSchema = paimonFileStoreTable.schema();
                        Map<String, String> dvOptions = dvSchema == null ? null : dvSchema.options();
                        String mergeOnRead = dvOptions == null
                                ? null : dvOptions.get(DELETION_VECTORS_MERGE_ON_READ);
                        dvMergeOnRead = "true".equalsIgnoreCase(mergeOnRead);
                    }
                    // deduplicate.ignore-delete=true tables stay on JNI:
                    // Java's DeduplicateMergeFunction skips retract records
                    // when the option is set — including old, uncompacted
                    // files that still contain them — but the pinned rust
                    // read_pk does not pass table options into its
                    // deduplicate merge: it picks the latest row and omits
                    // the key when that row is DELETE/UPDATE_BEFORE. An
                    // uncompacted insert followed by a delete therefore
                    // returns the insert through JNI but silently disappears
                    // through rust. Gate the option until the rust merge
                    // implements it.
                    if (mergeEngine == CoreOptions.MergeEngine.DEDUPLICATE
                            && resolvedCoreOptions.ignoreDelete()) {
                        deduplicateIgnoreDelete = true;
                    }
                    // Non-DV merge options the pinned rust read rejects: Java
                    // supports partial-update.remove-record-on-delete /
                    // aggregation.remove-record-on-delete and the wider
                    // per-field retract matrix, but the rust
                    // PartialUpdateConfig / AggregationConfig validations
                    // return Unsupported for them — and the DV-derived gates
                    // above only cover deletion-vector tables, so an ordinary
                    // non-DV DataSplit with one of these options would pass the
                    // compound gate and fail during the rust merge
                    // construction. Mirror the exact rust key matrix (presence,
                    // not values) against the same schema options map BE
                    // deserializes.
                    if (mergeEngine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                            || mergeEngine == CoreOptions.MergeEngine.AGGREGATE) {
                        TableSchema mergeSchema = paimonFileStoreTable.schema();
                        Map<String, String> mergeOptions =
                                mergeSchema == null ? null : mergeSchema.options();
                        rustUnsupportedMergeOption = mergeOptions != null
                                && hasRustUnsupportedMergeOption(mergeOptions, mergeEngine);
                    }
                }
            }
            // paimon-rust additionally requires (a) FileScannerV2: the V1 FileScanner
            // explicitly rejects PAIMON_RUST, so with enable_file_scanner_v2 disabled
            // the split falls back to JNI instead of encoding a rust request that the
            // selected scanner cannot consume, and (b) a FileStoreTable: BE opens the
            // table via paimon_table_from_schema_json, which needs the resolved
            // TableSchema that only FileStoreTable exposes via schema(). If the table
            // is not a FileStoreTable (e.g. a sys table backed by DataSplit), we cannot
            // ship a schema JSON, so fall back to CPP / JNI rather than sending an
            // incomplete PAIMON_RUST request that BE would reject.
            //
            // The paimon-rust S3 bridge maps static credentials, anonymous
            // access (AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS -> s3.anonymous)
            // and assume-role (AWS_ROLE_ARN / AWS_EXTERNAL_ID ->
            // s3.assumed.role.*), but the remaining credential-provider modes
            // are ambient JVM provider chains (ENV, SYSTEM_PROPERTIES,
            // WEB_IDENTITY, CONTAINER, INSTANCE_PROFILE) with no paimon-rust
            // equivalent — rust would silently sign with whatever the ambient
            // chain resolves to. Gate those modes away from the rust reader
            // here so the configured provider is honored via the JNI path.
            boolean providerModeTranslatable = true;
            String providerType = backendStorageProperties == null
                    ? null : backendStorageProperties.get("AWS_CREDENTIALS_PROVIDER_TYPE");
            if (providerType != null) {
                String mode = providerType.trim().toUpperCase(Locale.ROOT);
                providerModeTranslatable = mode.equals("DEFAULT")
                        || mode.equals("ANONYMOUS");
                // The rust OSS FileIO parser (oss:// warehouses) has no
                // skip-signature switch, so an anonymous OSS catalog cannot be
                // served by the rust reader either — fall back to JNI.
                if (mode.equals("ANONYMOUS")) {
                    String location = source.getTableLocation();
                    if (location != null && location.startsWith("oss://")) {
                        providerModeTranslatable = false;
                    }
                }
            }
            // Incremental scans (binlog / changelog / delta / diff) must stay
            // on the JNI path: this wire format carries only an ordinary
            // DataSplit and the rust reader invokes TableRead::to_arrow, but
            // paimon 1.4 marks incremental splits as streaming (which the
            // pinned rust deserializer rejects), diff requires a separate
            // IncrementalPlan instead of an ordinary plan, and ordinary
            // primary-key reads can merge versions rather than return the
            // changes — until the C ABI transports the mode and plan, the
            // rust reader cannot express any of these.
            TableScanParams incrementalParams = getScanParams();
            boolean isIncremental = incrementalParams != null && incrementalParams.incrementalRead();
            // ORC TIMESTAMP_WITH_LOCAL_TIME_ZONE schemas stay on JNI: the pinned
            // paimon-rust ORC decoder materializes LTZ instants shifted by the
            // writer timezone (an upstream crate limitation), so a logical ORC
            // DataSplit that selects rust (e.g. with force_jni_scanner=true or
            // when raw conversion is unavailable) returns a different instant
            // than JNI — applying the session timezone in BE cannot repair an
            // epoch already shifted during decode. Two bypasses are covered:
            // (a) the format must come from EVERY member file — paimon allows
            // per-level file.format, so one DataSplit can mix Parquet and ORC
            // files and the split path's suffix (the first file) would hide
            // the ORC members; (b) the LTZ search must recurse into nested
            // types — an LTZ under MAP/ARRAY/ROW reaches the same shifted ORC
            // decode through the container's field materialization. Parquet
            // files with any LTZ, and ORC without any recursive LTZ, stay
            // rust-eligible. nativeSplit only guards the cast — non-DataSplit
            // splits already route to JNI.
            boolean orcLtzSchema = paimonFileStoreTable != null
                    && nativeSplit
                    && splitHasOrcFile((DataSplit) split)
                    && paimonFileStoreTable.schema().fields().stream()
                            .anyMatch(field -> containsTimestampLtz(field.type()));
            // data-file.external-paths splits stay on JNI (see
            // splitHasExternalFiles): the rust table's single FileIO cannot
            // serve an external file's backend. nativeSplit only guards the
            // cast — non-DataSplit splits already route to JNI.
            boolean externalFileSplit = nativeSplit && splitHasExternalFiles((DataSplit) split);
            // Projected VARIANT columns stay on JNI: the rust leaf feeds its
            // Arrow arrays to the slot serdes, and DataTypeVariantV2SerDe::
            // read_column_from_arrow unconditionally returns
            // NOT_IMPLEMENTED_ERROR — a nested Variant (ARRAY / MAP / STRUCT
            // containing one) reaches the same decoder through the container
            // serdes. desc carries only the slots this query projects, so a
            // table whose VARIANT column is not projected still scans on
            // rust. Gate until the rust leaf has a Variant Arrow decoder.
            boolean projectedVariant = desc.getSlots().stream()
                    .anyMatch(slot -> PaimonUtil.containsVariant(slot.getType()));
            // Scheme capability gate: the pinned paimon-rust storage
            // dispatcher (io/storage.rs) selects the FileIO parser from the
            // table location's URI scheme, and libpaimon_c.a compiles in
            // separate COS, OBS, GCS and Azdls parsers besides the OSS and S3
            // ones. Doris normalizes every object store's credentials into
            // the AWS_* / use_path_style aliases (see the *Properties storage
            // classes), which the BE rust bridge translates only into the
            // fs.oss.* and s3.* key families — a cosn:// / obs:// / gs:// /
            // abfs:// warehouse would reach its scheme's parser without the
            // key family it reads (fs.cosn.userinfo.*, fs.obs.*, gcs.*,
            // azure.*) and fail the open instead of using JNI. Only the
            // schemes whose property translation is implemented and
            // open-tested (s3 / s3a / oss, via RUST_VERIFIED_LOCATION_SCHEMES)
            // plus the credential-free hdfs and local-filesystem parsers stay
            // rust-eligible; every other scheme falls back to JNI. A null
            // location also routes to JNI: the rust path needs the
            // paimon_table that only a real location can provide (BE rejects
            // a split without it).
            boolean schemeCapabilityVerified = isRustVerifiedLocationScheme(source.getTableLocation());
            // An hdfs:// location is scheme-verified only together with the credential-free
            // backend shape: the backend storage properties that ship to BE also carry an
            // HDFS catalog's authentication (kerberos principal / keytab, proxy user, HA
            // nameservice config), none of which the pinned rust HDFS parser reads — the
            // scan would open as the BE process's ambient identity instead of the
            // catalog's configured one and fail the access JNI honors. See
            // isRustVerifiedHdfsBackend.
            boolean hdfsBackendVerified = !isHdfsLocationScheme(source.getTableLocation())
                    || isRustVerifiedHdfsBackend(backendStorageProperties);
            // With merge-on-read=true the whole table already routes to JNI (dvMergeOnRead);
            // for the remaining partial-update/aggregation DV tables, a split that is
            // not fully materialized (uncompacted level-0 data, or retractions not
            // known to be applied — even a split with no deletion file attached yet)
            // fails the rust is_fully_materialized_pk_dv guard, so it falls back per
            // split instead of turning into a BE-open failure. nativeSplit and
            // !fallbackRead only guard the cast — those splits already route to JNI.
            boolean splitDvNotMaterialized = puAggDeletionVectors && !dvMergeOnRead
                    && nativeSplit && !fallbackRead
                    && !isFullyMaterializedPkDvSplit((DataSplit) split);
            boolean canUseRust = sessionVariable.isEnablePaimonRustReader()
                    && sessionVariable.enableFileScannerV2 && nativeSplit && !fallbackRead
                    && !isIncremental && providerModeTranslatable && !queryAuthTable
                    && !restTokenTable
                    && !dvMergeOnRead && !splitDvNotMaterialized
                    && !orcLtzSchema && !projectedVariant && !externalFileSplit
                    && !deduplicateIgnoreDelete && !rustUnsupportedMergeOption
                    && schemeCapabilityVerified && hdfsBackendVerified
                    && paimonFileStoreTable != null;
            if (canUseRust) {
                fileDesc.setReaderType(TPaimonReaderType.PAIMON_RUST);
                fileDesc.setPaimonSplit(PaimonUtil.encodeDataSplitToString((DataSplit) split));
            } else {
                // A logical DataSplit may span multiple files, so keep it intact for the JNI reader.
                fileDesc.setReaderType(TPaimonReaderType.PAIMON_JNI);
                fileDesc.setPaimonSplit(PaimonUtil.encodeObjectToString(split));
            }
            // Set table location for paimon-cpp / paimon-rust reader
            String tableLocation = source.getTableLocation();
            if (tableLocation != null) {
                fileDesc.setPaimonTable(tableLocation);
            }
            // paimon-rust reader opens tables via paimon_table_from_schema_json:
            // ship db/table + resolved TableSchema JSON + non-default branch.
            if (canUseRust) {
                ExternalTable extTable = source.getExternalTable();
                fileDesc.setDbName(extTable.getDbName());
                fileDesc.setTableName(extTable.getName());

                // No catalog / warehouse needed. FE ships the resolved TableSchema JSON
                // and the branch (null-if-main; matches upstream paimon commit 742da63)
                // so BE can skip a schema-file round trip. The FileStoreTable cast is
                // safe here because canUseRust gates on it above. The fence's time-travel
                // selector is stripped before transport: the rust reader pins data via
                // the serialized DataSplit, and a shipped selector (scan.snapshot-id) is
                // re-resolved by paimon-rust's copy_with_time_travel, which would swap
                // these resolved fields for the pinned snapshot's older schema — a
                // column added after the last data commit would then fail projection
                // before per-file schema evolution could fill it (JNI keeps the resolved
                // schema, so stripping restores rust/JNI parity).
                TableSchema tableSchema =
                        PaimonScanParams.withoutTimeTravelSelectors(
                                ((FileStoreTable) paimonTable).schema());
                fileDesc.setPaimonTableSchemaJson(PaimonUtil.encodeTableSchemaToJson(tableSchema));

                String branch = CoreOptions.branch(tableSchema.options());
                if (!Identifier.DEFAULT_MAIN_BRANCH.equals(branch)) {
                    fileDesc.setPaimonBranch(branch);
                }
            }
            rangeDesc.setSelfSplitWeight(paimonSplit.getSelfSplitWeight());
        } else {
            // use native reader
            fileDesc.setReaderType(TPaimonReaderType.PAIMON_NATIVE);
            if (fileFormat.equals("orc")) {
                rangeDesc.setFormatType(TFileFormatType.FORMAT_ORC);
            } else if (fileFormat.equals("parquet")) {
                rangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
            } else {
                throw new RuntimeException("Unsupported file format: " + fileFormat);
            }

            putHistorySchemaInfo(paimonSplit.getSchemaId());
            fileDesc.setSchemaId(paimonSplit.getSchemaId());
        }
        fileDesc.setFileFormat(fileFormat);
        // Hadoop conf is set at ScanNode level via params.properties in createScanRangeLocations(),
        // no need to set it for each split to avoid redundant configuration
        Optional<DeletionFile> optDeletionFile = paimonSplit.getDeletionFile();
        if (optDeletionFile.isPresent()) {
            DeletionFile deletionFile = optDeletionFile.get();
            TPaimonDeletionFileDesc tDeletionFile = new TPaimonDeletionFileDesc();
            // convert the deletion file uri to make sure FileReader can read it in be
            LocationPath locationPath = LocationPath.of(deletionFile.path(), storagePropertiesMap);
            String path = locationPath.toStorageLocation().toString();
            tDeletionFile.setPath(path);
            tDeletionFile.setOffset(deletionFile.offset());
            tDeletionFile.setLength(deletionFile.length());
            fileDesc.setDeletionFile(tDeletionFile);
        }
        if (paimonSplit.getRowCount().isPresent()) {
            tableFormatFileDesc.setTableLevelRowCount(paimonSplit.getRowCount().get());
        } else {
            // MUST explicitly set to -1, to be distinct from valid row count >= 0
            tableFormatFileDesc.setTableLevelRowCount(-1);
        }
        tableFormatFileDesc.setPaimonParams(fileDesc);
        setPartitionValues(rangeDesc, paimonSplit.getPaimonPartitionValues());
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    protected List<String> getDeleteFiles(TFileRangeDesc rangeDesc) {
        List<String> deleteFiles = new ArrayList<>();
        if (rangeDesc == null || !rangeDesc.isSetTableFormatParams()) {
            return deleteFiles;
        }
        TTableFormatFileDesc tableFormatParams = rangeDesc.getTableFormatParams();
        if (tableFormatParams == null || !tableFormatParams.isSetPaimonParams()) {
            return deleteFiles;
        }
        TPaimonFileDesc paimonParams = tableFormatParams.getPaimonParams();
        if (paimonParams == null || !paimonParams.isSetDeletionFile()) {
            return deleteFiles;
        }
        TPaimonDeletionFileDesc deletionFile = paimonParams.getDeletionFile();
        if (deletionFile != null && deletionFile.isSetPath()) {
            // Format: path [offset: offset, length: length]
            deleteFiles.add(deletionFile.getPath());
        }
        return deleteFiles;
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        boolean forceJniScanner = sessionVariable.isForceJniScanner();
        // Paimon system tables need Paimon-side semantics:
        // - binlog: pack/merge + array materialization
        // - audit_log: rowkind / sequence-number projection
        // TODO: Allow native reader after Doris native parquet/orc reader can materialize
        // these system-table rows consistently with Paimon system-table semantics.
        boolean forceJniForSystemTable = shouldForceJniForSystemTable();
        SessionVariable.IgnoreSplitType ignoreSplitType = SessionVariable.IgnoreSplitType
                .valueOf(sessionVariable.getIgnoreSplitType());
        List<Split> splits = new ArrayList<>();
        List<Split> pushDownCountSplits = new ArrayList<>();
        long pushDownCountSum = 0;

        List<org.apache.paimon.table.source.Split> paimonSplits = getPaimonSplitFromAPI();
        List<DataSplit> dataSplits = new ArrayList<>();
        List<org.apache.paimon.table.source.Split> nonDataSplits = new ArrayList<>();
        for (org.apache.paimon.table.source.Split split : paimonSplits) {
            if (split instanceof DataSplit) {
                dataSplits.add((DataSplit) split);
            } else {
                // Non-DataSplit types (e.g., from some system tables) will use JNI reader
                nonDataSplits.add(split);
            }
        }

        // Handle non-DataSplit splits (typically from metadata system tables)
        // These must use JNI reader as they can't be converted to raw files
        for (org.apache.paimon.table.source.Split split : nonDataSplits) {
            if (ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_JNI) {
                continue;
            }
            splits.add(new PaimonSplit(split));
            ++paimonSplitNum;
        }

        // Merged row counts contain only COUNT(*) semantics. COUNT(col) must keep every DataSplit
        // because BE will read the argument column to account for NULL and schema-mapping rules.
        // Incremental binlog readers pack an UPDATE_BEFORE/UPDATE_AFTER pair into one logical row,
        // so DataSplit's physical merged count is not a valid COUNT(*) result for this relation.
        boolean applyCountPushdown = isTableLevelCountStarPushdown() && !isIncrementalBinlogScan();
        // Used to avoid repeatedly calculating partition info map for the same
        // partition data.
        // And for counting the number of selected partitions for this paimon table.
        Map<BinaryRow, Map<String, String>> partitionInfoMaps = new HashMap<>();
        boolean needPartitionMetadata = !getOrderedPathPartitionKeys().isEmpty();
        // if applyCountPushdown is true, we can't split the DataSplit
        boolean hasDeterminedTargetFileSplitSize = false;
        long targetFileSplitSize = 0;
        for (DataSplit dataSplit : dataSplits) {
            SplitStat splitStat = new SplitStat();
            splitStat.setRowCount(dataSplit.rowCount());

            BinaryRow partitionValue = dataSplit.partition();
            Map<String, String> partitionInfoMap = null;
            if (needPartitionMetadata) {
                partitionInfoMap = partitionInfoMaps.computeIfAbsent(partitionValue, k -> {
                    return PaimonUtil.getPartitionInfoMap(
                            source.getPaimonTable(), partitionValue, sessionVariable.getTimeZone());
                });
            } else {
                partitionInfoMaps.put(partitionValue, null);
            }
            Optional<List<RawFile>> optRawFiles = dataSplit.convertToRawFiles();
            Optional<List<DeletionFile>> optDeletionFiles = dataSplit.deletionFiles();
            // Only evaluate merged row counts when COUNT(*) pushdown is active; ordinary scans
            // must not pay the planning cost of merging Paimon manifest statistics.
            OptionalLong mergedRowCount = applyCountPushdown
                    ? dataSplit.mergedRowCount() : OptionalLong.empty();
            if (applyCountPushdown && mergedRowCount.isPresent()) {
                long count = mergedRowCount.getAsLong();
                splitStat.setMergedRowCount(count);
                PaimonSplit split = new PaimonSplit(dataSplit);
                split.setRowCount(count);
                if (partitionInfoMap != null) {
                    split.setPaimonPartitionValues(partitionInfoMap);
                }
                pushDownCountSplits.add(split);
                pushDownCountSum += count;
            } else if (!forceJniScanner && !forceJniForSystemTable && supportNativeReader(optRawFiles)) {
                if (ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_NATIVE) {
                    continue;
                }
                if (!hasDeterminedTargetFileSplitSize) {
                    targetFileSplitSize = determineTargetFileSplitSize(dataSplits, isBatchMode());
                    hasDeterminedTargetFileSplitSize = true;
                }
                splitStat.setType(SplitReadType.NATIVE);
                splitStat.setRawFileConvertable(true);
                List<RawFile> rawFiles = optRawFiles.get();
                for (int i = 0; i < rawFiles.size(); i++) {
                    RawFile file = rawFiles.get(i);
                    LocationPath locationPath = LocationPath.of(file.path(), storagePropertiesMap);
                    try {
                        long splitSize = selectFeSplitSizeForRawFile(
                                file.path(), targetFileSplitSize, !applyCountPushdown);
                        List<Split> dorisSplits = fileSplitter.splitFile(
                                locationPath,
                                splitSize,
                                null,
                                file.length(),
                                -1,
                                !applyCountPushdown,
                                Collections.emptyList(),
                                PaimonSplit.PaimonSplitCreator.DEFAULT);
                        for (Split dorisSplit : dorisSplits) {
                            PaimonSplit paimonSplit = (PaimonSplit) dorisSplit;
                            paimonSplit.setSchemaId(file.schemaId());
                            paimonSplit.setPaimonPartitionValues(partitionInfoMap);
                            // try to set deletion file
                            if (optDeletionFiles.isPresent() && optDeletionFiles.get().get(i) != null) {
                                paimonSplit.setDeletionFile(optDeletionFiles.get().get(i));
                                splitStat.setHasDeletionVector(true);
                            }
                        }
                        splits.addAll(dorisSplits);
                        ++rawFileSplitNum;
                    } catch (IOException e) {
                        throw new UserException("Paimon error to split file: " + e.getMessage(), e);
                    }
                }
            } else {
                if (ignoreSplitType == SessionVariable.IgnoreSplitType.IGNORE_JNI) {
                    continue;
                }
                PaimonSplit jniSplit = new PaimonSplit(dataSplit);
                jniSplit.setPaimonPartitionValues(partitionInfoMap);
                splits.add(jniSplit);
                ++paimonSplitNum;
            }

            splitStats.add(splitStat);
        }

        // if applyCountPushdown is true, calcute row count for count pushdown
        if (applyCountPushdown && !pushDownCountSplits.isEmpty()) {
            if (pushDownCountSum > COUNT_WITH_PARALLEL_SPLITS) {
                int minSplits = sessionVariable.getParallelExecInstanceNum(scanContext.getClusterName())
                        * numBackends;
                pushDownCountSplits = pushDownCountSplits.subList(0, Math.min(pushDownCountSplits.size(), minSplits));
            } else {
                pushDownCountSplits = Collections.singletonList(pushDownCountSplits.get(0));
            }
            setPushDownCount(pushDownCountSum);
            assignCountToSplits(pushDownCountSplits, pushDownCountSum);
            splits.addAll(pushDownCountSplits);
        }

        // We need to set the target size for all splits so that we can calculate the
        // proportion of each split later.
        long legacyWeightTarget = sessionVariable.getFileSplitSize() > 0
                ? sessionVariable.getFileSplitSize() : sessionVariable.getMaxSplitSize();
        splits.stream().filter(s -> ((PaimonSplit) s).getTargetSplitSize() == null)
                .forEach(s -> s.setTargetSplitSize(legacyWeightTarget));

        this.selectedPartitionNum = partitionInfoMaps.size();
        return splits;
    }

    @VisibleForTesting
    Map<String, String> getBackendPaimonOptions() {
        if (source == null) {
            return Collections.emptyMap();
        }
        if (!(source.getCatalog() instanceof PaimonExternalCatalog)) {
            return Collections.emptyMap();
        }
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) source.getCatalog();
        Map<String, String> backendOptions = new HashMap<>();
        Map<String, String> catalogProperties = catalog.getCatalogProperty().getProperties();
        if (catalogProperties == null) {
            catalogProperties = Collections.emptyMap();
        }
        for (String option : BACKEND_PAIMON_OPTIONS) {
            String catalogProperty = PAIMON_PROPERTY_PREFIX + option;
            if (catalogProperties.containsKey(catalogProperty)) {
                backendOptions.put(option, catalogProperties.get(catalogProperty));
            }
        }
        if (!(catalog.getCatalogProperty().getMetastoreProperties() instanceof PaimonJdbcMetaStoreProperties)) {
            return backendOptions;
        }
        PaimonJdbcMetaStoreProperties jdbcMetaStoreProperties =
                (PaimonJdbcMetaStoreProperties) catalog.getCatalogProperty().getMetastoreProperties();
        backendOptions.putAll(jdbcMetaStoreProperties.getBackendPaimonOptions());
        return backendOptions;
    }

    @VisibleForTesting
    boolean shouldForceJniForSystemTable() {
        if (source == null) {
            return false;
        }
        ExternalTable externalTable = source.getExternalTable();
        if (!(externalTable instanceof PaimonSysExternalTable)) {
            return false;
        }
        PaimonSysExternalTable paimonSysExternalTable = (PaimonSysExternalTable) externalTable;
        String sysTableType = paimonSysExternalTable.getSysTableType();
        return PaimonScanParams.requiresPaimonReader(sysTableType);
    }

    private boolean isIncrementalBinlogScan() {
        TableScanParams params = getScanParams();
        if (params == null || !params.incrementalRead() || source == null) {
            return false;
        }
        ExternalTable externalTable = source.getExternalTable();
        return externalTable instanceof PaimonSysExternalTable
                && "binlog".equalsIgnoreCase(((PaimonSysExternalTable) externalTable).getSysTableType());
    }

    private long determineTargetFileSplitSize(List<DataSplit> dataSplits,
            boolean isBatchMode) {
        if (sessionVariable.getFileSplitSize() > 0) {
            return sessionVariable.getFileSplitSize();
        }
        /** Paimon batch split mode will return 0. and <code>FileSplitter</code>
         *  will determine file split size.
         */
        if (isBatchMode) {
            return 0;
        }
        long result = sessionVariable.getMaxInitialSplitSize();
        long totalFileSize = 0;
        boolean exceedInitialThreshold = false;
        for (DataSplit dataSplit : dataSplits) {
            Optional<List<RawFile>> rawFiles = dataSplit.convertToRawFiles();
            if (!supportNativeReader(rawFiles)) {
                continue;
            }
            for (RawFile rawFile : rawFiles.get()) {
                totalFileSize += rawFile.fileSize();
                if (!exceedInitialThreshold && totalFileSize
                        >= sessionVariable.getMaxSplitSize() * sessionVariable.getMaxInitialSplitNum()) {
                    exceedInitialThreshold = true;
                }
            }
        }
        result = exceedInitialThreshold ? sessionVariable.getMaxSplitSize() : result;
        result = applyMaxFileSplitNumLimit(result, totalFileSize);
        return result;
    }

    @VisibleForTesting
    public Map<String, String> getIncrReadParams() throws UserException {
        Map<String, String> paimonScanParams = new HashMap<>();
        if (scanParams != null && scanParams.incrementalRead()) {
            // Validate parameter combinations and get the result map
            paimonScanParams = validateIncrementalReadParams(scanParams.getMapParams());
        }
        return paimonScanParams;
    }

    @VisibleForTesting
    public List<org.apache.paimon.table.source.Split> getPaimonSplitFromAPI() throws UserException {
        long startTime = System.currentTimeMillis();
        try {
            Optional<MvccSnapshot> relationSnapshot = getRelationSnapshot();
            if (!(source.getExternalTable() instanceof PaimonSysExternalTable)
                    && relationSnapshot.isPresent() && relationSnapshot.get() instanceof PaimonMvccSnapshot
                    && ((PaimonMvccSnapshot) relationSnapshot.get()).getSnapshotCacheValue()
                            .getSnapshot().getSnapshotId() == PaimonSnapshot.INVALID_SNAPSHOT_ID) {
                // An empty data snapshot is a bound generation, but metadata system tables such as
                // $schemas can still contain rows and must plan their own independent row domain.
                return Collections.emptyList();
            }
            Table paimonTable = getProcessedTable();
            Map<String, String> resolvedOptions = scanParams == null
                    ? Collections.emptyMap()
                    : scanParams.getResolvedMapParams().orElse(Collections.emptyMap());
            if (PaimonScanParams.isPinnedEmptyScan(resolvedOptions)) {
                return Collections.emptyList();
            }
            int[] projectedColumns = new int[0];
            if (!PaimonScanParams.getPinnedFileCreationTime(resolvedOptions).isPresent()) {
                List<String> fieldNames = paimonTable.rowType().getFieldNames();
                projectedColumns = desc.getSlots().stream().mapToInt(
                        slot -> getFieldIndex(fieldNames, slot.getColumn().getName()))
                        .toArray();
                if (Arrays.stream(projectedColumns).anyMatch(index -> index < 0)) {
                    throw new UserException("Paimon scan schema does not contain all bound Doris columns.");
                }
            }
            int[] projected = projectedColumns;
            PaimonSplitTaskCacheKey cacheKey = createPaimonSplitTaskCacheKey(
                    relationSnapshot, paimonTable, resolvedOptions,
                    scanParams != null && scanParams.incrementalRead()
                            ? getIncrReadParams() : Collections.emptyMap(),
                    projected);
            if (!canReuseExternalScanTasks()
                    && !(source.getExternalTable() instanceof PaimonSysExternalTable)) {
                // Reuse is off (or no statement cache): consume the native splits directly, so an
                // opt-out never pays the serialization cost of the cache path. System tables keep
                // the serialized path: their splits must be isolated copies for the JNI reader.
                return planPaimonSplits(paimonTable, resolvedOptions, projected);
            }
            List<PaimonSerializedScanTask> serializedSplits;
            try {
                serializedSplits = getOrLoadExternalScanTasks(cacheKey,
                        remainingBytes -> serializePaimonSplitsWithinLimit(
                                planPaimonSplits(paimonTable, resolvedOptions, projected), remainingBytes),
                        PaimonScanNode::serializedTaskBytes,
                        StatementContext.ExternalScanTaskCache.WeightBudget.PAIMON_SERIALIZED_BYTES,
                        maxRetainedSerializedTaskBytes, maxRetainedSerializedTaskBytes, true);
            } catch (PaimonTaskCacheLimitException e) {
                List<org.apache.paimon.table.source.Split> uncachedSplits = e.takePlannedSplits();
                return uncachedSplits == null
                        ? planPaimonSplits(paimonTable, resolvedOptions, projected)
                        : uncachedSplits;
            }
            return serializedSplits.stream()
                    .map(PaimonSerializedScanTask::deserialize)
                    .collect(Collectors.toList());
        } catch (UserException e) {
            throw e;
        } catch (Exception e) {
            throw new UserException("Failed to plan Paimon scan tasks", e);
        } finally {
            if (getSummaryProfile() != null) {
                getSummaryProfile().addExternalTableGetFileScanTasksTime(System.currentTimeMillis() - startTime);
            }
        }
    }

    private List<org.apache.paimon.table.source.Split> planPaimonSplits(
            Table paimonTable, Map<String, String> resolvedOptions, int[] projected) throws UserException {
        Optional<Long> fileCreationTime = PaimonScanParams.getPinnedFileCreationTime(resolvedOptions);
        if (fileCreationTime.isPresent()) {
            if (!(paimonTable instanceof FileStoreTable)) {
                throw new UserException("Paimon file-creation OPTIONS require a data table.");
            }
            FileStoreTable fileStoreTable = (FileStoreTable) paimonTable;
            SnapshotReader snapshotReader = fileStoreTable.newSnapshotReader()
                    .withMode(ScanMode.ALL)
                    .withSnapshot(Long.parseLong(
                            paimonTable.options().get(CoreOptions.SCAN_SNAPSHOT_ID.key())))
                    .withManifestEntryFilter(entry ->
                            entry.file().creationTimeEpochMillis() >= fileCreationTime.get());
            preserveBatchScanFilters(fileStoreTable, snapshotReader);
            if (predicates != null) {
                predicates.forEach(snapshotReader::withFilter);
            }
            return snapshotReader.read().splits();
        }
        ReadBuilder readBuilder = paimonTable.newReadBuilder();
        TableScan scan = readBuilder.withFilter(predicates)
                .withProjection(projected)
                .newScan();
        PaimonMetricRegistry registry = new PaimonMetricRegistry();
        if (scan instanceof InnerTableScan) {
            scan = ((InnerTableScan) scan).withMetricRegistry(registry);
        }
        List<org.apache.paimon.table.source.Split> splits = scan.plan().splits();
        PaimonScanMetricsReporter.report(source.getTargetTable(), paimonTable.name(), registry);
        if (!registry.getAllGroups().isEmpty()) {
            registry.clear();
        }
        return splits;
    }

    private PaimonSplitTaskCacheKey createPaimonSplitTaskCacheKey(
            Optional<MvccSnapshot> relationSnapshot, Table paimonTable,
            Map<String, String> resolvedOptions, Map<String, String> incrementalOptions,
            int[] projected) {
        Long snapshotId = null;
        Long schemaId = null;
        if (relationSnapshot.isPresent() && relationSnapshot.get() instanceof PaimonMvccSnapshot) {
            PaimonSnapshot snapshot = ((PaimonMvccSnapshot) relationSnapshot.get())
                    .getSnapshotCacheValue().getSnapshot();
            snapshotId = snapshot.getSnapshotId();
            schemaId = snapshot.getSchemaId();
        }
        return new PaimonSplitTaskCacheKey(
                source.getCatalog().getId(),
                source.getExternalTable().getId(),
                source.getTargetTable().getId(),
                snapshotId,
                schemaId,
                scanParams == null ? null : scanParams.getParamType(),
                scanParams == null ? Collections.emptyList() : scanParams.getListParams(),
                resolvedOptions,
                incrementalOptions,
                paimonTable.options(),
                projected,
                PaimonUtil.encodeObjectToString(predicates));
    }

    @VisibleForTesting
    void setMaxRetainedSerializedTaskBytes(long maxRetainedSerializedTaskBytes) {
        this.maxRetainedSerializedTaskBytes = maxRetainedSerializedTaskBytes;
    }

    private List<PaimonSerializedScanTask> serializePaimonSplitsWithinLimit(
            List<org.apache.paimon.table.source.Split> splits, long maxSerializedBytes) {
        List<PaimonSerializedScanTask> serializedTasks = new ArrayList<>();
        long serializedBytes = 0;
        for (org.apache.paimon.table.source.Split split : splits) {
            Optional<byte[]> serializedSplit = PaimonUtil.serializeObjectWithinLimit(
                    split, maxSerializedBytes - serializedBytes);
            if (!serializedSplit.isPresent()) {
                throw new PaimonTaskCacheLimitException(splits);
            }
            PaimonSerializedScanTask task = new PaimonSerializedScanTask(serializedSplit.get());
            serializedBytes += task.serializedSize();
            serializedTasks.add(task);
        }
        return serializedTasks;
    }

    private static long serializedTaskBytes(List<PaimonSerializedScanTask> tasks) {
        return Math.max(1, tasks.stream().mapToLong(PaimonSerializedScanTask::serializedSize).sum());
    }

    private static final class PaimonSplitTaskCacheKey
            implements ExternalScanTaskCacheKey<PaimonSerializedScanTask> {
        private final long catalogId;
        private final long relationTableId;
        private final long targetTableId;
        private final Long snapshotId;
        private final Long schemaId;
        private final String scanParamType;
        private final List<String> listParams;
        private final Map<String, String> resolvedOptions;
        private final Map<String, String> incrementalOptions;
        private final Map<String, String> tableOptions;
        private final int[] projected;
        private final String serializedPredicates;

        private PaimonSplitTaskCacheKey(
                long catalogId, long relationTableId, long targetTableId, Long snapshotId, Long schemaId,
                String scanParamType, List<String> listParams,
                Map<String, String> resolvedOptions, Map<String, String> incrementalOptions,
                Map<String, String> tableOptions, int[] projected,
                String serializedPredicates) {
            this.catalogId = catalogId;
            this.relationTableId = relationTableId;
            this.targetTableId = targetTableId;
            this.snapshotId = snapshotId;
            this.schemaId = schemaId;
            this.scanParamType = scanParamType;
            this.listParams = Collections.unmodifiableList(new ArrayList<>(listParams));
            this.resolvedOptions = Collections.unmodifiableMap(new HashMap<>(resolvedOptions));
            this.incrementalOptions = Collections.unmodifiableMap(new HashMap<>(incrementalOptions));
            this.tableOptions = Collections.unmodifiableMap(new HashMap<>(tableOptions));
            this.projected = Arrays.copyOf(projected, projected.length);
            this.serializedPredicates = serializedPredicates;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof PaimonSplitTaskCacheKey)) {
                return false;
            }
            PaimonSplitTaskCacheKey that = (PaimonSplitTaskCacheKey) object;
            return catalogId == that.catalogId
                    && relationTableId == that.relationTableId
                    && targetTableId == that.targetTableId
                    && Objects.equals(snapshotId, that.snapshotId)
                    && Objects.equals(schemaId, that.schemaId)
                    && Objects.equals(scanParamType, that.scanParamType)
                    && listParams.equals(that.listParams)
                    && resolvedOptions.equals(that.resolvedOptions)
                    && incrementalOptions.equals(that.incrementalOptions)
                    && tableOptions.equals(that.tableOptions)
                    && Arrays.equals(projected, that.projected)
                    && serializedPredicates.equals(that.serializedPredicates);
        }

        @Override
        public int hashCode() {
            return 31 * Objects.hash(
                    catalogId, relationTableId, targetTableId, snapshotId, schemaId, scanParamType, listParams,
                    resolvedOptions, incrementalOptions, tableOptions, serializedPredicates)
                    + Arrays.hashCode(projected);
        }
    }

    private static final class PaimonSerializedScanTask {
        private final byte[] serializedSplit;

        private PaimonSerializedScanTask(byte[] serializedSplit) {
            this.serializedSplit = serializedSplit;
        }

        private org.apache.paimon.table.source.Split deserialize() {
            return PaimonUtil.deserializeObject(Arrays.copyOf(serializedSplit, serializedSplit.length));
        }

        private int serializedSize() {
            return serializedSplit.length;
        }
    }

    private static final class PaimonTaskCacheLimitException extends RuntimeException {
        private List<org.apache.paimon.table.source.Split> plannedSplits;

        private PaimonTaskCacheLimitException(List<org.apache.paimon.table.source.Split> plannedSplits) {
            this.plannedSplits = plannedSplits;
        }

        private synchronized List<org.apache.paimon.table.source.Split> takePlannedSplits() {
            List<org.apache.paimon.table.source.Split> splits = plannedSplits;
            plannedSplits = null;
            return splits;
        }
    }

    private void preserveBatchScanFilters(FileStoreTable table, SnapshotReader snapshotReader) {
        CoreOptions options = table.coreOptions();
        // This direct reader bypasses DataTableBatchScan, so preserve its correctness filters for
        // deletion-vector/first-row tables and postponed buckets before reading the pinned plan.
        if (!table.primaryKeys().isEmpty()
                && options.batchScanSkipLevel0()
                && options.toConfiguration().get(CoreOptions.BATCH_SCAN_MODE) == CoreOptions.BatchScanMode.NONE) {
            snapshotReader.withLevelFilter(level -> level > 0).enableValueFilter();
        }
        if (options.bucket() == BucketMode.POSTPONE_BUCKET) {
            snapshotReader.onlyReadRealBuckets();
        }
    }

    @VisibleForTesting
    static int getFieldIndex(List<String> fieldNames, String columnName) {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldNames.get(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private String getFileFormat(String path) {
        return FileFormatUtils.getFileFormatBySuffix(path).orElse(source.getFileFormatFromTableProperties());
    }

    @VisibleForTesting
    long selectFeSplitSizeForRawFile(String path, long fallbackSize, boolean supportsBeSplit) {
        // Unsupported semantic paths must not require native-file metadata just to keep the
        // connector's legacy split target.
        if (!supportsBeSplit) {
            return fallbackSize;
        }
        String format = FileFormatUtils.getFileFormatBySuffix(path)
                .orElseGet(() -> source.getFileFormatFromTableProperties());
        TFileFormatType thriftFormat;
        if ("parquet".equals(format)) {
            thriftFormat = TFileFormatType.FORMAT_PARQUET;
        } else if ("orc".equals(format)) {
            thriftFormat = TFileFormatType.FORMAT_ORC;
        } else {
            return fallbackSize;
        }
        return selectFeSplitSizeForBe(fallbackSize, thriftFormat, true);
    }

    @VisibleForTesting
    public boolean supportNativeReader(Optional<List<RawFile>> optRawFiles) {
        if (!optRawFiles.isPresent()) {
            return false;
        }
        List<String> files = optRawFiles.get().stream().map(RawFile::path).collect(Collectors.toList());
        for (String f : files) {
            String splitFileFormat = getFileFormat(f);
            if (!splitFileFormat.equals("orc") && !splitFileFormat.equals("parquet")) {
                return false;
            }
        }
        return true;
    }

    @Override
    public TFileFormatType getFileFormatType() throws DdlException, MetaNotFoundException {
        return TFileFormatType.FORMAT_JNI;
    }

    @Override
    public List<String> getPathPartitionKeys() throws DdlException, MetaNotFoundException {
        return getOrderedPathPartitionKeys();
    }

    @Override
    public TableIf getTargetTable() {
        return desc.getTable();
    }

    @Override
    protected Map<String, String> getLocationProperties() {
        return backendStorageProperties;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder sb = new StringBuilder(super.getNodeExplainString(prefix, detailLevel));
        sb.append(String.format("%spaimonNativeReadSplits=%d/%d\n",
                prefix, rawFileSplitNum, (paimonSplitNum + rawFileSplitNum)));

        sb.append(prefix).append("predicatesFromPaimon:");
        if (predicates.isEmpty()) {
            sb.append(" NONE\n");
        } else {
            sb.append("\n");
            for (Predicate predicate : predicates) {
                sb.append(prefix).append(prefix).append(predicate).append("\n");
            }
        }

        if (detailLevel == TExplainLevel.VERBOSE) {
            sb.append(prefix).append("PaimonSplitStats: \n");
            int size = splitStats.size();
            if (size <= 4) {
                for (SplitStat splitStat : splitStats) {
                    sb.append(String.format("%s  %s\n", prefix, splitStat));
                }
            } else {
                for (int i = 0; i < 3; i++) {
                    SplitStat splitStat = splitStats.get(i);
                    sb.append(String.format("%s  %s\n", prefix, splitStat));
                }
                int other = size - 4;
                sb.append(prefix).append("  ... other ").append(other).append(" paimon split stats ...\n");
                SplitStat split = splitStats.get(size - 1);
                sb.append(String.format("%s  %s\n", prefix, split));
            }
        }
        return sb.toString();
    }

    private void assignCountToSplits(List<Split> splits, long totalCount) {
        int size = splits.size();
        long countPerSplit = totalCount / size;
        for (int i = 0; i < size - 1; i++) {
            ((PaimonSplit) splits.get(i)).setRowCount(countPerSplit);
        }
        ((PaimonSplit) splits.get(size - 1)).setRowCount(countPerSplit + totalCount % size);
    }

    @VisibleForTesting
    public static Map<String, String> validateIncrementalReadParams(Map<String, String> params) throws UserException {
        // Check if snapshot-based parameters exist
        boolean hasStartSnapshotId = params.containsKey(DORIS_START_SNAPSHOT_ID)
                && params.get(DORIS_START_SNAPSHOT_ID) != null;
        boolean hasEndSnapshotId = params.containsKey(DORIS_END_SNAPSHOT_ID)
                && params.get(DORIS_END_SNAPSHOT_ID) != null;
        boolean hasIncrementalBetweenScanMode = params.containsKey(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE)
                && params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE) != null;

        // Check if timestamp-based parameters exist
        boolean hasStartTimestamp = params.containsKey(DORIS_START_TIMESTAMP)
                && params.get(DORIS_START_TIMESTAMP) != null;
        boolean hasEndTimestamp = params.containsKey(DORIS_END_TIMESTAMP) && params.get(DORIS_END_TIMESTAMP) != null;

        // Check if any snapshot-based parameters are present
        boolean hasSnapshotParams = hasStartSnapshotId || hasEndSnapshotId || hasIncrementalBetweenScanMode;

        // Check if any timestamp-based parameters are present
        boolean hasTimestampParams = hasStartTimestamp || hasEndTimestamp;

        // Rule 2: The two groups are mutually exclusive
        if (hasSnapshotParams && hasTimestampParams) {
            throw new UserException(
                    "Cannot specify both snapshot-based parameters"
                            + "(startSnapshotId, endSnapshotId, incrementalBetweenScanMode) "
                            + "and timestamp-based parameters (startTimestamp, endTimestamp) at the same time");
        }

        // Validate snapshot-based parameters group
        if (hasSnapshotParams) {
            // Rule 3.1 & 3.2: DORIS_START_SNAPSHOT_ID is required
            if (!hasStartSnapshotId) {
                throw new UserException("startSnapshotId is required when using snapshot-based incremental read");
            }

            // Rule 3.3: DORIS_INCREMENTAL_BETWEEN_SCAN_MODE can only appear
            // when both start and end snapshot IDs are specified
            if (hasIncrementalBetweenScanMode && (!hasStartSnapshotId || !hasEndSnapshotId)) {
                throw new UserException(
                        "incrementalBetweenScanMode can only be specified when"
                                + " both startSnapshotId and endSnapshotId are provided");
            }

            // Validate snapshot ID values
            if (hasStartSnapshotId) {
                try {
                    long startSId = Long.parseLong(params.get(DORIS_START_SNAPSHOT_ID));
                    if (startSId < 0) {
                        throw new UserException("startSnapshotId must be greater than or equal to 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid startSnapshotId format: " + e.getMessage());
                }
            }

            if (hasEndSnapshotId) {
                try {
                    long endSId = Long.parseLong(params.get(DORIS_END_SNAPSHOT_ID));
                    if (endSId < 0) {
                        throw new UserException("endSnapshotId must be greater than or equal to 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid endSnapshotId format: " + e.getMessage());
                }
            }

            // Check if both snapshot IDs are present and validate their relationship
            if (hasStartSnapshotId && hasEndSnapshotId) {
                try {
                    long startSId = Long.parseLong(params.get(DORIS_START_SNAPSHOT_ID));
                    long endSId = Long.parseLong(params.get(DORIS_END_SNAPSHOT_ID));
                    if (startSId > endSId) {
                        throw new UserException("startSnapshotId must be less than or equal to endSnapshotId");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid snapshot ID format: " + e.getMessage());
                }
            }

            // Validate DORIS_INCREMENTAL_BETWEEN_SCAN_MODE
            if (hasIncrementalBetweenScanMode) {
                String scanMode = params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE).toLowerCase();
                if (!scanMode.equals("auto") && !scanMode.equals("diff")
                        && !scanMode.equals("delta") && !scanMode.equals("changelog")) {
                    throw new UserException("incrementalBetweenScanMode must be one of: auto, diff, delta, changelog");
                }
            }
        }

        // Validate timestamp-based parameters group
        if (hasTimestampParams) {
            // Rule 4.1 & 4.2: DORIS_START_TIMESTAMP is required
            if (!hasStartTimestamp) {
                throw new UserException("startTimestamp is required when using timestamp-based incremental read");
            }

            // Validate timestamp values
            if (hasStartTimestamp) {
                try {
                    long startTS = Long.parseLong(params.get(DORIS_START_TIMESTAMP));
                    if (startTS < 0) {
                        throw new UserException("startTimestamp must be greater than or equal to 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid startTimestamp format: " + e.getMessage());
                }
            }

            if (hasEndTimestamp) {
                try {
                    long endTS = Long.parseLong(params.get(DORIS_END_TIMESTAMP));
                    if (endTS <= 0) {
                        throw new UserException("endTimestamp must be greater than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid endTimestamp format: " + e.getMessage());
                }
            }

            // Check if both timestamps are present and validate their relationship
            if (hasStartTimestamp && hasEndTimestamp) {
                try {
                    long startTS = Long.parseLong(params.get(DORIS_START_TIMESTAMP));
                    long endTS = Long.parseLong(params.get(DORIS_END_TIMESTAMP));
                    if (startTS >= endTS) {
                        throw new UserException("startTimestamp must be less than endTimestamp");
                    }
                } catch (NumberFormatException e) {
                    throw new UserException("Invalid timestamp format: " + e.getMessage());
                }
            }
        }

        // If no incremental parameters are provided at all, that's also invalid in this context
        if (!hasSnapshotParams && !hasTimestampParams) {
            throw new UserException(
                    "Invalid paimon incremental read params: at least one valid parameter group must be specified");
        }

        // Fill the result map based on parameter combinations
        Map<String, String> paimonScanParams = new HashMap<>();

        if (hasSnapshotParams) {
            if (hasStartSnapshotId && !hasEndSnapshotId) {
                // Only startSnapshotId is specified
                throw new UserException("endSnapshotId is required when using snapshot-based incremental read");
            } else if (hasStartSnapshotId && hasEndSnapshotId) {
                // Both start and end snapshot IDs are specified
                String startSId = params.get(DORIS_START_SNAPSHOT_ID);
                String endSId = params.get(DORIS_END_SNAPSHOT_ID);
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN, startSId + "," + endSId);
            }

            // Add incremental between scan mode if present
            if (hasIncrementalBetweenScanMode) {
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_SCAN_MODE,
                        params.get(DORIS_INCREMENTAL_BETWEEN_SCAN_MODE));
            }
        }

        if (hasTimestampParams) {
            String startTS = params.get(DORIS_START_TIMESTAMP);
            String endTS = params.get(DORIS_END_TIMESTAMP);

            if (hasStartTimestamp && !hasEndTimestamp) {
                // Only startTimestamp is specified
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, startTS + "," + Long.MAX_VALUE);
            } else if (hasStartTimestamp && hasEndTimestamp) {
                // Both start and end timestamps are specified
                paimonScanParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, startTS + "," + endTS);
            }
        }

        return PaimonScanParams.isolateIncrementalRead(paimonScanParams);
    }

    private Table getProcessedTable() throws UserException {
        if (processedTable != null) {
            return processedTable;
        }
        TableScanParams theScanParams = getScanParams();
        Table baseTable = source.getPaimonTable();
        PaimonSysExternalTable systemTable = null;
        if (source.getExternalTable() instanceof PaimonSysExternalTable) {
            systemTable = (PaimonSysExternalTable) source.getExternalTable();
            try {
                PaimonScanParams.validateSystemTable(systemTable.getSysTableType(), theScanParams);
            } catch (IllegalArgumentException e) {
                throw new UserException(e.getMessage(), e);
            }
            if (getQueryTableSnapshot() != null) {
                throw new UserException("Paimon system tables do not support time travel.");
            }
        }
        if (theScanParams != null && getQueryTableSnapshot() != null) {
            throw new UserException("Can not specify scan params and table snapshot at same time.");
        }

        Table finalTable;
        if (theScanParams != null && theScanParams.incrementalRead()) {
            if (systemTable != null) {
                // System wrappers hide the manifest-planning data table. Start paths that copy the
                // wrapper directly from a disposable CPU-capped handle.
                baseTable = source.getPaimonTable(null);
            }
            // System table handles are cached, so preserve query isolation by applying dynamic
            // options to a copied Paimon table instead of changing the shared handle.
            finalTable = baseTable.copy(getIncrReadParams());
        } else if (theScanParams != null && theScanParams.isOptions()) {
            try {
                finalTable = source.getPaimonTable(theScanParams);
            } catch (IllegalArgumentException e) {
                throw new UserException(e.getMessage(), e);
            }
        } else {
            finalTable = systemTable == null ? baseTable : source.getPaimonTable(null);
        }
        try {
            // This is the last common boundary before planning and serialization, including scans
            // with no relation copy and incremental/system-table paths that bypass applyOptions.
            finalTable = PaimonReaderOptions.runtimeSafeTable(finalTable);
            PaimonReaderOptions.validateEffectiveTable(finalTable);
            if (source.getExternalTable() instanceof PaimonSysExternalTable) {
                // Read-only system wrappers hide the data table that performs manifest planning.
                source.validateEffectiveSystemDataTable(theScanParams);
            }
        } catch (IllegalArgumentException e) {
            throw new UserException(e.getMessage(), e);
        }
        return finalTable;
    }
}
