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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.PaimonUtils;
import org.apache.doris.datasource.paimon.profile.PaimonMetricRegistry;
import org.apache.doris.datasource.paimon.profile.PaimonScanMetricsReporter;
import org.apache.doris.datasource.property.metastore.PaimonJdbcMetaStoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
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
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
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
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.utils.SnapshotManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PaimonScanNode extends FileQueryScanNode {
    private static final Logger LOG = LogManager.getLogger(PaimonScanNode.class);

    private static final long COUNT_WITH_PARALLEL_SPLITS = 10000;
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
    private static final List<String> BACKEND_PAIMON_OPTIONS = Arrays.asList(
            DORIS_ENABLE_JNI_IO_MANAGER,
            DORIS_JNI_IO_MANAGER_TMP_DIR,
            DORIS_JNI_IO_MANAGER_IMPL_CLASS,
            DORIS_ENABLE_FILE_READER_ASYNC);
    private static final String PAIMON_FILES_SYSTEM_TABLE_TYPE = "files";
    private static final String PAIMON_PARTITIONS_SYSTEM_TABLE_TYPE = "partitions";
    private static final String PAIMON_MANIFESTS_SYSTEM_TABLE_TYPE = "manifests";
    private static final String PAIMON_STATISTICS_SYSTEM_TABLE_TYPE = "statistics";
    private static final String PAIMON_TABLE_INDEXES_SYSTEM_TABLE_TYPE = "table_indexes";

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
        if (source == null) {
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
        backendPaimonOptions = getBackendPaimonOptions();
        if (getSummaryProfile() != null) {
            getSummaryProfile().addExternalTableGetTableMetaTime(System.currentTimeMillis() - startTime);
        }
    }

    private void serializeProcessedTable() throws UserException {
        // System-table splits are materialized by the BE JNI reader, so it must receive the same
        // option-bearing table copy that FE uses to plan the split.
        serializedTable = PaimonUtil.encodeObjectToString(getPaimonTableForBackend());
    }

    /**
     * Build the Paimon table object that is serialized to the BE.
     *
     * <p>Every table loaded from a metastore-backed Paimon catalog (HMS / DLF) carries a Paimon
     * {@code HiveCatalogLoader} in its {@link org.apache.paimon.table.CatalogEnvironment}. The BE
     * only reads — via FE-resolved splits and the object store — and never needs the catalog, yet
     * deserializing that loader forces the whole Hive metastore stack onto the BE classpath:
     * {@code HiveConf}, the metastore API, and, when a system table resolves its latest snapshot,
     * even the metastore client (DLF's {@code ProxyMetaStoreClient} and its REST stack). So we
     * serialize a catalog-less table to the BE:
     * <ul>
     *   <li>data table: drop the catalog loader. A {@link FileStoreTable} is fully defined by
     *       fileIO / location / schema / catalogEnvironment, and its dynamic options (time travel,
     *       incremental) are merged into the schema by {@code copy(...)}, so rebuilding from
     *       fileIO / location / schema preserves everything except the catalog loader.</li>
     *   <li>system table (e.g. {@code $snapshots}): rebuild it over a catalog-less data table so
     *       {@code SnapshotManager#latestSnapshotId} lists the snapshot directory on the filesystem
     *       instead of calling the metastore. The base table is the one the FE-side wrapper was
     *       built over, and for the system tables that pick their snapshot on the BE ({@link
     *       #resolvesSnapshotOnBackend}) what the catalog would have done there is done here
     *       instead: see {@link #authorizeDeferredScan} and {@link #pinCatalogSnapshot}. Every
     *       other system table reads what the FE already planned, so it is handed over untouched.
     *       The relation-scoped scan params the FE applied to the original wrapper are re-applied
     *       to the rebuilt one by {@link #reapplyScanParams(Table)}.</li>
     * </ul>
     */
    private Table getPaimonTableForBackend() throws UserException {
        Table paimonTable = getProcessedTable();
        if (paimonTable instanceof FileStoreTable) {
            // copy(...) merges the relation's dynamic options into the schema, and the rebuild
            // below goes through that schema, so this branch needs no re-application.
            return dropCatalogLoader((FileStoreTable) paimonTable);
        }
        if (!(source.getExternalTable() instanceof PaimonSysExternalTable)) {
            return paimonTable;
        }
        PaimonSysExternalTable sysTable = (PaimonSysExternalTable) source.getExternalTable();
        // The very same base table the FE-side wrapper was built over, so that the BE never sees a
        // different schema generation than the one this query was planned with.
        FileStoreTable dataTable = sysTable.getSysBaseTable();
        if (dataTable == null) {
            return paimonTable;
        }
        String sysTableType = sysTable.getSysTableType();
        boolean resolvesOnBackend = resolvesSnapshotOnBackend(sysTableType);
        if (PAIMON_FILES_SYSTEM_TABLE_TYPE.equalsIgnoreCase(sysTableType)) {
            authorizeDeferredScan(dataTable);
        }
        FileStoreTable baseForBackend = dropCatalogLoader(dataTable);
        if (resolvesOnBackend) {
            baseForBackend = pinCatalogSnapshot(baseForBackend, dataTable);
        }
        Table catalogLessSysTable = SystemTableLoader.load(sysTableType, baseForBackend);
        if (catalogLessSysTable == null) {
            return paimonTable;
        }
        return reapplyScanParams(catalogLessSysTable, dataTable, resolvesOnBackend);
    }

    /**
     * Re-apply the relation-scoped scan params to a rebuilt system-table wrapper.
     *
     * <p>{@link #getProcessedTable()} applies {@code @incr} / {@code @options} to the wrapper the
     * meta cache holds, and every Paimon system table delegates {@code copy(...)} to the data table
     * it wraps. The wrapper rebuilt above is a different object, so the same copy has to be redone
     * on it, otherwise the BE would materialize its splits against the unpinned latest state.
     *
     * <p>This runs last on purpose: an explicit relation option outranks anything this class pins
     * on the rebuilt table, and {@code copy(...)} lets the option win. An incremental relation
     * outranks {@link #pinCatalogSnapshot} the same way but cannot inherit its bound, so it is
     * bound to the catalog's snapshot separately by {@link #bindIncrementalRangeToCatalog}.
     *
     * <p>Known gap: the {@code @options} branch reaches tables outside
     * {@link #resolvesSnapshotOnBackend}, {@code $ro} among them, and on a
     * {@code scan.fallback-branch} table the {@code copy(...)} here is what triggers
     * {@code rewriteFallbackOptions} - on the already catalog-less pair, so the fallback branch's
     * {@code scan.snapshot-id} is derived from its snapshot directory rather than from the catalog
     * pointer, the same gap {@link #pinCatalogSnapshot} documents. It lands harder here: unlike the
     * pin this is {@code copy(...)}, not {@code copyWithoutTimeTravel(...)}, so it also time-travels
     * the fallback branch's schema, and {@code $ro} is a data table rather than read-only metadata.
     */
    private Table reapplyScanParams(Table rebuiltSysTable, FileStoreTable dataTable, boolean resolvesOnBackend)
            throws UserException {
        TableScanParams theScanParams = getScanParams();
        if (theScanParams == null) {
            return rebuiltSysTable;
        }
        if (theScanParams.incrementalRead()) {
            Map<String, String> incrementalParams = getIncrReadParams();
            if (resolvesOnBackend) {
                incrementalParams = bindIncrementalRangeToCatalog(incrementalParams, dataTable);
            }
            return rebuiltSysTable.copy(incrementalParams);
        }
        if (theScanParams.isOptions()) {
            return PaimonScanParams.applyOptions(rebuiltSysTable,
                    theScanParams.getResolvedMapParams().orElse(Collections.emptyMap()));
        }
        return rebuiltSysTable;
    }

    /**
     * Bind an incremental relation's range to the catalog-visible snapshot.
     *
     * <p>Only for {@link #resolvesSnapshotOnBackend} system tables, i.e. today {@code $partitions},
     * the one table that both re-plans on the BE and accepts {@code @incr}. Paimon selects the
     * incremental scanner from {@code incremental-between*} and never reads {@code scan.snapshot-id}
     * in that mode, so {@link #pinCatalogSnapshot}'s pin cannot bound this scan - and
     * {@code PaimonScanParams#isolateIncrementalRead} clears it anyway, so one relation's read state
     * cannot leak into another's.
     *
     * <p>The timestamp form is the exposed one. {@code IncrementalDeltaStartingScanner
     * #betweenTimestamps} turns both endpoints into snapshot ids through
     * {@code SnapshotManager#earlierOrEqualTimeMills}, whose binary search runs up to
     * {@code latestSnapshotId()}: the catalog's pointer here, but the newest file in the snapshot
     * directory on the catalog-less BE. So resolve the endpoints while the loader is still around
     * and hand the BE the explicit id range Paimon would have computed itself. Only the delta
     * scanner's rule is reachable: {@code incremental-between-scan-mode} is cleared with the rest of
     * the read state, and its default {@code AUTO} never selects the diff scanner.
     *
     * <p>Only when the requested end reaches the catalog's snapshot. An older end already resolves
     * to the same id on both sides, because every snapshot the catalog has not published yet is
     * younger than the one it points at. The snapshot-id form ({@code startSnapshotId} /
     * {@code endSnapshotId}) names its endpoints outright and needs nothing.
     *
     * <p>Never on a {@code scan.fallback-branch} table, because an id range cannot be expressed for
     * the pair. The two branches keep independent snapshot id sequences, and
     * {@code FallbackReadFileStoreTable#rewriteFallbackOptions} translates only
     * {@code scan.snapshot-id} - {@code incremental-between} is copied to the fallback branch
     * verbatim. A main-branch id range would then be validated against the fallback branch's own
     * range in {@code IncrementalDeltaStartingScanner#betweenSnapshotIds} and either fail out of
     * range or select unrelated commits. The timestamp form needs no translation to begin with: a
     * wall clock means the same thing on both branches, and each resolves it against its own
     * {@code SnapshotManager}, so handing it over untouched is the branch-correct choice. What that
     * keeps is the catalog-versus-filesystem endpoint gap {@link #pinCatalogSnapshot} documents for
     * the fallback branch, which is the same gap on the same table.
     */
    private static Map<String, String> bindIncrementalRangeToCatalog(
            Map<String, String> incrementalParams, FileStoreTable dataTable) {
        String range = incrementalParams.get(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP);
        if (range == null) {
            return incrementalParams;
        }
        // Before any pair-level accessor is read: on a fallback pair both catalogEnvironment() and
        // snapshotManager() describe the main branch alone, and the bound could not be carried to
        // the fallback branch anyway.
        if (PaimonUtil.unwrapToFallbackOrBase(dataTable) instanceof FallbackReadFileStoreTable) {
            return incrementalParams;
        }
        if (!dataTable.catalogEnvironment().supportsVersionManagement()) {
            return incrementalParams;
        }
        SnapshotManager snapshotManager = dataTable.snapshotManager();
        Snapshot latest = snapshotManager.latestSnapshot();
        Snapshot earliest = snapshotManager.earliestSnapshot();
        if (latest == null || earliest == null) {
            return incrementalParams;
        }
        String[] endpoints = range.split(",");
        long startMillis = Long.parseLong(endpoints[0]);
        long endMillis = Long.parseLong(endpoints[1]);
        if (endMillis < latest.timeMillis()) {
            return incrementalParams;
        }
        Snapshot start = snapshotManager.earlierOrEqualTimeMills(startMillis);
        // The starting snapshot is exclusive, hence the id before the earliest one when the range
        // opens before it - exactly what betweenTimestamps computes.
        long startId = (start == null || earliest.timeMillis() > startMillis)
                ? earliest.id() - 1
                : start.id();
        Map<String, String> boundParams = new HashMap<>(incrementalParams);
        boundParams.put(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP, null);
        boundParams.put(PAIMON_INCREMENTAL_BETWEEN, startId + "," + latest.id());
        return boundParams;
    }

    /**
     * The system tables whose rows are not fixed by what the FE planned: the FE only sends marker
     * splits and the snapshot is picked inside the BE reader. Two shapes, both honoring
     * {@code scan.snapshot-id}:
     * <ul>
     *   <li>{@code $files} / {@code $partitions} re-plan the base table on the BE
     *       ({@code FilesTable.FilesRead#createReader} -&gt; {@code DataTableScan#plan()},
     *       {@code PartitionsTable.PartitionsRead#createReader} -&gt;
     *       {@code newScan().listPartitionEntries()});</li>
     *   <li>{@code $manifests} / {@code $table_indexes} / {@code $statistics} resolve it directly
     *       ({@code TimeTravelUtil#tryTravelOrLatest}, reached from {@code ManifestsTable},
     *       {@code TableIndexesTable} and {@code AbstractFileStoreTable#statistics}).</li>
     * </ul>
     * All five have a fixed row type, so {@link #pinCatalogSnapshot} cannot rewind the BE schema.
     */
    private static boolean resolvesSnapshotOnBackend(String sysTableType) {
        return PAIMON_FILES_SYSTEM_TABLE_TYPE.equalsIgnoreCase(sysTableType)
                || PAIMON_PARTITIONS_SYSTEM_TABLE_TYPE.equalsIgnoreCase(sysTableType)
                || PAIMON_MANIFESTS_SYSTEM_TABLE_TYPE.equalsIgnoreCase(sysTableType)
                || PAIMON_STATISTICS_SYSTEM_TABLE_TYPE.equalsIgnoreCase(sysTableType)
                || PAIMON_TABLE_INDEXES_SYSTEM_TABLE_TYPE.equalsIgnoreCase(sysTableType);
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
    private static void authorizeDeferredScan(FileStoreTable dataTable) {
        FileStoreTable undecorated = PaimonUtil.unwrapToFallbackOrBase(dataTable);
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
     * <p>Only for {@link #resolvesSnapshotOnBackend} system tables, because only those pick their
     * snapshot inside the BE reader. Every other system table materializes the splits the FE
     * already planned, so pinning them would bind nothing that is not already bound. The pin goes
     * through {@code copyWithoutTimeTravel}, so it bounds which snapshot the BE plans on without
     * rewinding the BE's schema to that snapshot.
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
     * independent marker either. A commit landing between initialization and split generation that drops a partition
     * therefore hides that partition's rows even though the BE stays on the older snapshot. Paimon's
     * two-phase plan has this gap regardless: before the pin the BE resolved the latest snapshot at
     * read time, i.e. across a strictly wider window, so this only moves which side of the window the
     * mismatch falls on.
     *
     * <p>Known gap: on a table with {@code scan.fallback-branch} this bounds the main branch only.
     * The pin itself is carried across correctly - {@code copyWithoutTimeTravel} is pair-aware and
     * {@code rewriteFallbackOptions} converts the main id to the fallback branch's own through
     * {@code SnapshotManager#earlierOrEqualTimeMills}. What is lost is the pointer that conversion
     * searches against: the rebuilt pair has no catalog loader on either branch, so the fallback
     * bound is capped by the newest file in that branch's snapshot directory instead of by the
     * catalog. Of the pinned tables only {@code $partitions} and {@code $files} read the fallback
     * branch at all ({@code $manifests} / {@code $statistics} / {@code $table_indexes} reach it
     * through {@code store()} / {@code statistics()}, which describe the main branch), so the
     * effect is confined to their rows. Note this outlasts a publication window when it is caused
     * by a rollback: the abandoned snapshot file stays on the filesystem until it expires, and for
     * as long as it does the fallback branch can be bound to it.
     */
    private static FileStoreTable pinCatalogSnapshot(FileStoreTable catalogLessTable, FileStoreTable dataTable) {
        if (!dataTable.catalogEnvironment().supportsVersionManagement()) {
            return catalogLessTable;
        }
        Long snapshotId = dataTable.snapshotManager().latestSnapshotId();
        if (snapshotId == null) {
            return catalogLessTable;
        }
        // Without time travel: pin which snapshot the BE plans on, leave schema resolution alone.
        return catalogLessTable.copyWithoutTimeTravel(
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
    private static FileStoreTable dropCatalogLoader(FileStoreTable dataTable) {
        if (dataTable.catalogEnvironment().catalogLoader() == null) {
            return dataTable;
        }
        FileStoreTable undecorated = PaimonUtil.unwrapToFallbackOrBase(dataTable);
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

            TableSchema tableSchema = PaimonUtils.getSchemaCacheValue(targetTable, schemaId).getTableSchema();
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
            rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
            // A logical DataSplit may span multiple files, so keep it intact for the JNI reader
            // until the C++ path has a split-aware V2 adapter.
            fileDesc.setReaderType(TPaimonReaderType.PAIMON_JNI);
            fileDesc.setPaimonSplit(PaimonUtil.encodeObjectToString(split));
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
            if (applyCountPushdown && dataSplit.mergedRowCountAvailable()) {
                splitStat.setMergedRowCount(dataSplit.mergedRowCount());
                PaimonSplit split = new PaimonSplit(dataSplit);
                split.setRowCount(dataSplit.mergedRowCount());
                if (partitionInfoMap != null) {
                    split.setPaimonPartitionValues(partitionInfoMap);
                }
                pushDownCountSplits.add(split);
                pushDownCountSum += dataSplit.mergedRowCount();
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
                        List<Split> dorisSplits = fileSplitter.splitFile(
                                locationPath,
                                targetFileSplitSize,
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
        splits.forEach(s -> s.setTargetSplitSize(sessionVariable.getFileSplitSize() > 0
                ? sessionVariable.getFileSplitSize() : sessionVariable.getMaxSplitSize()));

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
            Table paimonTable = getProcessedTable();
            Map<String, String> resolvedOptions = scanParams == null
                    ? Collections.emptyMap()
                    : scanParams.getResolvedMapParams().orElse(Collections.emptyMap());
            if (PaimonScanParams.isPinnedEmptyScan(resolvedOptions)) {
                return Collections.emptyList();
            }
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
            List<String> fieldNames = paimonTable.rowType().getFieldNames();
            int[] projected = desc.getSlots().stream().mapToInt(
                    slot -> getFieldIndex(fieldNames, slot.getColumn().getName()))
                    .toArray();
            if (Arrays.stream(projected).anyMatch(index -> index < 0)) {
                throw new UserException("Paimon scan schema does not contain all bound Doris columns.");
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
        } finally {
            if (getSummaryProfile() != null) {
                getSummaryProfile().addExternalTableGetFileScanTasksTime(System.currentTimeMillis() - startTime);
            }
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
        Table baseTable = source.getPaimonTable();
        TableScanParams theScanParams = getScanParams();
        if (source.getExternalTable() instanceof PaimonSysExternalTable) {
            PaimonSysExternalTable systemTable = (PaimonSysExternalTable) source.getExternalTable();
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

        if (theScanParams != null && theScanParams.incrementalRead()) {
            // System table handles are cached, so preserve query isolation by applying dynamic
            // options to a copied Paimon table instead of changing the shared handle.
            return baseTable.copy(getIncrReadParams());
        }
        if (theScanParams != null && theScanParams.isOptions()) {
            try {
                return source.getPaimonTable(theScanParams);
            } catch (IllegalArgumentException e) {
                throw new UserException(e.getMessage(), e);
            }
        }
        return baseTable;
    }
}
