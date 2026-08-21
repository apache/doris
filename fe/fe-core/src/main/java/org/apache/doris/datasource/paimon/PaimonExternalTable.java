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

package org.apache.doris.datasource.paimon;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CacheException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.systable.PaimonSysTable;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PaimonExternalTable extends ExternalTable implements MTMVRelatedTableIf, MTMVBaseTableIf, MvccTable {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalTable.class);

    public PaimonExternalTable(long id, String name, String remoteName, PaimonExternalCatalog catalog,
            PaimonExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.PAIMON_EXTERNAL_TABLE);
    }

    @Override
    public String getMetaCacheEngine() {
        return PaimonExternalMetaCache.ENGINE;
    }

    public String getPaimonCatalogType() {
        return ((PaimonExternalCatalog) catalog).getCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    public Table getPaimonTable(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            // MTMV scenario: get from snapshot cache
            return getOrFetchSnapshotCacheValue(snapshot).getSnapshot().getTable();
        } else {
            // Normal query scenario: get directly from table cache
            return PaimonUtils.getPaimonTable(this);
        }
    }

    public Table getPaimonTable(TableScanParams scanParams) {
        if (scanParams != null && scanParams.isOptions()) {
            Map<String, String> options = scanParams.getMapParams();
            // Resolve the snapshot pinned for this exact alias; another alias of the same table may
            // carry different relation options and therefore a different statement snapshot.
            Table statementTable = getPaimonTable(MvccUtil.getSnapshotFromContext(this, null, scanParams));
            Table resolutionTable = PaimonScanParams.usesStatementSnapshot(options)
                    ? statementTable
                    : getBasePaimonTable();
            Map<String, String> resolvedOptions = scanParams.getOrResolveMapParams(
                    relationOptions -> PaimonScanParams.resolveOptions(resolutionTable, relationOptions));
            // Startup options are normalized to an immutable snapshot before schema binding. The
            // scan phase reuses that exact resolution instead of consulting a mutable tag or clock.
            Table table = PaimonScanParams.selectsSchema(resolvedOptions)
                    ? getBasePaimonTable()
                    : statementTable;
            return PaimonScanParams.applyOptions(table, resolvedOptions);
        }
        return getPaimonTable(MvccUtil.getSnapshotFromContext(this));
    }

    public List<Column> getFullSchema(TableScanParams scanParams) {
        Table table = getPaimonTable(scanParams);
        return PaimonUtil.parseSchema(table,
                getCatalog().getEnableMappingVarbinary(),
                getCatalog().getEnableMappingTimestampTz());
    }

    /**
     * Load the current remote table for a write target.
     *
     * <p>A statement MVCC snapshot belongs to a read relation. In a time-travel self-insert the
     * same Doris table identity can therefore have a historical source snapshot registered in
     * StatementContext. Write planning must never reuse that snapshot: the writer, target schema
     * and partition metadata must all come from the latest remote table handle.
     */
    public Table getPaimonTableForWrite() {
        return ((PaimonExternalCatalog) catalog).getPaimonTable(getOrBuildNameMapping());
    }

    private PaimonSnapshotCacheValue getPaimonSnapshotCacheValue(Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams) {
        makeSureInitialized();

        // Current limitation: cannot specify both table snapshot and scan parameters simultaneously.
        if (tableSnapshot.isPresent() || (scanParams.isPresent() && scanParams.get().isTag())) {
            // If a snapshot is specified,
            // use the specified snapshot and the corresponding schema(not the latest
            // schema).
            try {
                Table baseTable = getBasePaimonTable();
                DataTable dataTable = (DataTable) baseTable;
                Snapshot snapshot;
                Map<String, String> scanOptions = new HashMap<>();

                if (tableSnapshot.isPresent()) {
                    TableSnapshot snapshotOpt = tableSnapshot.get();
                    String value = snapshotOpt.getValue();
                    if (snapshotOpt.getType() == TableSnapshot.VersionType.TIME) {
                        snapshot = PaimonUtil.getPaimonSnapshotByTimestamp(
                                dataTable, value, PaimonUtil.isDigitalString(value));
                        scanOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshot.id()));
                    } else {
                        if (PaimonUtil.isDigitalString(value)) {
                            snapshot = PaimonUtil.getPaimonSnapshotBySnapshotId(dataTable, value);
                            scanOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshot.id()));
                        } else {
                            snapshot = PaimonUtil.getPaimonSnapshotByTag(dataTable, value);
                            scanOptions.put(CoreOptions.SCAN_TAG_NAME.key(), value);
                        }
                    }
                } else {
                    String tagName = PaimonUtil.extractBranchOrTagName(scanParams.get());
                    snapshot = PaimonUtil.getPaimonSnapshotByTag(dataTable, tagName);
                    scanOptions.put(CoreOptions.SCAN_TAG_NAME.key(), tagName);
                }

                Table scanTable = baseTable.copy(scanOptions);
                return new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(snapshot.id(), snapshot.schemaId(), scanTable));
            } catch (Exception e) {
                LOG.warn("Failed to get Paimon snapshot for table {}", getOrBuildNameMapping().getFullLocalName(), e);
                throw new RuntimeException(
                        "Failed to get Paimon snapshot: " + (e.getMessage() == null ? "unknown cause" : e.getMessage()),
                        e);
            }
        } else if (scanParams.isPresent() && scanParams.get().isOptions()) {
            // Capture the generation once: the effective table derived from this base handle must
            // load, and later hydrate, under the same generation's execution context even if a
            // concurrent ALTER replaces the catalog resources mid-statement.
            PaimonTableCacheValue baseGeneration = PaimonUtils.getPaimonTableCacheValue(this);
            Table baseTable = baseGeneration.getPaimonTable();
            Map<String, String> resolvedOptions = scanParams.get().getOrResolveMapParams(
                    options -> PaimonScanParams.resolveOptions(baseTable, options));
            Table effectiveTable = PaimonScanParams.applyOptions(baseTable, resolvedOptions);
            if (PaimonScanParams.hasOnlyReaderOptions(resolvedOptions)) {
                // Reader tuning cannot change snapshot metadata. Reuse the memoized projection so
                // a per-query batch-size change does not enumerate every partition again.
                return PaimonUtils.getLatestSnapshotCacheValue(this);
            }
            // The shared latest cache was built from the catalog-scoped handle. Relation options
            // need their own projection so partition enumeration uses the final safe table copy.
            return PaimonUtils.loadSnapshotProjection(this, effectiveTable, baseGeneration);
        } else if (scanParams.isPresent() && scanParams.get().isBranch()) {
            try {
                Table baseTable = getBasePaimonTable();
                String branch = PaimonUtil.resolvePaimonBranch(scanParams.get(), baseTable);
                Table table = ((PaimonExternalCatalog) catalog).getPaimonTable(getOrBuildNameMapping(), branch, null);
                Optional<Snapshot> latestSnapshot = table.latestSnapshot();
                long latestSnapshotId = PaimonSnapshot.INVALID_SNAPSHOT_ID;
                if (latestSnapshot.isPresent()) {
                    latestSnapshotId = latestSnapshot.get().id();
                }
                // Use the branch table's effective schema directly: its schema manager can
                // otherwise resolve the main branch namespace for this independently versioned table.
                // TODO: Add time travel support for paimon branch tables.
                DataTable dataTable = (DataTable) table;
                long schemaId = ((FileStoreTable) dataTable).schema().id();
                return new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(latestSnapshotId, schemaId, dataTable), true);
            } catch (Exception e) {
                LOG.warn("Failed to get Paimon branch for table {}", getOrBuildNameMapping().getFullLocalName(), e);
                throw new RuntimeException(
                        "Failed to get Paimon branch: " + (e.getMessage() == null ? "unknown cause" : e.getMessage()),
                        e);
            }
        } else {
            // Otherwise, use the latest snapshot and the latest schema.
            return PaimonUtils.getLatestSnapshotCacheValue(this);
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (PaimonExternalCatalog.PAIMON_HMS.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_FILESYSTEM.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_DLF.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_REST.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_JDBC.equals(getPaimonCatalogType())) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException(
                    "Currently only supports hms/dlf/rest/filesystem/jdbc catalog, do not support: "
                    + getPaimonCatalogType());
        }
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        long rowCount = 0;
        // Row-count planning bypasses ScanNode, so build the same CPU-capped disposable handle
        // here instead of validating the hardware-neutral catalog copy directly.
        Table effectiveTable = PaimonReaderOptions.runtimeSafeTable(getBasePaimonTable());
        // Statistics and row-count cache planning run before ScanNode and must not reach an
        // unsafe manifest executor, even when the foreground relation later supplies an override.
        PaimonReaderOptions.validateEffectiveTable(effectiveTable);
        List<Split> splits = effectiveTable.newReadBuilder().newScan().plan().splits();
        for (Split split : splits) {
            rowCount += split.rowCount();
        }
        if (rowCount == 0) {
            LOG.info("Paimon table {} row count is 0, return -1", name);
        }
        return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
    }

    @Override
    public void beforeMTMVRefresh(MTMV mtmv) throws DdlException {
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot) {
        return Maps.newHashMap(getNameToPartitionItems(snapshot));
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        PaimonPartitionInfo partitionInfo = getOrFetchSnapshotCacheValue(snapshot).getPartitionInfo();
        if (partitionInfo.getPruningStatus() == PaimonPartitionInfo.PruningStatus.UNPRUNABLE) {
            return PartitionType.UNPARTITIONED;
        }
        return getPaimonSchemaCacheValue(snapshot).getPartitionColumns().isEmpty()
                ? PartitionType.UNPARTITIONED : PartitionType.LIST;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        PaimonPartitionInfo partitionInfo = getOrFetchSnapshotCacheValue(snapshot).getPartitionInfo();
        if (partitionInfo.getPruningStatus() == PaimonPartitionInfo.PruningStatus.UNPRUNABLE) {
            return Collections.emptyList();
        }
        return getPaimonSchemaCacheValue(snapshot).getPartitionColumns();
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        Partition paimonPartition = getOrFetchSnapshotCacheValue(snapshot).getPartitionInfo().getNameToPartition()
                .get(partitionName);
        if (paimonPartition == null) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return new MTMVTimestampSnapshot(paimonPartition.lastFileCreationTime());
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getTableSnapshot(snapshot);
    }

    public Map<String, Partition> getPartitionSnapshot(
            Optional<MvccSnapshot> snapshot) {

        return getOrFetchSnapshotCacheValue(snapshot).getPartitionInfo()
                .getNameToPartition();
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        PaimonSnapshotCacheValue paimonSnapshot = getOrFetchSnapshotCacheValue(snapshot);
        return new MTMVSnapshotIdSnapshot(paimonSnapshot.getSnapshot().getSnapshotId());
    }

    @Override
    public long getNewestUpdateVersionOrTime() {
        // Dictionary loading records getTableSnapshot(), whose version is the Paimon snapshot ID.
        // Use the same monotonic version here instead of deriving a timestamp from partition
        // metadata. Partition metadata can intentionally be UNPRUNABLE and contain no Doris map.
        return getPaimonSnapshotCacheValue(Optional.empty(), Optional.empty())
                .getSnapshot().getSnapshotId();
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        // Paimon will write to the 'null' partition regardless of whether it is' null or 'null'.
        // The logic is inconsistent with Doris' empty partition logic, so it needs to return false.
        // However, when Spark creates Paimon tables, specifying 'not null' does not take effect.
        // In order to successfully create the materialized view, false is returned here.
        // The cost is that Paimon partition writes a null value, and the materialized view cannot detect this data.
        return true;
    }

    @Override
    public MvccSnapshot loadSnapshot(Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        return new PaimonMvccSnapshot(getPaimonSnapshotCacheValue(tableSnapshot, scanParams));
    }

    @Override
    public MvccSnapshot loadLatestSnapshotFence() {
        return new PaimonMvccSnapshot(PaimonUtils.loadLatestSnapshotFence(this));
    }

    @Override
    public boolean requiresLatestSnapshotFence(
            Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        return !tableSnapshot.isPresent()
                && scanParams.isPresent()
                && scanParams.get().isOptions()
                && PaimonScanParams.usesStatementSnapshot(scanParams.get().getMapParams());
    }

    @Override
    public MvccSnapshot loadSnapshot(
            Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams,
            Optional<MvccSnapshot> latestSnapshotFence) {
        if (latestSnapshotFence.isPresent() && !tableSnapshot.isPresent() && !scanParams.isPresent()) {
            return new PaimonMvccSnapshot(PaimonUtils.loadSnapshotAtFence(this,
                    ((PaimonMvccSnapshot) latestSnapshotFence.get()).getSnapshotCacheValue()));
        }
        if (!latestSnapshotFence.isPresent()
                || !requiresLatestSnapshotFence(tableSnapshot, scanParams)) {
            return loadSnapshot(tableSnapshot, scanParams);
        }
        PaimonMvccSnapshot fence = (PaimonMvccSnapshot) latestSnapshotFence.get();
        PaimonSnapshotCacheValue fenceValue = fence.getSnapshotCacheValue();
        PaimonSnapshot fenceSnapshot = fenceValue.getSnapshot();
        long snapshotId = fenceSnapshot.getSnapshotId();
        TableScanParams params = scanParams.get();
        Map<String, String> rawOptions = params.getMapParams();
        params.reuseResolvedMapParams(PaimonScanParams.pinOptionsToSnapshot(
                rawOptions, snapshotId));
        if (PaimonScanParams.hasOnlyReaderOptions(rawOptions)) {
            // Reader tuning cannot change the fenced schema or partition projection. Reusing the
            // exact value also avoids a live latest lookup caused by the normal OPTIONS path.
            return new PaimonMvccSnapshot(fenceValue);
        }
        if (snapshotId == PaimonSnapshot.INVALID_SNAPSHOT_ID) {
            // An empty table is a real statement state; reopening a projection after a concurrent
            // first commit would otherwise turn only the later alias non-empty.
            return new PaimonMvccSnapshot(fenceValue);
        }
        FileStoreTable effectiveTable = PaimonScanParams.applyOptionsWithoutTimeTravel(
                (FileStoreTable) fenceSnapshot.getTable(), params.getResolvedMapParams().get());
        return new PaimonMvccSnapshot(
                PaimonUtils.loadSnapshotAtFence(this, effectiveTable, fenceValue));
    }

    @Override
    public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> snapshot) {
        return getOrFetchSnapshotCacheValue(snapshot).getPartitionInfo().getNameToPartitionItem();
    }

    @Override
    public boolean supportInternalPartitionPruned() {
        return true;
    }

    @Override
    public boolean supportsExternalMetadataPreload() {
        return true;
    }

    @Override
    public boolean supportsLatestSnapshotPreload() {
        return true;
    }

    @Override
    public List<Column> getFullSchema() {
        // Descriptor serialization is table-only and may follow multiple OPTIONS aliases. Keep it
        // on a validated statement projection rather than falling back to the neutral cache.
        return getPaimonSchemaCacheValue(
                MvccUtil.getSnapshotForTableMetadataFromContext(this)).getSchema();
    }

    @Override
    public List<Column> getFullSchema(Optional<MvccSnapshot> snapshot) {
        return getPaimonSchemaCacheValue(snapshot).getSchema();
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        makeSureInitialized();
        PaimonSchemaCacheKey paimonSchemaCacheKey = (PaimonSchemaCacheKey) key;
        try {
            return Optional.of(loadSchema((DataTable) getBasePaimonTable(), paimonSchemaCacheKey.getSchemaId()));
        } catch (Exception e) {
            throw new CacheException("failed to initSchema for: %s.%s.%s.%s",
                    null, getCatalog().getName(), key.getNameMapping().getLocalDbName(),
                    key.getNameMapping().getLocalTblName(),
                    paimonSchemaCacheKey.getSchemaId());
        }
    }

    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        return Optional.of(getPaimonSchemaCacheValue(
                MvccUtil.getSnapshotForTableMetadataFromContext(this)));
    }

    private PaimonSchemaCacheValue getPaimonSchemaCacheValue(Optional<MvccSnapshot> snapshot) {
        PaimonSnapshotCacheValue snapshotCacheValue = getOrFetchSnapshotCacheValue(snapshot);
        if (snapshotCacheValue.isSchemaFromSnapshotTable()) {
            PaimonSnapshot paimonSnapshot = snapshotCacheValue.getSnapshot();
            // The snapshot table already carries the branch-specific schema; looking it up by id
            // can accidentally use the base table's schema namespace.
            return loadSchema(((FileStoreTable) paimonSnapshot.getTable()).schema());
        }
        return PaimonUtils.getSchemaCacheValue(this, snapshotCacheValue);
    }

    private PaimonSchemaCacheValue loadSchema(DataTable table, long schemaId) {
        return loadSchema(table.schemaManager().schema(schemaId));
    }

    PaimonSchemaCacheValue loadSchemaForCache(Table retainedTable, long schemaId) {
        if (!(retainedTable instanceof DataTable)) {
            throw new CacheException("retained paimon table does not expose schema history: %s",
                    null, retainedTable == null ? "null" : retainedTable.getClass().getName());
        }
        return loadSchema((DataTable) retainedTable, schemaId);
    }

    private PaimonSchemaCacheValue loadSchema(TableSchema tableSchema) {
        List<DataField> columns = tableSchema.fields();
        List<Column> dorisColumns = Lists.newArrayListWithCapacity(columns.size());
        Set<String> partitionColumnNames = Sets.newHashSet(tableSchema.partitionKeys());
        List<Column> partitionColumns = Lists.newArrayList();
        for (DataField field : columns) {
            Column column = new Column(field.name(),
                    PaimonUtil.paimonTypeToDorisType(field.type(), getCatalog().getEnableMappingVarbinary(),
                            getCatalog().getEnableMappingTimestampTz()),
                    true,
                    null, true, field.description(), true,
                    -1);
            PaimonUtil.updatePaimonColumnUniqueId(column, field);
            if (field.type().getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                column.setWithTZExtraInfo();
            }
            dorisColumns.add(column);
            if (partitionColumnNames.contains(field.name())) {
                partitionColumns.add(column);
            }
        }
        return new PaimonSchemaCacheValue(dorisColumns, partitionColumns, tableSchema);
    }

    private PaimonSnapshotCacheValue getOrFetchSnapshotCacheValue(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            return ((PaimonMvccSnapshot) snapshot.get()).getSnapshotCacheValue();
        } else {
            // Use new lazy-loading snapshot cache API
            return PaimonUtils.getSnapshotCacheValue(snapshot, this);
        }
    }

    @Override
    public Map<String, SysTable> getSupportedSysTables() {
        makeSureInitialized();
        return PaimonSysTable.SUPPORTED_SYS_TABLES;
    }

    @Override
    public String getComment() {
        Table table = getBasePaimonTable();
        return table.comment().isPresent() ? table.comment().get() : "";
    }

    public Map<String, String> getTableProperties() {
        Table table = getBasePaimonTable();
        if (table instanceof DataTable) {
            DataTable dataTable = (DataTable) table;
            Map<String, String> properties = new LinkedHashMap<>(dataTable.coreOptions().toMap());

            if (!dataTable.primaryKeys().isEmpty()) {
                properties.put(CoreOptions.PRIMARY_KEY.key(), String.join(",", dataTable.primaryKeys()));
            }

            return properties;
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public boolean isPartitionedTable() {
        makeSureInitialized();
        return !getBasePaimonTable().partitionKeys().isEmpty();
    }

    Table getBasePaimonTable() {
        return PaimonUtils.getPaimonTable(this);
    }
}
