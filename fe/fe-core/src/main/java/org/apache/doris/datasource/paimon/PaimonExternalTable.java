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
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.metacache.EngineMtmvSupport;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.systable.PaimonSysTable;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;

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
        // MTMV scenario gets table from snapshot cache, normal query from latest table cache.
        return snapshot.isPresent()
                ? getOrFetchSnapshotCacheValue(snapshot).getSnapshot().getTable()
                : PaimonUtils.getPaimonTable(this);
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
                PaimonSchemaCacheValue schema = PaimonMetadataCache.buildSchemaCacheValue(catalog, scanTable,
                        snapshot.schemaId());
                return new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(snapshot.id(), snapshot.schemaId(), scanTable), schema);
            } catch (Exception e) {
                LOG.warn("Failed to get Paimon snapshot for table {}", getOrBuildNameMapping().getFullLocalName(), e);
                throw new RuntimeException(
                        "Failed to get Paimon snapshot: " + (e.getMessage() == null ? "unknown cause" : e.getMessage()),
                        e);
            }
        } else if (scanParams.isPresent() && scanParams.get().isBranch()) {
            try {
                Table baseTable = getBasePaimonTable();
                String branch = PaimonUtil.resolvePaimonBranch(scanParams.get(), baseTable);
                Table table = ((PaimonExternalCatalog) catalog).getPaimonTable(getOrBuildNameMapping(), branch, null);
                long latestSnapshotId = table.latestSnapshot().map(Snapshot::id)
                        .orElse(PaimonSnapshot.INVALID_SNAPSHOT_ID);
                // Branches in Paimon can have independent schemas and snapshots.
                // TODO: Add time travel support for paimon branch tables.
                DataTable dataTable = (DataTable) table;
                Long schemaId = dataTable.schemaManager().latest().map(TableSchema::id).orElse(0L);
                PaimonSchemaCacheValue schema = PaimonMetadataCache.buildSchemaCacheValue(catalog, dataTable, schemaId);
                return new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(latestSnapshotId, schemaId, dataTable), schema);
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
        if (!PaimonExternalCatalog.PAIMON_HMS.equals(getPaimonCatalogType())
                && !PaimonExternalCatalog.PAIMON_FILESYSTEM.equals(getPaimonCatalogType())
                && !PaimonExternalCatalog.PAIMON_DLF.equals(getPaimonCatalogType())
                && !PaimonExternalCatalog.PAIMON_REST.equals(getPaimonCatalogType())) {
            throw new IllegalArgumentException(
                    "Currently only supports hms/dlf/rest/filesystem catalog, do not support :"
                    + getPaimonCatalogType());
        }
        THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                getName(), dbName);
        tTableDescriptor.setHiveTable(tHiveTable);
        return tTableDescriptor;
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
        List<Split> splits = getBasePaimonTable().newReadBuilder().newScan().plan().splits();
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
        return EngineMtmvSupport.getAndCopyPartitionItems(this, snapshot);
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        return !isPartitionInvalid(snapshot) && !getPartitionColumns(snapshot).isEmpty()
                ? PartitionType.LIST
                : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns(snapshot).stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        if (isPartitionInvalid(snapshot)) {
            return Collections.emptyList();
        }
        return getPaimonSchemaCacheValue(snapshot).getPartitionColumns();
    }

    public boolean isPartitionInvalid(Optional<MvccSnapshot> snapshot) {
        PaimonSnapshotCacheValue paimonSnapshotCacheValue = getOrFetchSnapshotCacheValue(snapshot);
        return paimonSnapshotCacheValue.getPartitionInfo().isPartitionInvalid();
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return EngineMtmvSupport.getPartitionSnapshot(this, partitionName, snapshot);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getTableSnapshot(snapshot);
    }

    public Map<String, Partition> getPartitionSnapshot(
            Optional<MvccSnapshot> snapshot) {
        try {
            return EngineMtmvSupport.getPaimonPartitionSnapshot(this, snapshot);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        return EngineMtmvSupport.getTableSnapshot(this, snapshot);
    }

    @Override
    public long getNewestUpdateVersionOrTime() {
        return getPaimonSnapshotCacheValue(Optional.empty(), Optional.empty()).getPartitionInfo().getNameToPartition()
                .values().stream()
                .mapToLong(Partition::lastFileCreationTime).max().orElse(0);
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
    public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> snapshot) {
        return EngineMtmvSupport.getPartitionItems(this, snapshot);
    }

    @Override
    public boolean supportInternalPartitionPruned() {
        return true;
    }

    @Override
    public List<Column> getFullSchema() {
        makeSureInitialized();
        return super.getFullSchema();
    }

    @Override
    public long resolveSchemaVersionToken(Optional<MvccSnapshot> snapshot) {
        MvccSnapshot mvccSnapshot = snapshot.orElse(null);
        if (mvccSnapshot instanceof PaimonMvccSnapshot) {
            return ((PaimonMvccSnapshot) mvccSnapshot).getSnapshotCacheValue().getSnapshot().getSchemaId();
        }
        return CURRENT_SCHEMA_VERSION_TOKEN;
    }

    @Override
    public Optional<SchemaCacheValue> loadSchemaByVersion(long versionToken) {
        makeSureInitialized();
        setUpdateTime(System.currentTimeMillis());
        long schemaId = versionToken == CURRENT_SCHEMA_VERSION_TOKEN
                ? getOrFetchSnapshotCacheValue(Optional.empty()).getSnapshot().getSchemaId()
                : versionToken;
        try {
            return Optional.of(PaimonMetadataCache.buildSchemaCacheValue(catalog, getBasePaimonTable(), schemaId));
        } catch (Exception e) {
            throw new CacheException("failed to initSchema for: %s.%s.%s.%s",
                    e, getCatalog().getName(), getDbName(), getName(), schemaId);
        }
    }

    private PaimonSchemaCacheValue getPaimonSchemaCacheValue(Optional<MvccSnapshot> snapshot) {
        PaimonSnapshotCacheValue snapshotCacheValue = getOrFetchSnapshotCacheValue(snapshot);
        return PaimonUtils.getSchemaCacheValue(this, snapshotCacheValue);
    }

    private PaimonSnapshotCacheValue getOrFetchSnapshotCacheValue(Optional<MvccSnapshot> snapshot) {
        return snapshot.isPresent()
                ? ((PaimonMvccSnapshot) snapshot.get()).getSnapshotCacheValue()
                // Use new lazy-loading snapshot cache API
                : PaimonUtils.getSnapshotCacheValue(snapshot, this);
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

    private Table getBasePaimonTable() {
        return PaimonUtils.getPaimonTable(this);
    }
}
