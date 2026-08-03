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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Represents a Paimon system table (e.g., snapshots, binlog, audit_log) that wraps a source data table.
 *
 * <p>This class enables system tables to be queried using the native table execution path
 * (FileQueryScanNode) instead of the TVF path (MetadataScanNode). This provides:
 * <ul>
 *   <li>Unified execution path with regular tables</li>
 *   <li>Native vectorized reading for data-oriented system tables</li>
 *   <li>Better integration with query optimization</li>
 * </ul>
 *
 * <p>System tables are classified into two categories:
 * <ul>
 *   <li><b>Data tables</b> (e.g., binlog, audit_log, ro): Read actual ORC/Parquet data files</li>
 *   <li><b>Metadata tables</b> (snapshots, partitions, etc.): Read metadata/manifest files</li>
 * </ul>
 */
public class PaimonSysExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(PaimonSysExternalTable.class);

    private final PaimonExternalTable sourceTable;
    private final String sysTableType;
    private volatile Boolean isDataTable;
    private volatile FileStoreTable paimonSysDataTable;
    private volatile Table paimonSysTable;
    private volatile List<Column> fullSchema;
    private volatile SchemaCacheValue schemaCacheValue;

    /**
     * Creates a new Paimon system external table.
     *
     * @param sourceTable the underlying data table being wrapped
     * @param sysTableType the type of system table (e.g., "snapshots", "binlog")
     */
    public PaimonSysExternalTable(PaimonExternalTable sourceTable, String sysTableType) {
        super(generateSysTableId(sourceTable.getId(), sysTableType),
                sourceTable.getName() + "$" + sysTableType,
                sourceTable.getRemoteName() + "$" + sysTableType,
                (PaimonExternalCatalog) sourceTable.getCatalog(),
                (PaimonExternalDatabase) sourceTable.getDatabase(),
                TableIf.TableType.PAIMON_EXTERNAL_TABLE);
        this.sourceTable = sourceTable;
        this.sysTableType = sysTableType;
    }

    @Override
    public String getMetaCacheEngine() {
        return PaimonExternalMetaCache.ENGINE;
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    /**
     * Generate a unique ID for the system table based on source table ID and system table type.
     */
    private static long generateSysTableId(long sourceTableId, String sysTableType) {
        // Use a simple hash combination to generate a unique ID
        return sourceTableId ^ (sysTableType.hashCode() * 31L);
    }

    /**
     * Returns the Paimon system table instance (e.g., snapshots, binlog).
     */
    public Table getSysPaimonTable() {
        getRawSysPaimonTable();
        FileStoreTable safeDataTable = (FileStoreTable) PaimonReaderOptions.runtimeSafeTable(paimonSysDataTable);
        PaimonReaderOptions.validateEffectiveTable(safeDataTable);
        // The cached wrapper must remain catalog-neutral; rebuild only when this FE needs a capped
        // data handle so the hidden manifest planner sees the normalized value.
        return safeDataTable == paimonSysDataTable ? paimonSysTable : createSystemTable(safeDataTable);
    }

    /** Returns the cached wrapper without validating its hidden data table. */
    public Table getRawSysPaimonTable() {
        if (paimonSysTable == null) {
            synchronized (this) {
                if (paimonSysTable == null) {
                    Table dataTable = sourceTable.getBasePaimonTable();
                    if (!(dataTable instanceof FileStoreTable)) {
                        throw new IllegalArgumentException(
                                "Paimon system tables require a file-store data table.");
                    }
                    paimonSysDataTable = (FileStoreTable) dataTable;
                    // Build the wrapper from this exact cached handle. Reloading it through the
                    // catalog can pair validation with one generation and planning with another.
                    paimonSysTable = createSystemTable(paimonSysDataTable);
                    LOG.info("Created Paimon system table: {} for source table: {}",
                            sysTableType, sourceTable.getName());
                }
            }
        }
        return paimonSysTable;
    }

    public Table getSysPaimonTable(TableScanParams scanParams) {
        return getSysPaimonTable(getRawSysPaimonDataTable(), scanParams);
    }

    public Table getSysPaimonTable(FileStoreTable dataTable, TableScanParams scanParams) {
        if (scanParams == null || !scanParams.isOptions()) {
            FileStoreTable safeDataTable = (FileStoreTable) PaimonReaderOptions.runtimeSafeTable(dataTable);
            PaimonReaderOptions.validateEffectiveTable(safeDataTable);
            return createSystemTable(safeDataTable);
        }
        Map<String, String> resolvedOptions = resolvedOptions(dataTable, scanParams);
        if (PaimonScanParams.getPinnedFileCreationTime(resolvedOptions).isPresent()) {
            // Generic system-table wrappers cannot carry Paimon's manifest-entry predicate.
            // Reject the fallback instead of silently widening it to the whole pinned snapshot.
            throw new IllegalArgumentException(
                    "Paimon system tables cannot apply a creation-time file filter.");
        }
        FileStoreTable effectiveDataTable = PaimonScanParams.applyOptionsToBoundTable(
                dataTable, resolvedOptions);
        return createSystemTable(effectiveDataTable);
    }

    public void validateEffectiveDataTable(TableScanParams scanParams) {
        validateEffectiveDataTable(getRawSysPaimonDataTable(), scanParams);
    }

    public void validateEffectiveDataTable(FileStoreTable dataTable, TableScanParams scanParams) {
        if (scanParams != null && scanParams.isOptions()) {
            // Apply the same relation copy to the data table hidden by ReadonlyTable wrappers.
            PaimonScanParams.applyOptionsToBoundTable(
                    dataTable, resolvedOptions(dataTable, scanParams));
        } else {
            PaimonReaderOptions.validateEffectiveTable(PaimonReaderOptions.runtimeSafeTable(dataTable));
        }
    }

    public OptionalInt runtimeSafeManifestParallelism(TableScanParams scanParams) {
        return runtimeSafeManifestParallelism(getRawSysPaimonDataTable(), scanParams);
    }

    public OptionalInt runtimeSafeManifestParallelism(
            FileStoreTable dataTable, TableScanParams scanParams) {
        Table effectiveDataTable = runtimeSafeDataTable(dataTable, scanParams, Collections.emptyMap());
        // The serialized system wrapper does not expose this hidden value, so transport the
        // FE-safe bound separately for a smaller BE to lower after deserialization.
        return PaimonReaderOptions.backendManifestParallelismCap(effectiveDataTable);
    }

    public FileStoreTable runtimeSafeDataTable(
            TableScanParams scanParams, Map<String, String> incrementalOptions) {
        return runtimeSafeDataTable(getRawSysPaimonDataTable(), scanParams, incrementalOptions);
    }

    public FileStoreTable runtimeSafeDataTable(
            FileStoreTable dataTable, TableScanParams scanParams, Map<String, String> incrementalOptions) {
        if (scanParams != null && scanParams.isOptions()) {
            return PaimonScanParams.applyOptionsToBoundTable(
                    dataTable, resolvedOptions(dataTable, scanParams));
        }
        if (incrementalOptions != null && !incrementalOptions.isEmpty()) {
            return (FileStoreTable) PaimonReaderOptions.runtimeSafeTable(
                    dataTable.copy(incrementalOptions));
        }
        return (FileStoreTable) PaimonReaderOptions.runtimeSafeTable(dataTable);
    }

    private Map<String, String> resolvedOptions(FileStoreTable dataTable, TableScanParams scanParams) {
        return scanParams.getOrResolveMapParams(
                options -> PaimonScanParams.resolveOptions(dataTable, options));
    }

    public FileStoreTable getBoundDataTable(Optional<MvccSnapshot> snapshot) {
        if (snapshot.isPresent()) {
            if (!(snapshot.get() instanceof PaimonMvccSnapshot)) {
                throw new IllegalArgumentException("Expected a Paimon MVCC snapshot for a Paimon system table.");
            }
            Table table = ((PaimonMvccSnapshot) snapshot.get()).getSnapshotCacheValue().getSnapshot().getTable();
            if (!(table instanceof FileStoreTable)) {
                throw new IllegalArgumentException("Paimon system tables require a file-store data table.");
            }
            return (FileStoreTable) table;
        }
        return getRawSysPaimonDataTable();
    }

    private FileStoreTable getRawSysPaimonDataTable() {
        getRawSysPaimonTable();
        return paimonSysDataTable;
    }

    public Table getRawSysPaimonTable(FileStoreTable dataTable) {
        return createSystemTable(dataTable);
    }

    private Table createSystemTable(FileStoreTable dataTable) {
        Table systemTable = SystemTableLoader.load(
                sysTableType, PaimonTableDecorators.unwrapToFallbackOrBase(dataTable));
        if (systemTable == null) {
            throw new IllegalArgumentException("Unknown Paimon system table '" + sysTableType + "'.");
        }
        return systemTable;
    }

    public List<Column> getFullSchema(TableScanParams scanParams) {
        return getFullSchema(scanParams, Optional.empty());
    }

    public List<Column> getFullSchema(
            TableScanParams scanParams, Optional<MvccSnapshot> relationSnapshot) {
        Table table = getSysPaimonTable(getBoundDataTable(relationSnapshot), scanParams);
        return PaimonUtil.parseSchema(table,
                getCatalog().getEnableMappingVarbinary(),
                getCatalog().getEnableMappingTimestampTz());
    }

    /**
     * Returns the schema of the system table.
     * The schema is derived from the system table's rowType.
     */
    @Override
    public List<Column> getFullSchema() {
        return getOrCreateSchemaCacheValue().getSchema();
    }

    static List<Column> buildFullSchema(List<DataField> fields, boolean enableMappingVarbinary,
            boolean enableMappingTimestampTz) {
        List<Column> columns = Lists.newArrayListWithCapacity(fields.size());

        for (DataField field : fields) {
            Column column = new Column(
                    field.name(),
                    PaimonUtil.paimonTypeToDorisType(
                            field.type(), enableMappingVarbinary, enableMappingTimestampTz),
                    true,
                    null,
                    true,
                    field.description(),
                    true,
                    field.id());
            PaimonUtil.updatePaimonColumnUniqueId(column, field);
            if (field.type().getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                column.setWithTZExtraInfo();
            }
            columns.add(column);
        }
        return columns;
    }

    public PaimonExternalTable getSourceTable() {
        return sourceTable;
    }

    @Override
    public NameMapping getOrBuildNameMapping() {
        return sourceTable.getOrBuildNameMapping();
    }

    public String getSysTableType() {
        return sysTableType;
    }

    public boolean isDataTable() {
        return resolveIsDataTable();
    }

    private boolean resolveIsDataTable() {
        if (isDataTable == null) {
            synchronized (this) {
                if (isDataTable == null) {
                    // Type inspection happens before relation parameters reach ScanNode. It must not
                    // reject a hidden physical value that a later OPTIONS copy safely overrides.
                    isDataTable = getRawSysPaimonTable() instanceof DataTable;
                }
            }
        }
        return isDataTable;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        String catalogType = sourceTable.getPaimonCatalogType();
        if (PaimonExternalCatalog.PAIMON_HMS.equals(catalogType)
                || PaimonExternalCatalog.PAIMON_FILESYSTEM.equals(catalogType)
                || PaimonExternalCatalog.PAIMON_DLF.equals(catalogType)
                || PaimonExternalCatalog.PAIMON_REST.equals(catalogType)
                || PaimonExternalCatalog.PAIMON_JDBC.equals(catalogType)) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException(
                    "Currently only supports hms/dlf/rest/filesystem/jdbc catalog, do not support: " + catalogType);
        }
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        long rowCount = 0;
        List<Split> splits = getSysPaimonTable().newReadBuilder().newScan().plan().splits();
        for (Split split : splits) {
            rowCount += split.rowCount();
        }
        if (rowCount == 0) {
            LOG.info("Paimon system table {} row count is 0, return -1", name);
        }
        return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        return Optional.of(getOrCreateSchemaCacheValue());
    }

    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        return Optional.of(getOrCreateSchemaCacheValue());
    }

    @Override
    public Map<String, SysTable> getSupportedSysTables() {
        return sourceTable.getSupportedSysTables();
    }

    public Map<String, String> getTableProperties() {
        return sourceTable.getTableProperties();
    }

    @Override
    public String getComment() {
        return "Paimon system table: " + sysTableType + " for " + sourceTable.getName();
    }

    private SchemaCacheValue getOrCreateSchemaCacheValue() {
        if (schemaCacheValue == null) {
            synchronized (this) {
                if (schemaCacheValue == null) {
                    if (fullSchema == null) {
                        // Schema serialization never plans manifests, so keep a safe relation
                        // override from being replaced by validation of the physical data handle.
                        Table sysTable = getRawSysPaimonTable();
                        fullSchema = buildFullSchema(sysTable.rowType().getFields(),
                                getCatalog().getEnableMappingVarbinary(),
                                getCatalog().getEnableMappingTimestampTz());
                    }
                    schemaCacheValue = new SchemaCacheValue(fullSchema);
                }
            }
        }
        return schemaCacheValue;
    }

}
