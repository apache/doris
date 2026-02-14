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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
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
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
    private volatile Table paimonSysTable;

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
     * Note: system tables currently ignore snapshot semantics.
     */
    public Table getSysPaimonTable() {
        if (paimonSysTable == null) {
            synchronized (this) {
                if (paimonSysTable == null) {
                    PaimonExternalCatalog catalog = (PaimonExternalCatalog) getCatalog();
                    paimonSysTable = catalog.getPaimonTable(
                            sourceTable.getOrBuildNameMapping(),
                            "main",  // branch
                            sysTableType  // queryType: snapshots, binlog, etc.
                    );
                    LOG.info("Created Paimon system table: {} for source table: {}",
                            sysTableType, sourceTable.getName());
                }
            }
        }
        return paimonSysTable;
    }

    /**
     * Returns the schema of the system table.
     * The schema is derived from the system table's rowType.
     */
    @Override
    public List<Column> getFullSchema() {
        Table sysTable = getSysPaimonTable();
        List<DataField> fields = sysTable.rowType().getFields();
        List<Column> columns = Lists.newArrayListWithCapacity(fields.size());

        for (DataField field : fields) {
            Column column = new Column(
                    field.name().toLowerCase(),
                    PaimonUtil.paimonTypeToDorisType(
                            field.type(),
                            getCatalog().getEnableMappingVarbinary(),
                            getCatalog().getEnableMappingTimestampTz()),
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
                    isDataTable = getSysPaimonTable() instanceof DataTable;
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
                || PaimonExternalCatalog.PAIMON_REST.equals(catalogType)) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException(
                    "Currently only supports hms/dlf/rest/filesystem catalog, do not support :" + catalogType);
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
        return Optional.of(new SchemaCacheValue(getFullSchema()));
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
}
