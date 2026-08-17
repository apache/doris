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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IcebergSysExternalTable extends ExternalTable {
    private final IcebergExternalTable sourceTable;
    private final String sysTableType;

    public IcebergSysExternalTable(IcebergExternalTable sourceTable, String sysTableType) {
        super(generateSysTableId(sourceTable.getId(), sysTableType),
                sourceTable.getName() + "$" + sysTableType,
                sourceTable.getRemoteName() + "$" + sysTableType,
                (IcebergExternalCatalog) sourceTable.getCatalog(),
                (IcebergExternalDatabase) sourceTable.getDatabase(),
                TableIf.TableType.ICEBERG_EXTERNAL_TABLE);
        this.sourceTable = sourceTable;
        this.sysTableType = sysTableType;
    }

    @Override
    public String getMetaCacheEngine() {
        return sourceTable.getMetaCacheEngine();
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    public IcebergExternalTable getSourceTable() {
        return sourceTable;
    }

    public String getSysTableType() {
        return sysTableType;
    }

    public boolean isPositionDeletesTable() {
        return MetadataTableType.POSITION_DELETES.name().equalsIgnoreCase(sysTableType);
    }

    public boolean supportsSnapshotSelection() {
        MetadataTableType tableType = MetadataTableType.from(sysTableType);
        // Static metadata scans and ALL_* scans ignore a selected snapshot, so accepting one would
        // only add failures for expired IDs without changing the rows returned.
        return tableType != MetadataTableType.ALL_DATA_FILES
                && tableType != MetadataTableType.ALL_DELETE_FILES
                && tableType != MetadataTableType.ALL_FILES
                && tableType != MetadataTableType.ALL_MANIFESTS
                && tableType != MetadataTableType.ALL_ENTRIES
                && tableType != MetadataTableType.HISTORY
                && tableType != MetadataTableType.SNAPSHOTS
                && tableType != MetadataTableType.REFS
                && tableType != MetadataTableType.METADATA_LOG_ENTRIES;
    }

    public Table getSysIcebergTable() {
        MetadataTableType tableType = MetadataTableType.from(sysTableType);
        if (tableType == null) {
            throw new IllegalArgumentException("Unknown iceberg system table type: " + sysTableType);
        }
        // Metadata tables capture their base operations. Keep them statement-local so exact
        // previousFiles/history state and stale-generation retry never leak into this table object.
        return MetadataTableUtils.createMetadataTableInstance(resolveBaseTable(), tableType);
    }

    /**
     * The base generation this statement binds the metadata table to. Snapshot-selectable
     * metadata tables derive both their scan and their schema from the source relation's frozen
     * snapshot (the same generation IcebergScanNode scans), so analysis and execution cannot see
     * different partition specs or schemas when the table entry refreshes mid-statement. Static
     * metadata tables and statements without a bound snapshot read the latest generation.
     *
     * <p>The statement snapshot is looked up by source table (like the scan node's fallback);
     * a statement that time-travels the same table under several relations resolves the default
     * or, if ambiguous, the latest generation for the schema.
     */
    @VisibleForTesting
    Table resolveBaseTable() {
        if (supportsSnapshotSelection()) {
            Optional<Table> frozenTable = MvccUtil.getSnapshotFromContext(sourceTable)
                    .filter(IcebergMvccSnapshot.class::isInstance)
                    .map(IcebergMvccSnapshot.class::cast)
                    .flatMap(snapshot -> snapshot.getSnapshotCacheValue().getIcebergTable());
            if (frozenTable.isPresent()) {
                return frozenTable.get();
            }
        }
        return IcebergUtils.getQueryScopedIcebergTable(sourceTable);
    }

    @Override
    public List<Column> getFullSchema() {
        return loadSchemaCacheValue().getSchema();
    }

    @Override
    public NameMapping getOrBuildNameMapping() {
        return sourceTable.getOrBuildNameMapping();
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (sourceTable.getIcebergCatalogType().equals("hms")) {
            THiveTable tHiveTable = new THiveTable(getDbName(), getName(), new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), getDbName());
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            TIcebergTable icebergTable = new TIcebergTable(getDbName(), getName(), new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_TABLE,
                    schema.size(), 0, getName(), getDbName());
            tTableDescriptor.setIcebergTable(icebergTable);
            return tTableDescriptor;
        }
    }

    @Override
    public long fetchRowCount() {
        return UNKNOWN_ROW_COUNT;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        return Optional.of(loadSchemaCacheValue());
    }

    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        return Optional.of(loadSchemaCacheValue());
    }

    @Override
    public Map<String, SysTable> getSupportedSysTables() {
        return sourceTable.getSupportedSysTables();
    }

    @Override
    public String getComment() {
        return "Iceberg system table: " + sysTableType + " for " + sourceTable.getName();
    }

    private static long generateSysTableId(long sourceTableId, String sysTableType) {
        return sourceTableId ^ (sysTableType.hashCode() * 31L);
    }

    private SchemaCacheValue loadSchemaCacheValue() {
        // Metadata-table schemas may change after source schema or partition-spec evolution.
        // Resolve the schema from the statement's bound generation instead of permanently pairing
        // this long-lived system-table object with its first observed generation.
        return new SchemaCacheValue(IcebergUtils.parseSchema(getSysIcebergTable().schema(),
                getCatalog().getEnableMappingVarbinary(),
                getCatalog().getEnableMappingTimestampTz()));
    }
}
