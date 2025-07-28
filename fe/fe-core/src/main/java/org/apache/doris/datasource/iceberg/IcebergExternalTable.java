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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalSchemaCache.SchemaCacheKey;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.EmptyMvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.systable.SupportedSysTables;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIdSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewVersion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergExternalTable extends ExternalTable implements MTMVRelatedTableIf, MTMVBaseTableIf, MvccTable {

    private Table table;
    private boolean isValidRelatedTableCached = false;
    private boolean isValidRelatedTable = false;
    private boolean isView;
    private static final String ENGINE_PROP_NAME = "engine-name";

    public IcebergExternalTable(long id, String name, String remoteName, IcebergExternalCatalog catalog,
            IcebergExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.ICEBERG_EXTERNAL_TABLE);
    }

    public String getIcebergCatalogType() {
        return ((IcebergExternalCatalog) catalog).getIcebergCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
            isView = catalog.viewExists(getRemoteDbName(), getRemoteName());
        }
    }

    @VisibleForTesting
    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        boolean isView = isView();
        return IcebergUtils.loadSchemaCacheValue(this, ((IcebergSchemaCacheKey) key).getSchemaId(), isView);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (getIcebergCatalogType().equals("hms")) {
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
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        long rowCount = IcebergUtils.getIcebergRowCount(this);
        return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
    }

    public Table getIcebergTable() {
        return IcebergUtils.getIcebergTable(this);
    }

    @Override
    public void beforeMTMVRefresh(MTMV mtmv) throws DdlException {
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot) {
        return Maps.newHashMap(
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, this)
                .getPartitionInfo().getNameToPartitionItem());
    }

    @Override
    public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> snapshot) {
        return IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, this)
            .getPartitionInfo().getNameToPartitionItem();
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        return isValidRelatedTable() ? PartitionType.RANGE : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) throws DdlException {
        return getPartitionColumns(snapshot).stream().map(Column::getName).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        IcebergSnapshotCacheValue snapshotValue =
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, this);
        IcebergSchemaCacheValue schemaValue = IcebergUtils.getSchemaCacheValue(
                this, snapshotValue.getSnapshot().getSchemaId());
        return schemaValue.getPartitionColumns();
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
                                               Optional<MvccSnapshot> snapshot) throws AnalysisException {
        IcebergSnapshotCacheValue snapshotValue =
                IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, this);
        long latestSnapshotId = snapshotValue.getPartitionInfo().getLatestSnapshotId(partitionName);
        if (latestSnapshotId <= 0) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return new MTMVSnapshotIdSnapshot(latestSnapshotId);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return getTableSnapshot(snapshot);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
        makeSureInitialized();
        IcebergSnapshotCacheValue snapshotValue = IcebergUtils.getOrFetchSnapshotCacheValue(snapshot, this);
        return new MTMVSnapshotIdSnapshot(snapshotValue.getSnapshot().getSnapshotId());
    }

    @Override
    public long getNewestUpdateVersionOrTime() {
        return IcebergUtils.getIcebergSnapshotCacheValue(Optional.empty(), this, Optional.empty())
                .getPartitionInfo().getNameToIcebergPartition().values().stream()
                .mapToLong(IcebergPartition::getLastUpdateTime).max().orElse(0);
    }

    @Override
    public boolean isPartitionColumnAllowNull() {
        return true;
    }

    /**
     * For now, we only support single partition column Iceberg table as related table.
     * The supported transforms now are YEAR, MONTH, DAY and HOUR.
     * And the column couldn't change to another column during partition evolution.
     */
    @Override
    public boolean isValidRelatedTable() {
        makeSureInitialized();
        if (isValidRelatedTableCached) {
            return isValidRelatedTable;
        }
        isValidRelatedTable = false;
        Set<String> allFields = Sets.newHashSet();
        table = getIcebergTable();
        for (PartitionSpec spec : table.specs().values()) {
            if (spec == null) {
                isValidRelatedTableCached = true;
                return false;
            }
            List<PartitionField> fields = spec.fields();
            if (fields.size() != 1) {
                isValidRelatedTableCached = true;
                return false;
            }
            PartitionField partitionField = spec.fields().get(0);
            String transformName = partitionField.transform().toString();
            if (!IcebergUtils.YEAR.equals(transformName)
                    && !IcebergUtils.MONTH.equals(transformName)
                    && !IcebergUtils.DAY.equals(transformName)
                    && !IcebergUtils.HOUR.equals(transformName)) {
                isValidRelatedTableCached = true;
                return false;
            }
            allFields.add(table.schema().findColumnName(partitionField.sourceId()));
        }
        isValidRelatedTableCached = true;
        isValidRelatedTable = allFields.size() == 1;
        return isValidRelatedTable;
    }

    @Override
    public MvccSnapshot loadSnapshot(Optional<TableSnapshot> tableSnapshot, Optional<TableScanParams> scanParams) {
        if (isView()) {
            return new EmptyMvccSnapshot();
        } else {
            return new IcebergMvccSnapshot(IcebergUtils.getIcebergSnapshotCacheValue(
                    tableSnapshot, this, scanParams));
        }
    }

    @Override
    public List<Column> getFullSchema() {
        return IcebergUtils.getIcebergSchema(this);
    }

    @Override
    public boolean supportInternalPartitionPruned() {
        return true;
    }

    @VisibleForTesting
    public boolean isValidRelatedTableCached() {
        return isValidRelatedTableCached;
    }

    @VisibleForTesting
    public boolean validRelatedTableCache() {
        return isValidRelatedTable;
    }

    public void setIsValidRelatedTableCached(boolean isCached) {
        this.isValidRelatedTableCached = isCached;
    }

    @Override
    public List<SysTable> getSupportedSysTables() {
        makeSureInitialized();
        return SupportedSysTables.ICEBERG_SUPPORTED_SYS_TABLES;
    }

    @Override
    public boolean isView() {
        makeSureInitialized();
        return isView;
    }

    public String getViewText() {
        try {
            return catalog.getExecutionAuthenticator().execute(() -> {
                View icebergView = IcebergUtils.getIcebergView(this);
                ViewVersion viewVersion = icebergView.currentVersion();
                if (viewVersion == null) {
                    throw new RuntimeException(String.format("Cannot get view version for view '%s'", icebergView));
                }
                Map<String, String> summary = viewVersion.summary();
                if (summary == null) {
                    throw new RuntimeException(String.format("Cannot get summary for view '%s'", icebergView));
                }
                String engineName = summary.get(ENGINE_PROP_NAME);
                if (StringUtils.isEmpty(engineName)) {
                    throw new RuntimeException(String.format("Cannot get engine-name for view '%s'", icebergView));
                }
                SQLViewRepresentation sqlViewRepresentation = icebergView.sqlFor(engineName.toLowerCase());
                if (sqlViewRepresentation == null) {
                    throw new UnsupportedOperationException("Cannot get view text from iceberg view");
                }
                return sqlViewRepresentation.sql();
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getSqlDialect() {
        try {
            return catalog.getExecutionAuthenticator().execute(() -> {
                View icebergView = IcebergUtils.getIcebergView(this);
                ViewVersion viewVersion = icebergView.currentVersion();
                if (viewVersion == null) {
                    throw new RuntimeException(String.format("Cannot get view version for view '%s'", icebergView));
                }
                Map<String, String> summary = viewVersion.summary();
                if (summary == null) {
                    throw new RuntimeException(String.format("Cannot get summary for view '%s'", icebergView));
                }
                String engineName = summary.get(ENGINE_PROP_NAME);
                if (StringUtils.isEmpty(engineName)) {
                    throw new RuntimeException(String.format("Cannot get engine-name for view '%s'", icebergView));
                }
                return engineName.toLowerCase();
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public View getIcebergView() {
        return IcebergUtils.getIcebergView(this);
    }

    /**
     * get location of an iceberg table or view
     * @return
     */
    public String location() {
        if (isView()) {
            View icebergView = getIcebergView();
            return icebergView.location();
        } else {
            Table icebergTable = getIcebergTable();
            return icebergTable.location();
        }
    }

    /**
     * get properties of an iceberg table or view
     * @return
     */
    public Map<String, String> properties() {
        if (isView()) {
            View icebergView = getIcebergView();
            return icebergView.properties();
        } else {
            Table icebergTable = getIcebergTable();
            return icebergTable.properties();
        }
    }

}
