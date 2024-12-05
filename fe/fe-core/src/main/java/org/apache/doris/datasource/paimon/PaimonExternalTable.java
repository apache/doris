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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;
import org.apache.doris.mtmv.MTMVVersionSnapshot;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.system.PartitionsTable;
import org.apache.paimon.table.system.SnapshotsTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PaimonExternalTable extends ExternalTable implements MTMVRelatedTableIf, MTMVBaseTableIf {

    private static final Logger LOG = LogManager.getLogger(PaimonExternalTable.class);

    public PaimonExternalTable(long id, String name, String dbName, PaimonExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.PAIMON_EXTERNAL_TABLE);
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

    public Table getPaimonTable() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue.map(value -> ((PaimonSchemaCacheValue) value).getPaimonTable()).orElse(null);
    }

    private PaimonPartitionInfo getPartitionInfoFromCache() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        if (!schemaCacheValue.isPresent()) {
            return new PaimonPartitionInfo();
        }
        return ((PaimonSchemaCacheValue) schemaCacheValue.get()).getPartitionInfo();
    }

    private List<Column> getPartitionColumnsFromCache() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        if (!schemaCacheValue.isPresent()) {
            return Lists.newArrayList();
        }
        return ((PaimonSchemaCacheValue) schemaCacheValue.get()).getPartitionColumns();
    }

    public long getLatestSnapshotIdFromCache() throws AnalysisException {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        if (!schemaCacheValue.isPresent()) {
            throw new AnalysisException("not present");
        }
        return ((PaimonSchemaCacheValue) schemaCacheValue.get()).getSnapshootId();
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        Table paimonTable = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName, name);
        TableSchema schema = ((FileStoreTable) paimonTable).schema();
        List<DataField> columns = schema.fields();
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(columns.size());
        Set<String> partitionColumnNames = Sets.newHashSet(paimonTable.partitionKeys());
        List<Column> partitionColumns = Lists.newArrayList();
        for (DataField field : columns) {
            Column column = new Column(field.name().toLowerCase(),
                    paimonTypeToDorisType(field.type()), true, null, true, field.description(), true,
                    field.id());
            tmpSchema.add(column);
            if (partitionColumnNames.contains(field.name())) {
                partitionColumns.add(column);
            }
        }
        try {
            // after 0.9.0 paimon will support table.getLatestSnapshotId()
            long latestSnapshotId = loadLatestSnapshotId();
            PaimonPartitionInfo partitionInfo = loadPartitionInfo(partitionColumns);
            return Optional.of(new PaimonSchemaCacheValue(tmpSchema, partitionColumns, paimonTable, latestSnapshotId,
                    partitionInfo));
        } catch (IOException | AnalysisException e) {
            LOG.warn(e);
            return Optional.empty();
        }
    }

    private long loadLatestSnapshotId() throws IOException {
        Table table = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName,
                name + Catalog.SYSTEM_TABLE_SPLITTER + SnapshotsTable.SNAPSHOTS);
        // snapshotId
        List<InternalRow> rows = PaimonUtil.read(table, new int[][] {{0}});
        long latestSnapshotId = 0L;
        for (InternalRow row : rows) {
            long snapshotId = row.getLong(0);
            if (snapshotId > latestSnapshotId) {
                latestSnapshotId = snapshotId;
            }
        }
        return latestSnapshotId;
    }

    private PaimonPartitionInfo loadPartitionInfo(List<Column> partitionColumns) throws IOException, AnalysisException {
        if (CollectionUtils.isEmpty(partitionColumns)) {
            return new PaimonPartitionInfo();
        }
        List<PaimonPartition> paimonPartitions = loadPartitions();
        return PaimonUtil.generatePartitionInfo(partitionColumns, paimonPartitions);
    }

    private List<PaimonPartition> loadPartitions()
            throws IOException {
        Table table = ((PaimonExternalCatalog) catalog).getPaimonTable(dbName,
                name + Catalog.SYSTEM_TABLE_SPLITTER + PartitionsTable.PARTITIONS);
        List<InternalRow> rows = PaimonUtil.read(table, null);
        List<PaimonPartition> res = Lists.newArrayListWithCapacity(rows.size());
        for (InternalRow row : rows) {
            res.add(PaimonUtil.rowToPartition(row));
        }
        return res;
    }

    private Type paimonPrimitiveTypeToDorisType(org.apache.paimon.types.DataType dataType) {
        int tsScale = 3; // default
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INTEGER:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case SMALLINT:
                return Type.SMALLINT;
            case TINYINT:
                return Type.TINYINT;
            case VARCHAR:
            case BINARY:
            case CHAR:
            case VARBINARY:
                return Type.STRING;
            case DECIMAL:
                DecimalType decimal = (DecimalType) dataType;
                return ScalarType.createDecimalV3Type(decimal.getPrecision(), decimal.getScale());
            case DATE:
                return ScalarType.createDateV2Type();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.TimestampType) {
                    tsScale = ((org.apache.paimon.types.TimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                } else if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (dataType instanceof org.apache.paimon.types.LocalZonedTimestampType) {
                    tsScale = ((org.apache.paimon.types.LocalZonedTimestampType) dataType).getPrecision();
                    if (tsScale > 6) {
                        tsScale = 6;
                    }
                }
                return ScalarType.createDatetimeV2Type(tsScale);
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Type innerType = paimonPrimitiveTypeToDorisType(arrayType.getElementType());
                return org.apache.doris.catalog.ArrayType.create(innerType, true);
            case MAP:
                MapType mapType = (MapType) dataType;
                return new org.apache.doris.catalog.MapType(
                        paimonTypeToDorisType(mapType.getKeyType()), paimonTypeToDorisType(mapType.getValueType()));
            case ROW:
                RowType rowType = (RowType) dataType;
                List<DataField> fields = rowType.getFields();
                return new org.apache.doris.catalog.StructType(fields.stream()
                        .map(field -> new org.apache.doris.catalog.StructField(field.name(),
                                paimonTypeToDorisType(field.type())))
                        .collect(Collectors.toCollection(ArrayList::new)));
            case TIME_WITHOUT_TIME_ZONE:
                return Type.UNSUPPORTED;
            default:
                LOG.warn("Cannot transform unknown type: " + dataType.getTypeRoot());
                return Type.UNSUPPORTED;
        }
    }

    protected Type paimonTypeToDorisType(org.apache.paimon.types.DataType type) {
        return paimonPrimitiveTypeToDorisType(type);
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (PaimonExternalCatalog.PAIMON_HMS.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_FILESYSTEM.equals(getPaimonCatalogType())
                || PaimonExternalCatalog.PAIMON_DLF.equals(getPaimonCatalogType())) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            throw new IllegalArgumentException("Currently only supports hms/filesystem catalog,not support :"
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
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        Table paimonTable = schemaCacheValue.map(value -> ((PaimonSchemaCacheValue) value).getPaimonTable())
                .orElse(null);
        if (paimonTable == null) {
            LOG.info("Paimon table {} is null.", name);
            return UNKNOWN_ROW_COUNT;
        }
        List<Split> splits = paimonTable.newReadBuilder().newScan().plan().splits();
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
        Env.getCurrentEnv().getRefreshManager()
                .refreshTable(getCatalog().getName(), getDbName(), getName(), true);
    }

    @Override
    public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot) {
        return Maps.newHashMap(getPartitionInfoFromCache().getNameToPartitionItem());
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumnsFromCache().size() > 0 ? PartitionType.LIST : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumnsFromCache().stream()
                .map(c -> c.getName().toLowerCase()).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumnsFromCache();
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
            Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        PaimonPartition paimonPartition = getPartitionInfoFromCache().getNameToPartition().get(partitionName);
        if (paimonPartition == null) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return new MTMVTimestampSnapshot(paimonPartition.getLastUpdateTime());
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return new MTMVVersionSnapshot(getLatestSnapshotIdFromCache());
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
}
