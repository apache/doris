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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVBaseTableIf;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVVersionSnapshot;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergExternalTable extends ExternalTable implements MTMVRelatedTableIf, MTMVBaseTableIf {

    public static final String YEAR = "year";
    public static final String MONTH = "month";
    public static final String DAY = "day";
    public static final String HOUR = "hour";
    public static final String IDENTITY = "identity";
    public static final int PARTITION_DATA_ID_START = 1000; // org.apache.iceberg.PartitionSpec

    private Table table;
    private List<Column> partitionColumns;
    private boolean isValidRelatedTableCached = false;
    private boolean isValidRelatedTable = false;

    public IcebergExternalTable(long id, String name, String dbName, IcebergExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.ICEBERG_EXTERNAL_TABLE);
    }

    public String getIcebergCatalogType() {
        return ((IcebergExternalCatalog) catalog).getIcebergCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @VisibleForTesting
    public void setTable(Table table) {
        this.table = table;
    }

    @VisibleForTesting
    public void setPartitionColumns(List<Column> partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        table = IcebergUtils.getIcebergTable(catalog, dbName, name);
        List<Column> schema = IcebergUtils.getSchema(catalog, dbName, name);
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            LOG.debug("Table {} is empty", name);
            return Optional.of(new IcebergSchemaCacheValue(schema, null, -1, null));
        }
        long snapshotId = snapshot.snapshotId();
        partitionColumns = null;
        IcebergPartitionInfo partitionInfo = null;
        if (isValidRelatedTable()) {
            PartitionSpec spec = table.spec();
            partitionColumns = Lists.newArrayList();

            // For iceberg table, we only support table with 1 partition column as RelatedTable.
            // So we use spec.fields().get(0) to get the partition column.
            Types.NestedField col = table.schema().findField(spec.fields().get(0).sourceId());
            for (Column c : schema) {
                if (c.getName().equalsIgnoreCase(col.name())) {
                    partitionColumns.add(c);
                    break;
                }
            }
            Preconditions.checkState(partitionColumns.size() == 1,
                    "Support 1 partition column for iceberg table, but found " + partitionColumns.size());
            try {
                partitionInfo = loadPartitionInfo();
            } catch (AnalysisException e) {
                LOG.warn("Failed to load iceberg table {} partition info.", name, e);
            }
        }
        return Optional.of(new IcebergSchemaCacheValue(schema, partitionColumns, snapshotId, partitionInfo));
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        if (getIcebergCatalogType().equals("hms")) {
            THiveTable tHiveTable = new THiveTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHiveTable(tHiveTable);
            return tTableDescriptor;
        } else {
            TIcebergTable icebergTable = new TIcebergTable(dbName, name, new HashMap<>());
            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ICEBERG_TABLE,
                    schema.size(), 0, getName(), dbName);
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
        long rowCount = IcebergUtils.getIcebergRowCount(getCatalog(), getDbName(), getName());
        return rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT;
    }

    public Table getIcebergTable() {
        return IcebergUtils.getIcebergTable(getCatalog(), getDbName(), getName());
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

    private IcebergPartitionInfo getPartitionInfoFromCache() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        if (!schemaCacheValue.isPresent()) {
            return new IcebergPartitionInfo();
        }
        return ((IcebergSchemaCacheValue) schemaCacheValue.get()).getPartitionInfo();
    }

    @Override
    public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
        makeSureInitialized();
        return isValidRelatedTable() ? PartitionType.RANGE : PartitionType.UNPARTITIONED;
    }

    @Override
    public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) throws DdlException {
        return getPartitionColumnsFromCache().stream().map(Column::getName).collect(Collectors.toSet());
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumnsFromCache();
    }

    private List<Column> getPartitionColumnsFromCache() {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        return schemaCacheValue
                .map(cacheValue -> ((IcebergSchemaCacheValue) cacheValue).getPartitionColumns())
                .orElseGet(Lists::newArrayList);
    }

    @Override
    public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
                                               Optional<MvccSnapshot> snapshot) throws AnalysisException {
        long latestSnapshotId = getPartitionInfoFromCache().getLatestSnapshotId(partitionName);
        if (latestSnapshotId <= 0) {
            throw new AnalysisException("can not find partition: " + partitionName);
        }
        return new MTMVVersionSnapshot(latestSnapshotId);
    }

    @Override
    public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
            throws AnalysisException {
        return new MTMVVersionSnapshot(getLatestSnapshotIdFromCache());
    }

    public long getLatestSnapshotIdFromCache() throws AnalysisException {
        makeSureInitialized();
        Optional<SchemaCacheValue> schemaCacheValue = getSchemaCacheValue();
        if (!schemaCacheValue.isPresent()) {
            throw new AnalysisException("Can't find schema cache of table " + name);
        }
        return ((IcebergSchemaCacheValue) schemaCacheValue.get()).getSnapshotId();
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
        if (isValidRelatedTableCached) {
            return isValidRelatedTable;
        }
        isValidRelatedTable = false;
        Set<String> allFields = Sets.newHashSet();
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
            if (!YEAR.equals(transformName)
                    && !MONTH.equals(transformName)
                    && !DAY.equals(transformName)
                    && !HOUR.equals(transformName)) {
                isValidRelatedTableCached = true;
                return false;
            }
            allFields.add(table.schema().findColumnName(partitionField.sourceId()));
        }
        isValidRelatedTableCached = true;
        isValidRelatedTable = allFields.size() == 1;
        return isValidRelatedTable;
    }

    protected IcebergPartitionInfo loadPartitionInfo() throws AnalysisException {
        List<IcebergPartition> icebergPartitions = loadIcebergPartition();
        Map<String, IcebergPartition> nameToPartition = Maps.newHashMap();
        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMap();
        for (IcebergPartition partition : icebergPartitions) {
            nameToPartition.put(partition.getPartitionName(), partition);
            String transform = table.specs().get(partition.getSpecId()).fields().get(0).transform().toString();
            Range<PartitionKey> partitionRange = getPartitionRange(partition.getPartitionValues().get(0), transform);
            PartitionItem item = new RangePartitionItem(partitionRange);
            nameToPartitionItem.put(partition.getPartitionName(), item);
        }
        Map<String, Set<String>> partitionNameMap = mergeOverlapPartitions(nameToPartitionItem);
        return new IcebergPartitionInfo(nameToPartitionItem, nameToPartition, partitionNameMap);
    }

    public List<IcebergPartition> loadIcebergPartition() {
        PartitionsTable partitionsTable = (PartitionsTable) MetadataTableUtils
                .createMetadataTableInstance(table, MetadataTableType.PARTITIONS);
        List<IcebergPartition> partitions = Lists.newArrayList();
        try (CloseableIterable<FileScanTask> tasks = partitionsTable.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                CloseableIterable<StructLike> rows = task.asDataTask().rows();
                for (StructLike row : rows) {
                    partitions.add(generateIcebergPartition(row));
                }
            }
        } catch (IOException e) {
            LOG.warn("Failed to get Iceberg table {} partition info.", name, e);
        }
        return partitions;
    }

    public IcebergPartition generateIcebergPartition(StructLike row) {
        // row format :
        // 0. partitionData,
        // 1. spec_id,
        // 2. record_count,
        // 3. file_count,
        // 4. total_data_file_size_in_bytes,
        // 5. position_delete_record_count,
        // 6. position_delete_file_count,
        // 7. equality_delete_record_count,
        // 8. equality_delete_file_count,
        // 9. last_updated_at,
        // 10. last_updated_snapshot_id
        Preconditions.checkState(!table.spec().fields().isEmpty(), table.name() + " is not a partition table.");
        int specId = row.get(1, Integer.class);
        PartitionSpec partitionSpec = table.specs().get(specId);
        StructProjection partitionData = row.get(0, StructProjection.class);
        StringBuilder sb = new StringBuilder();
        List<String> partitionValues = Lists.newArrayList();
        List<String> transforms = Lists.newArrayList();
        for (int i = 0; i < partitionSpec.fields().size(); ++i) {
            PartitionField partitionField = partitionSpec.fields().get(i);
            Class<?> fieldClass = partitionSpec.javaClasses()[i];
            int fieldId = partitionField.fieldId();
            // Iceberg partition field id starts at PARTITION_DATA_ID_START,
            // So we can get the field index in partitionData using fieldId - PARTITION_DATA_ID_START
            int index = fieldId - PARTITION_DATA_ID_START;
            Object o = partitionData.get(index, fieldClass);
            String fieldValue = o == null ? null : o.toString();
            String fieldName = partitionField.name();
            sb.append(fieldName);
            sb.append("=");
            sb.append(fieldValue);
            sb.append("/");
            partitionValues.add(fieldValue);
            transforms.add(partitionField.transform().toString());
        }
        if (sb.length() > 0) {
            sb.delete(sb.length() - 1, sb.length());
        }
        String partitionName = sb.toString();
        long recordCount = row.get(2, Long.class);
        long fileCount = row.get(3, Integer.class);
        long fileSizeInBytes = row.get(4, Long.class);
        long lastUpdateTime = row.get(9, Long.class);
        long lastUpdateSnapShotId = row.get(10, Long.class);
        return new IcebergPartition(partitionName, specId, recordCount, fileSizeInBytes, fileCount,
                lastUpdateTime, lastUpdateSnapShotId, partitionValues, transforms);
    }

    @VisibleForTesting
    public Range<PartitionKey> getPartitionRange(String value, String transform)
            throws AnalysisException {
        // For NULL value, create a lessThan partition for it.
        if (value == null) {
            PartitionKey nullKey = PartitionKey.createPartitionKey(
                    Lists.newArrayList(new PartitionValue("0000-01-02")), partitionColumns);
            return Range.lessThan(nullKey);
        }
        LocalDateTime epoch = Instant.EPOCH.atZone(ZoneId.of("UTC")).toLocalDateTime();
        LocalDateTime target;
        LocalDateTime lower;
        LocalDateTime upper;
        long longValue = Long.parseLong(value);
        switch (transform) {
            case HOUR:
                target = epoch.plusHours(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), target.getDayOfMonth(),
                        target.getHour(), 0, 0);
                upper = lower.plusHours(1);
                break;
            case DAY:
                target = epoch.plusDays(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), target.getDayOfMonth(), 0, 0, 0);
                upper = lower.plusDays(1);
                break;
            case MONTH:
                target = epoch.plusMonths(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), 1, 0, 0, 0);
                upper = lower.plusMonths(1);
                break;
            case YEAR:
                target = epoch.plusYears(longValue);
                lower = LocalDateTime.of(target.getYear(), Month.JANUARY, 1, 0, 0, 0);
                upper = lower.plusYears(1);
                break;
            default:
                throw new RuntimeException("Unsupported transform " + transform);
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        Column c = partitionColumns.get(0);
        Preconditions.checkState(c.getDataType().isDateType(), "Only support date type partition column");
        if (c.getType().isDate() || c.getType().isDateV2()) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        }
        PartitionValue lowerValue = new PartitionValue(lower.format(formatter));
        PartitionValue upperValue = new PartitionValue(upper.format(formatter));
        PartitionKey lowKey = PartitionKey.createPartitionKey(Lists.newArrayList(lowerValue), partitionColumns);
        PartitionKey upperKey =  PartitionKey.createPartitionKey(Lists.newArrayList(upperValue), partitionColumns);
        return Range.closedOpen(lowKey, upperKey);
    }

    /**
     * Merge overlapped iceberg partitions into one Doris partition.
     */
    public Map<String, Set<String>> mergeOverlapPartitions(Map<String, PartitionItem> originPartitions) {
        List<Map.Entry<String, PartitionItem>> entries = sortPartitionMap(originPartitions);
        Map<String, Set<String>> map = Maps.newHashMap();
        for (int i = 0; i < entries.size() - 1; i++) {
            Range<PartitionKey> firstValue = entries.get(i).getValue().getItems();
            String firstKey = entries.get(i).getKey();
            Range<PartitionKey> secondValue = entries.get(i + 1).getValue().getItems();
            String secondKey = entries.get(i + 1).getKey();
            // If the first entry enclose the second one, remove the second entry and keep a record in the return map.
            // So we can track the iceberg partitions those contained by one Doris partition.
            while (i < entries.size() && firstValue.encloses(secondValue)) {
                originPartitions.remove(secondKey);
                map.putIfAbsent(firstKey, Sets.newHashSet(firstKey));
                String finalSecondKey = secondKey;
                map.computeIfPresent(firstKey, (key, value) -> {
                    value.add(finalSecondKey);
                    return value;
                });
                i++;
                if (i >= entries.size() - 1) {
                    break;
                }
                secondValue = entries.get(i + 1).getValue().getItems();
                secondKey = entries.get(i + 1).getKey();
            }
        }
        return map;
    }

    /**
     * Sort the given map entries by PartitionItem Range(LOW, HIGH)
     * When comparing two ranges, the one with smaller LOW value is smaller than the other one.
     * If two ranges have same values of LOW, the one with larger HIGH value is smaller.
     *
     * For now, we only support year, month, day and hour,
     * so it is impossible to have two partially intersect partitions.
     * One range is either enclosed by another or has no intersection at all with another.
     *
     *
     * For example, we have these 4 ranges:
     * [10, 20), [30, 40), [0, 30), [10, 15)
     *
     * After sort, they become:
     * [0, 30), [10, 20), [10, 15), [30, 40)
     */
    public List<Map.Entry<String, PartitionItem>> sortPartitionMap(Map<String, PartitionItem> originPartitions) {
        List<Map.Entry<String, PartitionItem>> entries = new ArrayList<>(originPartitions.entrySet());
        entries.sort(new RangeComparator());
        return entries;
    }

    public static class RangeComparator implements Comparator<Map.Entry<String, PartitionItem>> {
        @Override
        public int compare(Map.Entry<String, PartitionItem> p1, Map.Entry<String, PartitionItem> p2) {
            PartitionItem value1 = p1.getValue();
            PartitionItem value2 = p2.getValue();
            if (value1 instanceof RangePartitionItem && value2 instanceof RangePartitionItem) {
                Range<PartitionKey> items1 = value1.getItems();
                Range<PartitionKey> items2 = value2.getItems();
                if (!items1.hasLowerBound()) {
                    return -1;
                }
                if (!items2.hasLowerBound()) {
                    return 1;
                }
                PartitionKey upper1 = items1.upperEndpoint();
                PartitionKey lower1 = items1.lowerEndpoint();
                PartitionKey upper2 = items2.upperEndpoint();
                PartitionKey lower2 = items2.lowerEndpoint();
                int compareLow = lower1.compareTo(lower2);
                return compareLow == 0 ? upper2.compareTo(upper1) : compareLow;
            }
            return 0;
        }
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
}
