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

package org.apache.doris.clone;

import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.RandomDistributionDesc;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.AutoBucketUtils;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to periodically add or drop partition on an olapTable which specify dynamic partition properties
 * Config.dynamic_partition_enable determine whether this feature is enable,
 * Config.dynamic_partition_check_interval_seconds determine how often the task is performed
 */
public class DynamicPartitionScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionScheduler.class);
    public static final String LAST_SCHEDULER_TIME = "lastSchedulerTime";
    public static final String LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String DYNAMIC_PARTITION_STATE = "dynamicPartitionState";
    public static final String CREATE_PARTITION_MSG = "createPartitionMsg";
    public static final String DROP_PARTITION_MSG = "dropPartitionMsg";

    private static final String DEFAULT_RUNTIME_VALUE = FeConstants.null_string;

    private Map<Long, Map<String, String>> runtimeInfos = Maps.newConcurrentMap();
    private Set<Pair<Long, Long>> dynamicPartitionTableInfo = Sets.newConcurrentHashSet();
    private boolean initialize;

    public enum State {
        NORMAL,
        ERROR
    }

    public DynamicPartitionScheduler(String name, long intervalMs) {
        super(name, intervalMs);
        this.initialize = false;
    }

    public void executeDynamicPartitionFirstTime(Long dbId, Long tableId) {
        List<Pair<Long, Long>> tempDynamicPartitionTableInfo = Lists.newArrayList(Pair.of(dbId, tableId));
        executeDynamicPartition(tempDynamicPartitionTableInfo);
    }

    public void registerDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.add(Pair.of(dbId, tableId));
    }

    // only for test
    public boolean containsDynamicPartitionTable(Long dbId, Long tableId) {
        return dynamicPartitionTableInfo.contains(Pair.of(dbId, tableId));
    }

    public void removeDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.remove(Pair.of(dbId, tableId));
    }

    public String getRuntimeInfo(long tableId, String key) {
        Map<String, String> tableRuntimeInfo = runtimeInfos.getOrDefault(tableId, createDefaultRuntimeInfo());
        return tableRuntimeInfo.getOrDefault(key, DEFAULT_RUNTIME_VALUE);
    }

    public void removeRuntimeInfo(long tableId) {
        runtimeInfos.remove(tableId);
    }

    public void createOrUpdateRuntimeInfo(long tableId, String key, String value) {
        Map<String, String> runtimeInfo = runtimeInfos.get(tableId);
        if (runtimeInfo == null) {
            runtimeInfo = createDefaultRuntimeInfo();
            runtimeInfo.put(key, value);
            runtimeInfos.put(tableId, runtimeInfo);
        } else {
            runtimeInfo.put(key, value);
        }
    }

    private Map<String, String> createDefaultRuntimeInfo() {
        Map<String, String> defaultRuntimeInfo = Maps.newConcurrentMap();
        defaultRuntimeInfo.put(LAST_UPDATE_TIME, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(LAST_SCHEDULER_TIME, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(DYNAMIC_PARTITION_STATE, State.NORMAL.toString());
        defaultRuntimeInfo.put(CREATE_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
        defaultRuntimeInfo.put(DROP_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
        return defaultRuntimeInfo;
    }

    // exponential moving average
    private static long ema(ArrayList<Long> history, int period) {
        double alpha = 2.0 / (period + 1);
        double ema = history.get(0);
        for (int i = 1; i < history.size(); i++) {
            ema = alpha * history.get(i) + (1 - alpha) * ema;
        }
        return (long) ema;
    }

    private static long getNextPartitionSize(ArrayList<Long> historyPartitionsSize) {
        if (historyPartitionsSize.size() < 2) {
            return historyPartitionsSize.get(0);
        }

        int size = historyPartitionsSize.size() > 7 ? 7 : historyPartitionsSize.size();

        boolean isAscending = true;
        for (int i = 1; i < size; i++) {
            if (historyPartitionsSize.get(i) < historyPartitionsSize.get(i - 1)) {
                isAscending = false;
                break;
            }
        }

        if (isAscending) {
            ArrayList<Long> historyDeltaSize = Lists.newArrayList();
            for (int i = 1; i < size; i++) {
                historyDeltaSize.add(historyPartitionsSize.get(i) - historyPartitionsSize.get(i - 1));
            }
            return historyPartitionsSize.get(size - 1) + ema(historyDeltaSize, 7);
        } else {
            return ema(historyPartitionsSize, 7);
        }
    }

    private static int getBucketsNum(DynamicPartitionProperty property, OlapTable table) {
        if (!table.isAutoBucket()) {
            return property.getBuckets();
        }

        // auto bucket
        // get all history partitions
        List<Partition> partitions = Lists.newArrayList();
        RangePartitionInfo info = (RangePartitionInfo) (table.getPartitionInfo());
        List<Map.Entry<Long, PartitionItem>> idToItems = new ArrayList<>(info.getIdToItem(false).entrySet());
        idToItems.sort(Comparator.comparing(o -> ((RangePartitionItem) o.getValue()).getItems().upperEndpoint()));
        for (Map.Entry<Long, PartitionItem> idToItem : idToItems) {
            Partition partition = table.getPartition(idToItem.getKey());
            if (partition != null) {
                partitions.add(partition);
            }
        }

        // no exist history partition
        if (partitions.size() == 0) {
            return property.getBuckets();
        }

        ArrayList<Long> partitionSizeArray = Lists.newArrayList();
        for (Partition partition : partitions) {
            if (partition.getVisibleVersion() >= 2) {
                partitionSizeArray.add(partition.getAllDataSize(true));
            }
        }

        // no exist history partition data
        if (partitionSizeArray.size() == 0) {
            return property.getBuckets();
        }

        // plus 5 for uncompressed data
        long uncompressedPartitionSize = getNextPartitionSize(partitionSizeArray) * 5;
        return AutoBucketUtils.getBucketsNum(uncompressedPartitionSize, Config.autobucket_min_buckets);
    }

    private ArrayList<AddPartitionClause> getAddPartitionClause(Database db, OlapTable olapTable,
            Column partitionColumn, String partitionFormat) {
        ArrayList<AddPartitionClause> addPartitionClauses = new ArrayList<>();
        DynamicPartitionProperty dynamicPartitionProperty = olapTable.getTableProperty().getDynamicPartitionProperty();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        ZonedDateTime now = ZonedDateTime.now(dynamicPartitionProperty.getTimeZone().toZoneId());

        boolean createHistoryPartition = dynamicPartitionProperty.isCreateHistoryPartition();
        int idx;
        int start = dynamicPartitionProperty.getStart();
        int historyPartitionNum = dynamicPartitionProperty.getHistoryPartitionNum();
        // When enable create_history_partition, will check the valid value from start and history_partition_num.
        if (createHistoryPartition) {
            if (historyPartitionNum == DynamicPartitionProperty.NOT_SET_HISTORY_PARTITION_NUM) {
                idx = start;
            } else {
                idx = Math.max(start, -historyPartitionNum);
            }
        } else {
            idx = 0;
        }
        int hotPartitionNum = dynamicPartitionProperty.getHotPartitionNum();
        String storagePolicyName = dynamicPartitionProperty.getStoragePolicy();

        for (; idx <= dynamicPartitionProperty.getEnd(); idx++) {
            String prevBorder = DynamicPartitionUtil.getPartitionRangeString(
                    dynamicPartitionProperty, now, idx, partitionFormat);
            String nextBorder = DynamicPartitionUtil.getPartitionRangeString(
                    dynamicPartitionProperty, now, idx + 1, partitionFormat);
            PartitionValue lowerValue = new PartitionValue(prevBorder);
            PartitionValue upperValue = new PartitionValue(nextBorder);

            boolean isPartitionExists = false;
            Range<PartitionKey> addPartitionKeyRange;
            try {
                PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue),
                        Collections.singletonList(partitionColumn));
                PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(upperValue),
                        Collections.singletonList(partitionColumn));
                addPartitionKeyRange = Range.closedOpen(lowerBound, upperBound);
            } catch (AnalysisException | IllegalArgumentException e) {
                // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
                // IllegalArgumentException: lb is greater than ub
                LOG.warn("Error in gen addPartitionKeyRange. Error={}, db: {}, table: {}", e.getMessage(),
                        db.getFullName(), olapTable.getName());
                continue;
            }
            for (PartitionItem partitionItem : rangePartitionInfo.getIdToItem(false).values()) {
                // only support single column partition now
                try {
                    RangeUtils.checkRangeIntersect(partitionItem.getItems(), addPartitionKeyRange);
                } catch (Exception e) {
                    isPartitionExists = true;
                    if (addPartitionKeyRange.equals(partitionItem.getItems())) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("partition range {} exist in table {}, clear fail msg",
                                    addPartitionKeyRange, olapTable.getName());
                        }
                        clearCreatePartitionFailedMsg(olapTable.getId());
                    } else {
                        recordCreatePartitionFailedMsg(db.getFullName(), olapTable.getName(),
                                e.getMessage(), olapTable.getId());
                    }
                    break;
                }
            }
            if (isPartitionExists) {
                continue;
            }

            // construct partition desc
            PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createFixed(Collections.singletonList(lowerValue),
                    Collections.singletonList(upperValue));
            HashMap<String, String> partitionProperties = new HashMap<>(1);
            if (dynamicPartitionProperty.getReplicaAllocation().isNotSet()) {
                partitionProperties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                        olapTable.getDefaultReplicaAllocation().toCreateStmt());
            } else {
                partitionProperties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                        dynamicPartitionProperty.getReplicaAllocation().toCreateStmt());
            }

            // set storage_medium and storage_cooldown_time based on dynamic_partition.hot_partition_num
            setStorageMediumProperty(partitionProperties, dynamicPartitionProperty, now, hotPartitionNum, idx);

            if (StringUtils.isNotEmpty(storagePolicyName)) {
                setStoragePolicyProperty(partitionProperties, dynamicPartitionProperty, now, idx, storagePolicyName);
            }

            String partitionName = dynamicPartitionProperty.getPrefix()
                    + DynamicPartitionUtil.getFormattedPartitionName(dynamicPartitionProperty.getTimeZone(),
                    prevBorder, dynamicPartitionProperty.getTimeUnit());
            SinglePartitionDesc rangePartitionDesc = new SinglePartitionDesc(true, partitionName,
                    partitionKeyDesc, partitionProperties);

            DistributionDesc distributionDesc = null;
            DistributionInfo distributionInfo = olapTable.getDefaultDistributionInfo();
            int bucketsNum = getBucketsNum(dynamicPartitionProperty, olapTable);
            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
                List<String> distColumnNames = new ArrayList<>();
                for (Column distributionColumn : hashDistributionInfo.getDistributionColumns()) {
                    distColumnNames.add(distributionColumn.getName());
                }
                distributionDesc = new HashDistributionDesc(bucketsNum, distColumnNames);
            } else {
                distributionDesc = new RandomDistributionDesc(bucketsNum);
            }
            // add partition according to partition desc and distribution desc
            addPartitionClauses.add(new AddPartitionClause(rangePartitionDesc, distributionDesc, null, false));
        }
        return addPartitionClauses;
    }

    /**
     * If dynamic_partition.storage_medium is set to SSD,
     * ignore hot_partition_num property and set to (SSD, 9999-12-31 23:59:59)
     * Else, if hot partition num is set, set storage medium to SSD due to time.
     *
     * @param partitionProperties
     * @param property
     * @param now
     * @param hotPartitionNum
     * @param offset
     */
    private void setStorageMediumProperty(HashMap<String, String> partitionProperties,
            DynamicPartitionProperty property, ZonedDateTime now, int hotPartitionNum, int offset) {
        if ((hotPartitionNum <= 0 || offset + hotPartitionNum <= 0) && !property.getStorageMedium()
                .equalsIgnoreCase("ssd")) {
            return;
        }
        String cooldownTime;
        if (property.getStorageMedium().equalsIgnoreCase("ssd")) {
            cooldownTime = TimeUtils.longToTimeString(DataProperty.MAX_COOLDOWN_TIME_MS);
        } else {
            cooldownTime = DynamicPartitionUtil.getPartitionRangeString(
                    property, now, offset + hotPartitionNum, DynamicPartitionUtil.DATETIME_FORMAT);
        }
        partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, TStorageMedium.SSD.name());
        partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, cooldownTime);
    }

    private void setStoragePolicyProperty(HashMap<String, String> partitionProperties,
            DynamicPartitionProperty property, ZonedDateTime now, int offset,
            String storagePolicyName) {
        partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY, storagePolicyName);
        String baseTime = DynamicPartitionUtil.getPartitionRangeString(
                property, now, offset, DynamicPartitionUtil.DATETIME_FORMAT);
        partitionProperties.put(PropertyAnalyzer.PROPERTIES_DATA_BASE_TIME, baseTime);
    }

    private Range<PartitionKey> getClosedRange(Database db, OlapTable olapTable, Column partitionColumn,
            String partitionFormat, String lowerBorderOfReservedHistory, String upperBorderOfReservedHistory) {
        Range<PartitionKey> reservedHistoryPartitionKeyRange = null;
        PartitionValue lowerBorderPartitionValue = new PartitionValue(lowerBorderOfReservedHistory);
        PartitionValue upperBorderPartitionValue = new PartitionValue(upperBorderOfReservedHistory);
        try {
            PartitionKey lowerBorderBound = PartitionKey.createPartitionKey(
                    Collections.singletonList(lowerBorderPartitionValue), Collections.singletonList(partitionColumn));
            PartitionKey upperBorderBound = PartitionKey.createPartitionKey(
                    Collections.singletonList(upperBorderPartitionValue), Collections.singletonList(partitionColumn));
            reservedHistoryPartitionKeyRange = Range.closed(lowerBorderBound, upperBorderBound);
        } catch (AnalysisException e) {
            // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
            // IllegalArgumentException: lb is greater than ub
            LOG.warn("Error in gen reservePartitionKeyRange. Error={}, db: {}, table: {}", e.getMessage(),
                    db.getFullName(), olapTable.getName());
        }
        return reservedHistoryPartitionKeyRange;
    }

    /**
     * 1. get the range of [start, 0) as a reserved range.
     * 2. get DropPartitionClause of partitions which range are before this reserved range.
     */
    private ArrayList<DropPartitionClause> getDropPartitionClause(Database db, OlapTable olapTable,
            Column partitionColumn, String partitionFormat) throws DdlException {
        ArrayList<DropPartitionClause> dropPartitionClauses = new ArrayList<>();
        DynamicPartitionProperty dynamicPartitionProperty = olapTable.getTableProperty().getDynamicPartitionProperty();
        if (dynamicPartitionProperty.getStart() == DynamicPartitionProperty.MIN_START_OFFSET) {
            // not set start offset, so not drop any partition
            return dropPartitionClauses;
        }

        ZonedDateTime now = ZonedDateTime.now(dynamicPartitionProperty.getTimeZone().toZoneId());
        String lowerBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty,
                now, dynamicPartitionProperty.getStart(), partitionFormat);
        String upperBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty,
                now, dynamicPartitionProperty.getEnd() + 1, partitionFormat);
        PartitionValue lowerPartitionValue = new PartitionValue(lowerBorder);
        PartitionValue upperPartitionValue = new PartitionValue(upperBorder);
        List<Range<PartitionKey>> reservedHistoryPartitionKeyRangeList = new ArrayList<Range<PartitionKey>>();
        Range<PartitionKey> reservePartitionKeyRange;
        try {
            PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerPartitionValue),
                    Collections.singletonList(partitionColumn));
            PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(upperPartitionValue),
                    Collections.singletonList(partitionColumn));
            reservePartitionKeyRange = Range.closedOpen(lowerBound, upperBound);
            reservedHistoryPartitionKeyRangeList.add(reservePartitionKeyRange);
        } catch (AnalysisException | IllegalArgumentException e) {
            // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
            // IllegalArgumentException: lb is greater than ub
            LOG.warn("Error in gen reservePartitionKeyRange. Error={}, db: {}, table: {}", e.getMessage(),
                    db.getFullName(), olapTable.getName());
            return dropPartitionClauses;
        }

        String reservedHistoryPeriods = dynamicPartitionProperty.getReservedHistoryPeriods();
        List<Range> ranges = DynamicPartitionUtil.convertStringToPeriodsList(reservedHistoryPeriods,
                dynamicPartitionProperty.getTimeUnit());

        if (ranges.size() != 0) {
            for (Range range : ranges) {
                try {
                    String lowerBorderOfReservedHistory = DynamicPartitionUtil.getHistoryPartitionRangeString(
                            dynamicPartitionProperty, range.lowerEndpoint().toString(), partitionFormat);
                    String upperBorderOfReservedHistory = DynamicPartitionUtil.getHistoryPartitionRangeString(
                            dynamicPartitionProperty, range.upperEndpoint().toString(), partitionFormat);
                    Range<PartitionKey> reservedHistoryPartitionKeyRange = getClosedRange(db, olapTable,
                            partitionColumn, partitionFormat,
                            lowerBorderOfReservedHistory, upperBorderOfReservedHistory);
                    reservedHistoryPartitionKeyRangeList.add(reservedHistoryPartitionKeyRange);
                } catch (IllegalArgumentException e) {
                    return dropPartitionClauses;
                } catch (AnalysisException e) {
                    throw new DdlException(e.getMessage());
                }
            }
        }
        RangePartitionInfo info = (RangePartitionInfo) (olapTable.getPartitionInfo());

        List<Map.Entry<Long, PartitionItem>> idToItems = new ArrayList<>(info.getIdToItem(false).entrySet());
        idToItems.sort(Comparator.comparing(o -> ((RangePartitionItem) o.getValue()).getItems().upperEndpoint()));
        Map<Long, Boolean> isContaineds = new HashMap<>();
        for (Map.Entry<Long, PartitionItem> idToItem : idToItems) {
            isContaineds.put(idToItem.getKey(), false);
            Long checkDropPartitionId = idToItem.getKey();
            Range<PartitionKey> checkDropPartitionKey = idToItem.getValue().getItems();
            for (Range<PartitionKey> reserveHistoryPartitionKeyRange : reservedHistoryPartitionKeyRangeList) {
                if (RangeUtils.checkIsTwoRangesIntersect(reserveHistoryPartitionKeyRange, checkDropPartitionKey)) {
                    isContaineds.put(checkDropPartitionId, true);
                }
            }
        }

        for (Long dropPartitionId : isContaineds.keySet()) {
            // Do not drop the partition "by force", or the partition will be dropped directly instread of being in
            // catalog recycle bin. This is for safe reason.
            if (!isContaineds.get(dropPartitionId)) {
                String dropPartitionName = olapTable.getPartition(dropPartitionId).getName();
                dropPartitionClauses.add(new DropPartitionClause(false, dropPartitionName, false, false));
            }
        }
        return dropPartitionClauses;
    }

    private void executeDynamicPartition(Collection<Pair<Long, Long>> dynamicPartitionTableInfoCol) {
        Iterator<Pair<Long, Long>> iterator = dynamicPartitionTableInfoCol.iterator();
        while (iterator.hasNext()) {
            Pair<Long, Long> tableInfo = iterator.next();
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                iterator.remove();
                continue;
            }

            ArrayList<AddPartitionClause> addPartitionClauses = new ArrayList<>();
            ArrayList<DropPartitionClause> dropPartitionClauses = new ArrayList<>();
            String tableName = null;
            boolean skipAddPartition = false;
            OlapTable olapTable;
            olapTable = (OlapTable) db.getTableNullable(tableId);
            // Only OlapTable has DynamicPartitionProperty
            if (olapTable == null
                    || !olapTable.dynamicPartitionExists()
                    || !olapTable.getTableProperty().getDynamicPartitionProperty().getEnable()) {
                iterator.remove();
                continue;
            } else if (olapTable.isBeingSynced()) {
                continue;
            }
            olapTable.readLock();
            try {
                if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                    String errorMsg = "Table[" + olapTable.getName() + "]'s state is not NORMAL."
                            + "Do not allow doing dynamic add partition. table state=" + olapTable.getState();
                    recordCreatePartitionFailedMsg(db.getFullName(), olapTable.getName(), errorMsg, olapTable.getId());
                    skipAddPartition = true;
                }

                // Determine the partition column type
                // if column type is Date, format partition name as yyyyMMdd
                // if column type is DateTime, format partition name as yyyyMMddHHssmm
                // scheduler time should be record even no partition added
                createOrUpdateRuntimeInfo(olapTable.getId(), LAST_SCHEDULER_TIME, TimeUtils.getCurrentFormatTime());
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
                if (rangePartitionInfo.getPartitionColumns().size() != 1) {
                    // currently only support partition with single column.
                    iterator.remove();
                    continue;
                }

                Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
                String partitionFormat;
                try {
                    partitionFormat = DynamicPartitionUtil.getPartitionFormat(partitionColumn);
                } catch (Exception e) {
                    recordCreatePartitionFailedMsg(db.getFullName(), olapTable.getName(),
                            e.getMessage(), olapTable.getId());
                    continue;
                }

                if (!skipAddPartition) {
                    addPartitionClauses = getAddPartitionClause(db, olapTable, partitionColumn, partitionFormat);
                }
                dropPartitionClauses = getDropPartitionClause(db, olapTable, partitionColumn, partitionFormat);
                tableName = olapTable.getName();
            } catch (DdlException e) {
                LOG.warn("should not happen", e);
            } finally {
                olapTable.readUnlock();
            }

            for (DropPartitionClause dropPartitionClause : dropPartitionClauses) {
                if (!olapTable.writeLockIfExist()) {
                    continue;
                }
                try {
                    Env.getCurrentEnv().dropPartition(db, olapTable, dropPartitionClause);
                    clearDropPartitionFailedMsg(olapTable.getId());
                } catch (Exception e) {
                    recordDropPartitionFailedMsg(db.getFullName(), tableName, e.getMessage(), olapTable.getId());
                } finally {
                    olapTable.writeUnlock();
                }
            }

            if (!skipAddPartition) {
                for (AddPartitionClause addPartitionClause : addPartitionClauses) {
                    try {
                        Env.getCurrentEnv().addPartition(db, tableName, addPartitionClause);
                        clearCreatePartitionFailedMsg(olapTable.getId());
                    } catch (Exception e) {
                        recordCreatePartitionFailedMsg(db.getFullName(), tableName, e.getMessage(), olapTable.getId());
                    }
                }
            }
        }
    }

    private void recordCreatePartitionFailedMsg(String dbName, String tableName, String msg, long tableId) {
        LOG.warn("dynamic add partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
        createOrUpdateRuntimeInfo(tableId, DYNAMIC_PARTITION_STATE, State.ERROR.toString());
        createOrUpdateRuntimeInfo(tableId, CREATE_PARTITION_MSG, msg);
    }

    private void clearCreatePartitionFailedMsg(long tableId) {
        createOrUpdateRuntimeInfo(tableId, DYNAMIC_PARTITION_STATE, State.NORMAL.toString());
        createOrUpdateRuntimeInfo(tableId, CREATE_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
    }

    private void recordDropPartitionFailedMsg(String dbName, String tableName, String msg, long tableId) {
        LOG.warn("dynamic drop partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
        createOrUpdateRuntimeInfo(tableId, DYNAMIC_PARTITION_STATE, State.ERROR.toString());
        createOrUpdateRuntimeInfo(tableId, DROP_PARTITION_MSG, msg);
    }

    private void clearDropPartitionFailedMsg(long tableId) {
        createOrUpdateRuntimeInfo(tableId, DYNAMIC_PARTITION_STATE, State.NORMAL.toString());
        createOrUpdateRuntimeInfo(tableId, DROP_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
    }

    private void initDynamicPartitionTable() {
        for (Long dbId : Env.getCurrentEnv().getInternalCatalog().getDbIds()) {
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
            if (db == null) {
                continue;
            }
            List<Table> tableList = db.getTables();
            for (Table table : tableList) {
                table.readLock();
                try {
                    if (DynamicPartitionUtil.isDynamicPartitionTable(table)) {
                        registerDynamicPartitionTable(db.getId(), table.getId());
                    }
                } finally {
                    table.readUnlock();
                }
            }
        }
        initialize = true;
    }

    @Override
    protected void runAfterCatalogReady() {
        if (!initialize) {
            // check Dynamic Partition tables only when FE start
            initDynamicPartitionTable();
        }
        setInterval(Config.dynamic_partition_check_interval_seconds * 1000L);
        if (Config.dynamic_partition_enable) {
            executeDynamicPartition(dynamicPartitionTableInfo);
        }
    }
}
