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
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.MetaIdGenerator;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.AutoBucketUtils;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.PartitionPersistInfo;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Strings;
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
import java.util.stream.Collectors;

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

    public void executeDynamicPartitionFirstTime(Long dbId, Long tableId) throws DdlException {
        List<Pair<Long, Long>> tempDynamicPartitionTableInfo = Lists.newArrayList(Pair.of(dbId, tableId));
        executeDynamicPartition(tempDynamicPartitionTableInfo, true);
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

    private static int getBucketsNum(DynamicPartitionProperty property, OlapTable table,
            String partitionName, String nowPartitionName, boolean executeFirstTime) {
        // if execute first time, all partitions no contain data
        if (!table.isAutoBucket() || executeFirstTime) {
            return property.getBuckets();
        }

        // auto bucket
        // get all history partitions
        RangePartitionInfo info = (RangePartitionInfo) (table.getPartitionInfo());
        List<Map.Entry<Long, PartitionItem>> idToItems = new ArrayList<>(info.getIdToItem(false).entrySet());
        idToItems.sort(Comparator.comparing(o -> ((RangePartitionItem) o.getValue()).getItems().upperEndpoint()));
        List<Partition> partitions = idToItems.stream()
                .map(entry -> table.getPartition(entry.getKey()))
                .filter(partition -> partition != null && !partition.getName().equals(nowPartitionName))
                .collect(Collectors.toList());
        List<Long> visibleVersions = null;
        try {
            visibleVersions = Partition.getVisibleVersions(partitions);
        } catch (RpcException e) {
            LOG.warn("autobucket use property's buckets get visible version fail, table: [{}-{}], "
                    + "partition: {}, buckets num: {}, exception: ",
                    table.getName(), table.getId(), partitionName, property.getBuckets(), e);
            return property.getBuckets();
        }

        List<Partition> hasDataPartitions = Lists.newArrayList();
        for (int i = 0; i < partitions.size(); i++) {
            if (visibleVersions.get(i) >= 2) {
                hasDataPartitions.add(partitions.get(i));
            }
        }

        // no exist history partition data
        if (hasDataPartitions.isEmpty()) {
            LOG.info("autobucket use property's buckets due to all partitions no data, table: [{}-{}], "
                    + "partition: {}, buckets num: {}",
                    table.getName(), table.getId(), partitionName, property.getBuckets());
            return property.getBuckets();
        }

        ArrayList<Long> partitionSizeArray = hasDataPartitions.stream()
                .map(partition -> partition.getAllDataSize(true))
                .collect(Collectors.toCollection(ArrayList::new));
        long estimatePartitionSize = getNextPartitionSize(partitionSizeArray);
        // plus 5 for uncompressed data
        long uncompressedPartitionSize = estimatePartitionSize * 5;
        int bucketsNum = AutoBucketUtils.getBucketsNum(uncompressedPartitionSize, Config.autobucket_min_buckets);
        LOG.info("autobucket calc with {} history partitions, table: [{}-{}], partition: {}, buckets num: {}, "
                + " estimate partition size: {}, last partitions(partition name, local size, remote size): {}",
                hasDataPartitions.size(), table.getName(), table.getId(), partitionName, bucketsNum,
                estimatePartitionSize,
                hasDataPartitions.stream()
                        .skip(Math.max(0, hasDataPartitions.size() - 7))
                        .map(partition -> "(" + partition.getName() + ", " + partition.getDataSize(true)
                                + ", " + partition.getRemoteDataSize() + ")")
                        .collect(Collectors.toList()));

        return bucketsNum;
    }

    private ArrayList<AddPartitionClause> getAddPartitionClause(Database db, OlapTable olapTable,
            Column partitionColumn, String partitionFormat, boolean executeFirstTime) throws DdlException {
        ArrayList<AddPartitionClause> addPartitionClauses = new ArrayList<>();
        DynamicPartitionProperty dynamicPartitionProperty = olapTable.getTableProperty().getDynamicPartitionProperty();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        ZonedDateTime now = ZonedDateTime.now(dynamicPartitionProperty.getTimeZone().toZoneId());

        boolean createHistoryPartition = dynamicPartitionProperty.isCreateHistoryPartition();
        int idx;
        // When enable create_history_partition, will check the valid value from start and history_partition_num.
        if (createHistoryPartition) {
            idx = DynamicPartitionUtil.getRealStart(dynamicPartitionProperty.getStart(),
                    dynamicPartitionProperty.getHistoryPartitionNum());
        } else {
            idx = 0;
        }
        int hotPartitionNum = dynamicPartitionProperty.getHotPartitionNum();
        String storagePolicyName = dynamicPartitionProperty.getStoragePolicy();

        String nowPartitionPrevBorder = DynamicPartitionUtil.getPartitionRangeString(
                dynamicPartitionProperty, now, 0, partitionFormat);
        String nowPartitionName = dynamicPartitionProperty.getPrefix()
                + DynamicPartitionUtil.getFormattedPartitionName(dynamicPartitionProperty.getTimeZone(),
                nowPartitionPrevBorder, dynamicPartitionProperty.getTimeUnit());

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
            } catch (Exception e) {
                // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
                // IllegalArgumentException: lb is greater than ub
                LOG.warn("Error in gen addPartitionKeyRange. db: {}, table: {}, partition idx: {}",
                        db.getFullName(), olapTable.getName(), idx, e);
                if (executeFirstTime) {
                    throw new DdlException("maybe dynamic_partition.start is too small, error: "
                            + e.getMessage());
                }
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
                            LOG.debug("partition range {} exist in db {} table {} partition idx {}, clear fail msg",
                                    addPartitionKeyRange, db.getFullName(), olapTable.getName(), idx);
                        }
                        clearCreatePartitionFailedMsg(olapTable.getId());
                    } else {
                        LOG.warn("check partition range {} in db {} table {} partiton idx {} fail",
                                addPartitionKeyRange, db.getFullName(), olapTable.getName(), idx, e);
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
            int bucketsNum = getBucketsNum(dynamicPartitionProperty, olapTable, partitionName,
                    nowPartitionName, executeFirstTime);
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
        // 1. no hot partition, then use dynamic_partition.storage_medium
        // 2. has hot partition
        //    1) dynamic_partition.storage_medium = 'ssd', then use ssd;
        //    2) otherwise
        //       a. cooldown partition, then use hdd
        //       b. hot partition. then use ssd
        if (hotPartitionNum <= 0 || property.getStorageMedium().equalsIgnoreCase("ssd")) {
            if (!Strings.isNullOrEmpty(property.getStorageMedium())) {
                partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, property.getStorageMedium());
                partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                        TimeUtils.longToTimeString(DataProperty.MAX_COOLDOWN_TIME_MS));
            }
        } else if (offset + hotPartitionNum <= 0) {
            partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, TStorageMedium.HDD.name());
            partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME,
                    TimeUtils.longToTimeString(DataProperty.MAX_COOLDOWN_TIME_MS));
        } else {
            String cooldownTime = DynamicPartitionUtil.getPartitionRangeString(
                    property, now, offset + hotPartitionNum, DynamicPartitionUtil.DATETIME_FORMAT);
            partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, TStorageMedium.SSD.name());
            partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, cooldownTime);
        }
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
        } catch (org.apache.doris.common.AnalysisException | org.apache.doris.nereids.exceptions.AnalysisException e) {
            // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
            // IllegalArgumentException: lb is greater than ub
            LOG.warn("Error in gen reservePartitionKeyRange. {}, table: {}",
                    db.getFullName(), olapTable.getName(), e);
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

        // drop partition only considering start, not considering history_partition_num.
        // int realStart = DynamicPartitionUtil.getRealStart(dynamicPartitionProperty.getStart(),
        //         dynamicPartitionProperty.getHistoryPartitionNum());
        int realStart = dynamicPartitionProperty.getStart();
        ZonedDateTime now = ZonedDateTime.now(dynamicPartitionProperty.getTimeZone().toZoneId());
        String lowerBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty,
                now, realStart, partitionFormat);
        PartitionValue lowerPartitionValue = new PartitionValue(lowerBorder);
        List<Range<PartitionKey>> reservedHistoryPartitionKeyRangeList = new ArrayList<Range<PartitionKey>>();
        Range<PartitionKey> reservePartitionKeyRange;
        try {
            PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerPartitionValue),
                    Collections.singletonList(partitionColumn));
            reservePartitionKeyRange = Range.atLeast(lowerBound);
            reservedHistoryPartitionKeyRangeList.add(reservePartitionKeyRange);
        } catch (Exception e) {
            // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
            // IllegalArgumentException: lb is greater than ub
            String hint = "'dynamic_partition.start' = " + dynamicPartitionProperty.getStart()
                    + ", maybe it's too small, can use alter table sql to increase it. ";
            LOG.warn("Error in gen reservePartitionKeyRange. db: {}, table: {}. {}",
                    db.getFullName(), olapTable.getName(), hint, e);
            recordDropPartitionFailedMsg(db.getFullName(), olapTable.getName(), hint + ", error: " + e.getMessage(),
                    olapTable.getId());
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
                } catch (org.apache.doris.common.AnalysisException
                        | org.apache.doris.nereids.exceptions.AnalysisException e) {
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

    // make public just for fe ut
    public void executeDynamicPartition(Collection<Pair<Long, Long>> dynamicPartitionTableInfoCol,
            boolean executeFirstTime) throws DdlException {
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
                    || olapTable instanceof MTMV
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
                    addPartitionClauses = getAddPartitionClause(db, olapTable, partitionColumn, partitionFormat,
                            executeFirstTime);
                }
                clearDropPartitionFailedMsg(olapTable.getId());
                dropPartitionClauses = getDropPartitionClause(db, olapTable, partitionColumn, partitionFormat);
                tableName = olapTable.getName();
            } catch (Exception e) {
                LOG.warn("db [{}-{}], table [{}-{}]'s dynamic partition has error",
                        db.getId(), db.getName(), olapTable.getId(), olapTable.getName(), e);
                if (executeFirstTime) {
                    throw new DdlException(e.getMessage());
                }
            } finally {
                olapTable.readUnlock();
            }

            for (DropPartitionClause dropPartitionClause : dropPartitionClauses) {
                if (!olapTable.writeLockIfExist()) {
                    continue;
                }
                try {
                    Env.getCurrentEnv().dropPartition(db, olapTable, dropPartitionClause);
                } catch (Exception e) {
                    recordDropPartitionFailedMsg(db.getFullName(), tableName, e.getMessage(), olapTable.getId());
                    LOG.warn("db [{}-{}], table [{}-{}]'s dynamic partition has error",
                            db.getId(), db.getName(), olapTable.getId(), olapTable.getName(), e);
                    if (executeFirstTime) {
                        throw new DdlException(e.getMessage());
                    }
                } finally {
                    olapTable.writeUnlock();
                }
            }

            if (!skipAddPartition) {
                // get partitionIds and indexIds
                List<Long> indexIds = new ArrayList<>(olapTable.getCopiedIndexIdToMeta().keySet());
                List<Long> generatedPartitionIds = new ArrayList<>();
                cloudBatchBeforeCreatePartitions(executeFirstTime, addPartitionClauses, olapTable, indexIds,
                        db, tableName, generatedPartitionIds);

                List<PartitionPersistInfo> partsInfo = new ArrayList<>();
                for (int i = 0; i < addPartitionClauses.size(); i++) {
                    try {
                        boolean needWriteEditLog = true;
                        // ATTN: !executeFirstTime, needWriteEditLog
                        // here executeFirstTime is create table, so in cloud edit log will postpone
                        if (Config.isCloudMode()) {
                            needWriteEditLog = !executeFirstTime;
                        }
                        PartitionPersistInfo info =
                                Env.getCurrentEnv().addPartition(db, tableName, addPartitionClauses.get(i),
                                    executeFirstTime,
                                    executeFirstTime && Config.isCloudMode() ? generatedPartitionIds.get(i) : 0,
                                    needWriteEditLog);
                        if (info == null) {
                            throw new Exception("null persisted partition returned");
                        }
                        partsInfo.add(info);
                        clearCreatePartitionFailedMsg(olapTable.getId());
                    } catch (Exception e) {
                        recordCreatePartitionFailedMsg(db.getFullName(), tableName, e.getMessage(), olapTable.getId());
                        LOG.warn("has error", e);
                        if (executeFirstTime) {
                            throw new DdlException(e.getMessage());
                        }
                    }
                }
                cloudBatchAfterCreatePartitions(executeFirstTime, partsInfo,
                        addPartitionClauses, db, olapTable, indexIds, tableName);
            }
        }
    }

    private void cloudBatchAfterCreatePartitions(boolean executeFirstTime, List<PartitionPersistInfo> partsInfo,
                                                       ArrayList<AddPartitionClause> addPartitionClauses, Database db,
                                                       OlapTable olapTable, List<Long> indexIds,
                                                       String tableName) throws DdlException {
        if (Config.isNotCloudMode()) {
            return;
        }
        List<Long> succeedPartitionIds = partsInfo.stream().map(partitionPersistInfo
                -> partitionPersistInfo.getPartition().getId()).collect(Collectors.toList());
        if (!executeFirstTime || addPartitionClauses.isEmpty()) {
            LOG.info("cloud commit rpc in batch, {}-{}", !executeFirstTime, addPartitionClauses.size());
            return;
        }
        try {
            // ATTN: failedPids = generatedPartitionIds - succeedPartitionIds,
            // means some partitions failed when addPartition, failedPids will be recycled by recycler
            if (DebugPointUtil.isEnable("FE.DynamicPartitionScheduler.before.commitCloudPartition")) {
                LOG.info("debug point FE.DynamicPartitionScheduler.before.commitCloudPartition, throw e");
                // not commit, not log edit
                throw new Exception("debug point FE.DynamicPartitionScheduler.before.commitCloudPartition");
            }
            Env.getCurrentInternalCatalog().afterCreatePartitions(db.getId(), olapTable.getId(),
                    succeedPartitionIds, indexIds, true);
            LOG.info("begin write edit log to add partitions in batch, "
                    + "numPartitions: {}, db: {}, table: {}, tableId: {}",
                    partsInfo.size(), db.getFullName(), tableName, olapTable.getId());
            // ATTN: here, edit log must after commit cloud partition,
            // prevent commit RPC failure from causing data loss
            if (DebugPointUtil.isEnable("FE.DynamicPartitionScheduler.before.logEditPartitions")) {
                LOG.info("debug point FE.DynamicPartitionScheduler.before.logEditPartitions, throw e");
                // committed, but not log edit
                throw new Exception("debug point FE.DynamicPartitionScheduler.before.commitCloudPartition");
            }
            for (int i = 0; i < partsInfo.size(); i++) {
                Env.getCurrentEnv().getEditLog().logAddPartition(partsInfo.get(i));
                if (DebugPointUtil.isEnable("FE.DynamicPartitionScheduler.in.logEditPartitions")) {
                    if (i == partsInfo.size() / 2) {
                        LOG.info("debug point FE.DynamicPartitionScheduler.in.logEditPartitions, throw e");
                        // committed, but log some edit, others failed
                        throw new Exception("debug point FE.DynamicPartitionScheduler"
                            + ".in.commitCloudPartition");
                    }
                }
            }
            LOG.info("finish write edit log to add partitions in batch, "
                    + "numPartitions: {}, db: {}, table: {}, tableId: {}",
                    partsInfo.size(), db.getFullName(), tableName, olapTable.getId());
        } catch (Exception e) {
            LOG.warn("cloud in commit step, dbName {}, tableName {}, tableId {} exception {}",
                    db.getFullName(), tableName, olapTable.getId(), e.getMessage());
            recordCreatePartitionFailedMsg(db.getFullName(), tableName, e.getMessage(), olapTable.getId());
            throw new DdlException("cloud in commit step err");
        }
    }

    private void cloudBatchBeforeCreatePartitions(boolean executeFirstTime,
                                                  ArrayList<AddPartitionClause> addPartitionClauses,
                                                  OlapTable olapTable, List<Long> indexIds, Database db,
                                                  String tableName, List<Long> generatedPartitionIds)
            throws DdlException {
        if (Config.isNotCloudMode()) {
            return;
        }
        if (!executeFirstTime || addPartitionClauses.isEmpty()) {
            LOG.info("cloud prepare rpc in batch, {}-{}", !executeFirstTime, addPartitionClauses.size());
            return;
        }
        AddPartitionClause addPartitionClause = addPartitionClauses.get(0);
        DistributionDesc distributionDesc = addPartitionClause.getDistributionDesc();
        try {
            DistributionInfo distributionInfo = distributionDesc
                    .toDistributionInfo(olapTable.getBaseSchema());
            if (distributionDesc == null) {
                distributionInfo =  olapTable.getDefaultDistributionInfo()
                    .toDistributionDesc().toDistributionInfo(olapTable.getBaseSchema());
            }
            long allPartitionBufferSize = 0;
            for (int i = 0; i < addPartitionClauses.size(); i++) {
                long bufferSize = InternalCatalog.checkAndGetBufferSize(indexIds.size(),
                        distributionInfo.getBucketNum(),
                        addPartitionClause.getSingeRangePartitionDesc()
                        .getReplicaAlloc().getTotalReplicaNum(),
                        db, tableName);
                allPartitionBufferSize += bufferSize;
            }
            MetaIdGenerator.IdGeneratorBuffer idGeneratorBuffer = Env.getCurrentEnv()
                    .getIdGeneratorBuffer(allPartitionBufferSize);
            addPartitionClauses.forEach(p -> generatedPartitionIds.add(idGeneratorBuffer.getNextId()));
            // executeFirstTime true
            Env.getCurrentInternalCatalog().beforeCreatePartitions(db.getId(), olapTable.getId(),
                    generatedPartitionIds, indexIds, true);
        } catch (Exception e) {
            LOG.warn("cloud in prepare step, dbName {}, tableName {}, tableId {} indexId {} exception {}",
                    db.getFullName(), tableName, olapTable.getId(), indexIds, e.getMessage());
            recordCreatePartitionFailedMsg(db.getFullName(), tableName, e.getMessage(), olapTable.getId());
            throw new DdlException("cloud in prepare step err");
        }
    }

    private void recordCreatePartitionFailedMsg(String dbName, String tableName, String msg, long tableId) {
        LOG.info("dynamic add partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
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
            try {
                executeDynamicPartition(dynamicPartitionTableInfo, false);
            } catch (Exception e) {
                // previous had log DdlException
                if (LOG.isDebugEnabled()) {
                    LOG.debug("dynamic partition has error: ", e);
                }
            }
        }
    }
}
