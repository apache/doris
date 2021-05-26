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
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
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
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.RangeUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TStorageMedium;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

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
 * Config.dynamic_partition_enable determine whether this feature is enable, Config.dynamic_partition_check_interval_seconds
 * determine how often the task is performed
 */
public class DynamicPartitionScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionScheduler.class);
    public static final String LAST_SCHEDULER_TIME = "lastSchedulerTime";
    public static final String LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String DYNAMIC_PARTITION_STATE = "dynamicPartitionState";
    public static final String CREATE_PARTITION_MSG = "createPartitionMsg";
    public static final String DROP_PARTITION_MSG = "dropPartitionMsg";

    private final String DEFAULT_RUNTIME_VALUE = FeConstants.null_string;

    private Map<String, Map<String, String>> runtimeInfos = Maps.newConcurrentMap();
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
        List<Pair<Long, Long>> tempDynamicPartitionTableInfo = Lists.newArrayList(new Pair<>(dbId, tableId));
        executeDynamicPartition(tempDynamicPartitionTableInfo);
    }

    public void registerDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.add(new Pair<>(dbId, tableId));
    }

    public void removeDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.remove(new Pair<>(dbId, tableId));
    }

    public String getRuntimeInfo(String tableName, String key) {
        Map<String, String> tableRuntimeInfo = runtimeInfos.getOrDefault(tableName, createDefaultRuntimeInfo());
        return tableRuntimeInfo.getOrDefault(key, DEFAULT_RUNTIME_VALUE);
    }

    public void removeRuntimeInfo(String tableName) {
        runtimeInfos.remove(tableName);
    }

    public void createOrUpdateRuntimeInfo(String tableName, String key, String value) {
        Map<String, String> runtimeInfo = runtimeInfos.get(tableName);
        if (runtimeInfo == null) {
            runtimeInfo = createDefaultRuntimeInfo();
            runtimeInfo.put(key, value);
            runtimeInfos.put(tableName, runtimeInfo);
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

    private ArrayList<AddPartitionClause> getAddPartitionClause(Database db, OlapTable olapTable,
            Column partitionColumn, String partitionFormat) {
        ArrayList<AddPartitionClause> addPartitionClauses = new ArrayList<>();
        DynamicPartitionProperty dynamicPartitionProperty = olapTable.getTableProperty().getDynamicPartitionProperty();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
        ZonedDateTime now = ZonedDateTime.now(dynamicPartitionProperty.getTimeZone().toZoneId());

        boolean createHistoryPartition = dynamicPartitionProperty.isCreateHistoryPartition();
        int idx = createHistoryPartition ? dynamicPartitionProperty.getStart() : 0;
        int hotPartitionNum = dynamicPartitionProperty.getHotPartitionNum();
        String timeUnit = dynamicPartitionProperty.getTimeUnit();

        for (; idx <= dynamicPartitionProperty.getEnd(); idx++) {
            String prevBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty, now, idx, partitionFormat);
            String nextBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty, now, idx + 1, partitionFormat);
            PartitionValue lowerValue = new PartitionValue(prevBorder);
            PartitionValue upperValue = new PartitionValue(nextBorder);

            boolean isPartitionExists = false;
            Range<PartitionKey> addPartitionKeyRange;
            try {
                PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerValue), Collections.singletonList(partitionColumn));
                PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(upperValue), Collections.singletonList(partitionColumn));
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
                } catch (DdlException e) {
                    isPartitionExists = true;
                    if (addPartitionKeyRange.equals(partitionItem.getItems())) {
                        clearCreatePartitionFailedMsg(olapTable.getName());
                    } else {
                        recordCreatePartitionFailedMsg(db.getFullName(), olapTable.getName(), e.getMessage());
                    }
                    break;
                }
            }
            if (isPartitionExists) {
                continue;
            }

            // construct partition desc
            PartitionKeyDesc partitionKeyDesc = PartitionKeyDesc.createFixed(Collections.singletonList(lowerValue), Collections.singletonList(upperValue));
            HashMap<String, String> partitionProperties = Maps.newHashMap();
            if (dynamicPartitionProperty.getReplicationNum() == DynamicPartitionProperty.NOT_SET_REPLICATION_NUM) {
                partitionProperties.put("replication_num", String.valueOf(olapTable.getDefaultReplicationNum()));
            } else {
                partitionProperties.put("replication_num", String.valueOf(dynamicPartitionProperty.getReplicationNum()));
            }

            if (hotPartitionNum > 0) {
                // set storage_medium and storage_cooldown_time based on dynamic_partition.hot_partition_num
                setStorageMediumProperty(partitionProperties, dynamicPartitionProperty, now, hotPartitionNum, idx);
            }


            String partitionName = dynamicPartitionProperty.getPrefix() + DynamicPartitionUtil.getFormattedPartitionName(
                    dynamicPartitionProperty.getTimeZone(), prevBorder, dynamicPartitionProperty.getTimeUnit());
            SinglePartitionDesc rangePartitionDesc = new SinglePartitionDesc(true, partitionName,
                    partitionKeyDesc, partitionProperties);

            // construct distribution desc
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) olapTable.getDefaultDistributionInfo();
            List<String> distColumnNames = new ArrayList<>();
            for (Column distributionColumn : hashDistributionInfo.getDistributionColumns()) {
                distColumnNames.add(distributionColumn.getName());
            }
            DistributionDesc distributionDesc = new HashDistributionDesc(dynamicPartitionProperty.getBuckets(), distColumnNames);

            // add partition according to partition desc and distribution desc
            addPartitionClauses.add(new AddPartitionClause(rangePartitionDesc, distributionDesc, null, false));
        }
        return addPartitionClauses;
    }

    private void setStorageMediumProperty(HashMap<String, String> partitionProperties, DynamicPartitionProperty property,
                                          ZonedDateTime now, int hotPartitionNum, int offset) {
        if (offset + hotPartitionNum <= 0) {
            return;
        }
        partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, TStorageMedium.SSD.name());
        String cooldownTime = DynamicPartitionUtil.getPartitionRangeString(property, now, offset + hotPartitionNum,
                DynamicPartitionUtil.DATETIME_FORMAT);
        partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COLDOWN_TIME, cooldownTime);
    }

    /**
     * 1. get the range of [start, 0) as a reserved range.
     * 2. get DropPartitionClause of partitions which range are before this reserved range.
     */
    private ArrayList<DropPartitionClause> getDropPartitionClause(Database db, OlapTable olapTable, Column partitionColumn, String partitionFormat) {
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
                now, 0, partitionFormat);
        PartitionValue lowerPartitionValue = new PartitionValue(lowerBorder);
        PartitionValue upperPartitionValue = new PartitionValue(upperBorder);
        Range<PartitionKey> reservePartitionKeyRange;
        try {
            PartitionKey lowerBound = PartitionKey.createPartitionKey(Collections.singletonList(lowerPartitionValue), Collections.singletonList(partitionColumn));
            PartitionKey upperBound = PartitionKey.createPartitionKey(Collections.singletonList(upperPartitionValue), Collections.singletonList(partitionColumn));
            reservePartitionKeyRange = Range.closedOpen(lowerBound, upperBound);
        } catch (AnalysisException | IllegalArgumentException e) {
            // AnalysisException: keys.size is always equal to column.size, cannot reach this exception
            // IllegalArgumentException: lb is greater than ub
            LOG.warn("Error in gen reservePartitionKeyRange. Error={}, db: {}, table: {}", e.getMessage(),
                    db.getFullName(), olapTable.getName());
            return dropPartitionClauses;
        }
        RangePartitionInfo info = (RangePartitionInfo) (olapTable.getPartitionInfo());

        List<Map.Entry<Long, PartitionItem>> idToItems = new ArrayList<>(info.getIdToItem(false).entrySet());
        idToItems.sort(Comparator.comparing(o -> ((RangePartitionItem) o.getValue()).getItems().upperEndpoint()));
        for (Map.Entry<Long, PartitionItem> idToItem : idToItems) {
            try {
                Long checkDropPartitionId = idToItem.getKey();
                Range<PartitionKey> checkDropPartitionKey = idToItem.getValue().getItems();
                RangeUtils.checkRangeIntersect(reservePartitionKeyRange, checkDropPartitionKey);
                if (checkDropPartitionKey.upperEndpoint().compareTo(reservePartitionKeyRange.lowerEndpoint()) <= 0) {
                    String dropPartitionName = olapTable.getPartition(checkDropPartitionId).getName();
                    dropPartitionClauses.add(new DropPartitionClause(false, dropPartitionName, false, false));
                }
            } catch (DdlException e) {
                break;
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
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                iterator.remove();
                continue;
            }

            ArrayList<AddPartitionClause> addPartitionClauses = new ArrayList<>();
            ArrayList<DropPartitionClause> dropPartitionClauses;
            String tableName;
            boolean skipAddPartition = false;
            OlapTable olapTable;
            olapTable = (OlapTable) db.getTable(tableId);
            // Only OlapTable has DynamicPartitionProperty
            if (olapTable == null
                    || !olapTable.dynamicPartitionExists()
                    || !olapTable.getTableProperty().getDynamicPartitionProperty().getEnable()) {
                iterator.remove();
                continue;
            }
            olapTable.readLock();
            try {
                if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                    String errorMsg = "Table[" + olapTable.getName() + "]'s state is not NORMAL."
                            + "Do not allow doing dynamic add partition. table state=" + olapTable.getState();
                    recordCreatePartitionFailedMsg(db.getFullName(), olapTable.getName(), errorMsg);
                    skipAddPartition = true;
                }

                // Determine the partition column type
                // if column type is Date, format partition name as yyyyMMdd
                // if column type is DateTime, format partition name as yyyyMMddHHssmm
                // scheduler time should be record even no partition added
                createOrUpdateRuntimeInfo(olapTable.getName(), LAST_SCHEDULER_TIME, TimeUtils.getCurrentFormatTime());
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
                } catch (DdlException e) {
                    recordCreatePartitionFailedMsg(db.getFullName(), olapTable.getName(), e.getMessage());
                    continue;
                }

                if (!skipAddPartition) {
                    addPartitionClauses = getAddPartitionClause(db, olapTable, partitionColumn, partitionFormat);
                }
                dropPartitionClauses = getDropPartitionClause(db, olapTable, partitionColumn, partitionFormat);
                tableName = olapTable.getName();
            } finally {
                olapTable.readUnlock();
            }

            for (DropPartitionClause dropPartitionClause : dropPartitionClauses) {
                olapTable.writeLock();
                try {
                    Catalog.getCurrentCatalog().dropPartition(db, olapTable, dropPartitionClause);
                    clearDropPartitionFailedMsg(tableName);
                } catch (DdlException e) {
                    recordDropPartitionFailedMsg(db.getFullName(), tableName, e.getMessage());
                } finally {
                    olapTable.writeUnlock();
                }
            }

            if (!skipAddPartition) {
                for (AddPartitionClause addPartitionClause : addPartitionClauses) {
                    try {
                        Catalog.getCurrentCatalog().addPartition(db, tableName, addPartitionClause);
                        clearCreatePartitionFailedMsg(tableName);
                    } catch (DdlException e) {
                        recordCreatePartitionFailedMsg(db.getFullName(), tableName, e.getMessage());
                    }
                }
            }
        }
    }

    private void recordCreatePartitionFailedMsg(String dbName, String tableName, String msg) {
        LOG.warn("dynamic add partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, State.ERROR.toString());
        createOrUpdateRuntimeInfo(tableName, CREATE_PARTITION_MSG, msg);
    }

    private void clearCreatePartitionFailedMsg(String tableName) {
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, State.NORMAL.toString());
        createOrUpdateRuntimeInfo(tableName, CREATE_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
    }

    private void recordDropPartitionFailedMsg(String dbName, String tableName, String msg) {
        LOG.warn("dynamic drop partition failed: {}, db: {}, table: {}", msg, dbName, tableName);
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, State.ERROR.toString());
        createOrUpdateRuntimeInfo(tableName, DROP_PARTITION_MSG, msg);
    }

    private void clearDropPartitionFailedMsg(String tableName) {
        createOrUpdateRuntimeInfo(tableName, DYNAMIC_PARTITION_STATE, State.NORMAL.toString());
        createOrUpdateRuntimeInfo(tableName, DROP_PARTITION_MSG, DEFAULT_RUNTIME_VALUE);
    }

    private void initDynamicPartitionTable() {
        for (Long dbId : Catalog.getCurrentCatalog().getDbIds()) {
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
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
