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

import com.google.common.collect.Range;
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is used to periodically add or drop partition on an olapTable which specify dynamic partition properties
 * Config.dynamic_partition_enable determine whether this feature is enable, Config.dynamic_partition_check_interval_seconds
 * determine how often the task is performed
 */
public class DynamicPartitionScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionScheduler.class);

    private final String defaultValue = "N/A";
    public String lastSchedulerTime = defaultValue;
    public String lastUpdateTime = defaultValue;
    public State dynamicPartitionState = State.NORMAL;
    public String msg = defaultValue;

    public enum State {
        NORMAL,
        ERROR
    }

    private Set<Pair<Long, Long>> dynamicPartitionTableInfo = new HashSet<>();
    private boolean initialize;

    public DynamicPartitionScheduler(String name, long intervalMs) {
        super(name, intervalMs);
        this.initialize = false;
    }
    public synchronized void registerDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.add(new Pair<>(dbId, tableId));
    }

    public synchronized void removeDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.remove(new Pair<>(dbId, tableId));
    }

    private void dynamicAddPartition() {
        for (Pair<Long, Long> tableInfo : dynamicPartitionTableInfo) {
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null) {
                removeDynamicPartitionTable(dbId, tableId);
                continue;
            }
            db.readLock();
            try {
                Table table = db.getTable(tableId);
                if (table == null ||
                        !Boolean.parseBoolean(((OlapTable) table).getTableProperty().getDynamicPartitionProperty().getEnable())) {
                    removeDynamicPartitionTable(dbId, tableId);
                    continue;
                }
            } finally {
                db.readUnlock();
            }

            // Determine the partition column type
            // if column type is Date, format partition name as yyyyMMdd
            // if column type is DateTime, format partition name as yyyyMMddHHssmm
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
            Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
            String partitionFormat = DynamicPartitionUtil.getPartitionFormat(partitionColumn);

            Calendar calendar = Calendar.getInstance();
            TableProperty tableProperty = olapTable.getTableProperty();
            DynamicPartitionProperty dynamicPartitionProperty = tableProperty.getDynamicPartitionProperty();
            int end = Integer.parseInt(dynamicPartitionProperty.getEnd());
            for (int i = 0; i <= end; i++) {
                String dynamicPartitionPrefix = dynamicPartitionProperty.getPrefix();
                String partitionRange = DynamicPartitionUtil.getPartitionRange(dynamicPartitionProperty.getTimeUnit(),
                        i, (Calendar) calendar.clone(), partitionFormat);
                String partitionName = dynamicPartitionPrefix + DynamicPartitionUtil.getFormattedPartitionName(partitionRange);

                // continue if partition already exists
                String nextBorder = DynamicPartitionUtil.getPartitionRange(dynamicPartitionProperty.getTimeUnit(),
                        i + 1, (Calendar) calendar.clone(), partitionFormat);
                PartitionInfo partitionInfo = olapTable.getPartitionInfo();
                if (partitionInfo.getType() != PartitionType.RANGE) {
                    continue;
                }
                boolean isPartitionExists = false;

                RangePartitionInfo info = (RangePartitionInfo) (partitionInfo);
                for (Range<PartitionKey> partitionKeyRange : info.getIdToRange().values()) {
                    // only support single column partition now
                    if (partitionKeyRange.upperEndpoint().getKeys().get(0).getStringValue().equals(nextBorder)) {
                        isPartitionExists = true;
                        break;
                    }
                }
                if (isPartitionExists) {
                    continue;
                }

                // construct partition desc
                PartitionValue partitionValue = new PartitionValue(nextBorder);
                PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(Collections.singletonList(partitionValue));
                HashMap<String, String> partitionProperties = new HashMap<>(1);
                partitionProperties.put("replication_num", String.valueOf(DynamicPartitionUtil.estimateReplicateNum(olapTable)));
                SingleRangePartitionDesc rangePartitionDesc = new SingleRangePartitionDesc(true, partitionName,
                        partitionKeyDesc, partitionProperties);

                // construct distribution desc
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) olapTable.getDefaultDistributionInfo();
                List<String> distColumnNames = new ArrayList<>();
                for (Column distributionColumn : hashDistributionInfo.getDistributionColumns()) {
                    distColumnNames.add(distributionColumn.getName());
                }
                DistributionDesc distributionDesc = new HashDistributionDesc(
                        Integer.parseInt(dynamicPartitionProperty.getBuckets()), distColumnNames);

                // add partition according to partition desc and distribution desc
                AddPartitionClause addPartitionClause = new AddPartitionClause(rangePartitionDesc, distributionDesc, null);
                try {
                    Catalog.getInstance().addPartition(db, olapTable.getName(), addPartitionClause);
                    dynamicPartitionState = State.NORMAL;
                    msg = defaultValue;
                } catch (DdlException e) {
                    LOG.info("Dynamic add partition failed: " + e.getMessage());
                    dynamicPartitionState = State.ERROR;
                    msg = e.getMessage();
                } finally {
                    lastSchedulerTime = TimeUtils.getCurrentFormatTime();
                }
            }
        }
    }

    private void initDynamicPartitionTable() {
        for (Long dbId : Catalog.getInstance().getDbIds()) {
            Database db = Catalog.getInstance().getDb(dbId);
            db.readLock();
            try {
                for (Table table : Catalog.getInstance().getDb(dbId).getTables()) {
                    if (DynamicPartitionUtil.isDynamicPartitionTable(table)) {
                        registerDynamicPartitionTable(db.getId(), table.getId());
                    }
                }
            } finally {
                db.readUnlock();
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
        dynamicAddPartition();
    }
}