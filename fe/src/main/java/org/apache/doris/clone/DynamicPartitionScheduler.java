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
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
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

public class DynamicPartitionScheduler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionScheduler.class);

    private static final String defaultValue = "N/A";
    private static String lastSchedulerTime = defaultValue;
    private static String lastUpdateTime = defaultValue;
    private static State dynamicPartitionState = State.NORMAL;
    private static String Msg = defaultValue;

    public enum State {
        NORMAL,
        ERROR
    }

    public static String getLastSchedulerTime() {
        return lastSchedulerTime;
    }

    public static void setLastSchedulerTime(String lastSchedulerTime) {
        DynamicPartitionScheduler.lastSchedulerTime = lastSchedulerTime;
    }

    public static String getLastUpdateTime() {
        return lastUpdateTime;
    }

    public static void setLastUpdateTime(String lastUpdateTime) {
        DynamicPartitionScheduler.lastUpdateTime = lastUpdateTime;
    }

    public static State getDynamicPartitionState() {
        return dynamicPartitionState;
    }

    public static void setDynamicPartitionState(State state) {
        dynamicPartitionState = state;
    }

    public static String getMsg() {
        return Msg;
    }

    public static void setMsg(String msg) {
        Msg = msg;
    }


    private static Set<Pair<Long, Long>> dynamicPartitionTableInfo = new HashSet<>();
    private boolean initialize;

    public DynamicPartitionScheduler(String name, long intervalMs) {
        super(name, intervalMs);
        this.initialize = false;
    }
    public synchronized static void registerDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.add(new Pair<>(dbId, tableId));
    }

    public synchronized static void removeDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.remove(new Pair<>(dbId, tableId));
    }



    private void dynamicAddPartition() {
        for (Pair<Long, Long> tableInfo : dynamicPartitionTableInfo) {
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null || db.getTable((tableId)) == null) {
                DynamicPartitionScheduler.removeDynamicPartitionTable(dbId, tableId);
                continue;
            }

            // Determine the partition column type
            // if column type is Date, format partition name as yyyyMMdd
            // if column type is DateTime, format partition name as yyyyMMddHHssmm
            OlapTable table = (OlapTable) db.getTable(tableId);
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
            String partitionFormat = DynamicPartitionUtil.getPartitionFormat(partitionColumn);

            Calendar calendar = Calendar.getInstance();
            TableProperty tableProperty = table.getTableProperty();
            int end = Integer.parseInt(tableProperty.getDynamicPartitionProperty().getEnd());
            for (int i = 0; i <= end; i++) {
                String dynamicPartitionPrefix = tableProperty.getDynamicPartitionProperty().getPrefix();
                String partitionRange = DynamicPartitionUtil.getPartitionRange(tableProperty.getDynamicPartitionProperty().getTimeUnit(),
                        i, (Calendar) calendar.clone(), partitionFormat);
                String partitionName = dynamicPartitionPrefix + DynamicPartitionUtil.getFormattedPartitionName(partitionRange);
                // continue if partition already exists
                if (table.getPartition(partitionName) != null) {
                    continue;
                }

                // construct partition desc
                String nextBorder = DynamicPartitionUtil.getPartitionRange(tableProperty.getDynamicPartitionProperty().getTimeUnit(),
                        i + 1, (Calendar) calendar.clone(), partitionFormat);
                PartitionValue partitionValue = new PartitionValue(nextBorder);
                PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(Collections.singletonList(partitionValue));
                HashMap<String, String> partitionProperties = new HashMap<>(1);
                partitionProperties.put("replication_num", String.valueOf(DynamicPartitionUtil.estimateReplicateNum(table)));
                SingleRangePartitionDesc rangePartitionDesc = new SingleRangePartitionDesc(true, partitionName,
                        partitionKeyDesc, partitionProperties);

                // construct distribution desc
                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) table.getDefaultDistributionInfo();
                List<String> distColumnNames = new ArrayList<>();
                for (Column distributionColumn : hashDistributionInfo.getDistributionColumns()) {
                    distColumnNames.add(distributionColumn.getName());
                }
                DistributionDesc distributionDesc = new HashDistributionDesc(
                        Integer.parseInt(tableProperty.getDynamicPartitionProperty().getBuckets()), distColumnNames);

                // add partition according to partition desc and distribution desc
                AddPartitionClause addPartitionClause = new AddPartitionClause(rangePartitionDesc, distributionDesc, null);
                try {
                    Catalog.getInstance().addPartition(db, table.getName(), addPartitionClause);
                    setDynamicPartitionState(State.NORMAL);
                    setMsg(defaultValue);
                } catch (DdlException e) {
                    setDynamicPartitionState(State.ERROR);
                    setMsg(e.getMessage());
                } finally {
                    setLastSchedulerTime(TimeUtils.getCurrentFormatTime());
                }
            }
        }
    }

    private void initDynamicPartitionTable() {
        for (Long dbId : Catalog.getInstance().getDbIds()) {
            Database db = Catalog.getInstance().getDb(dbId);
            for (Table table : Catalog.getInstance().getDb(dbId).getTables()) {
                if (DynamicPartitionUtil.isDynamicPartitionTable(table)) {
                    registerDynamicPartitionTable(db.getId(), table.getId());
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
        dynamicAddPartition();
    }
}