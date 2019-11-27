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

package org.apache.doris.task;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
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
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DynamicPartitionTask extends Daemon {
    private static final Logger LOG = LogManager.getLogger(DynamicPartitionTask.class);

    private static DynamicPartitionTask INSTANCE = null;

    private static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private Set<Pair<Long, Long>> dynamicPartitionTableInfo;
    private boolean initialize;

    private DynamicPartitionTask() {
        this.dynamicPartitionTableInfo = new HashSet<>();
        this.initialize = false;
    }

    public static DynamicPartitionTask getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new DynamicPartitionTask();
        }
        return INSTANCE;
    }

    private void initDynamicPartitionTable() {
        for (Long dbId : Catalog.getInstance().getDbIds()) {
            Database db = Catalog.getInstance().getDb(dbId);
            for (Table table : Catalog.getInstance().getDb(dbId).getTables()) {
                if (checkDynamicPartitionTable(table)) {
                    registerDynamicPartitionTable(db.getId(), table.getId());
                }
            }
        }
        this.initialize = true;
    }

    private boolean checkDynamicPartitionTable(Table table) {
        if (!(table instanceof OlapTable)) {
            return false;
        }
        if (!(((OlapTable) table).getPartitionInfo().getType().equals(PartitionType.RANGE))) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) ((OlapTable) table).getPartitionInfo();
        String enable = ((OlapTable) table).getTableProperty().getDynamicPartitionEnable();
        return rangePartitionInfo.getPartitionColumns().size() == 1 &&
                !Strings.isNullOrEmpty(enable) && enable.equalsIgnoreCase(PropertyAnalyzer.TRUE);
    }

    public void registerDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.add(new Pair<>(dbId, tableId));
    }

    public void removeDynamicPartitionTable(Long dbId, Long tableId) {
        dynamicPartitionTableInfo.remove(new Pair<>(dbId, tableId));
    }

    private void dynamicAddPartition() throws DdlException {
        for (Pair<Long, Long> tableInfo : dynamicPartitionTableInfo) {
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null) {
                dynamicPartitionTableInfo.remove(new Pair<>(dbId, tableId));
                continue;
            }
            OlapTable table = (OlapTable) db.getTable(tableId);
            if (table == null) {
                dynamicPartitionTableInfo.remove(new Pair<>(dbId, tableId));
                continue;
            }

            String rangePartitionFormat;
            RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) table.getPartitionInfo();
            Column partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
            if (partitionColumn.getDataType().equals(PrimitiveType.DATE)) {
                rangePartitionFormat = DATE_FORMAT;
            } else if (partitionColumn.getDataType().equals(PrimitiveType.DATETIME)) {
                rangePartitionFormat = DATETIME_FORMAT;
            } else {
                rangePartitionFormat = TIMESTAMP_FORMAT;
            }

            Calendar calendar = Calendar.getInstance();
            TableProperty tableProperty = table.getTableProperty();
            int end = Integer.parseInt(tableProperty.getDynamicPartitionEnd());
            for (int i = 1; i <= end; i++) {
                String dynamicPartitionPrefix = tableProperty.getDynamicPartitionPrefix();
                String partitionName = dynamicPartitionPrefix + getPartitionRange(
                        tableProperty.getDynamicPartitionTimeUnit(), i, (Calendar) calendar.clone(), rangePartitionFormat);
                String validPartitionName = partitionName.replace("-", "");
                if (table.getPartition(validPartitionName) != null) {
                    continue;
                }

                if (!(table.getDefaultDistributionInfo() instanceof HashDistributionInfo)) {
                    LOG.warn("Auto add partition task should only support hash distribution buckets");
                    continue;
                }

                String nextBorder = getPartitionRange(tableProperty.getDynamicPartitionTimeUnit(),
                        i + 1,(Calendar) calendar.clone(), rangePartitionFormat);
                PartitionValue partitionValue = new PartitionValue(nextBorder);
                PartitionKeyDesc partitionKeyDesc = new PartitionKeyDesc(Collections.singletonList(partitionValue));
                HashMap<String, String> partitionProperties = new HashMap<>(1);
                partitionProperties.put("replication_num", String.valueOf(estimateReplicateNum(table)));
                SingleRangePartitionDesc desc = new SingleRangePartitionDesc(false,
                        validPartitionName, partitionKeyDesc, partitionProperties);

                HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) table.getDefaultDistributionInfo();
                List<String> distColumnNames = new ArrayList<>();
                for (Column distributionColumn : hashDistributionInfo.getDistributionColumns()) {
                    distColumnNames.add(distributionColumn.getName());
                }
                DistributionDesc distributionDesc = new HashDistributionDesc(
                        Integer.parseInt(tableProperty.getDynamicPartitionBuckets()), distColumnNames);

                AddPartitionClause addPartitionClause = new AddPartitionClause(desc, distributionDesc, null);
                HashMap<String, String> properties = Maps.newHashMapWithExpectedSize(2);
                try {
                    Catalog.getInstance().addPartition(db, table.getName(), addPartitionClause);
                    properties.put(TableProperty.STATE, TableProperty.State.NORMAL.toString());
                    properties.put(TableProperty.MSG, " ");
                } catch (DdlException e) {
                    properties.put(TableProperty.STATE, TableProperty.State.ERROR.toString());
                    properties.put(TableProperty.MSG, e.getMessage());
                } finally {
                    Catalog.getInstance().modifyTableDynamicPartition(db, table, properties);
                }
            }
        }
    }

    private String getPartitionRange(String timeUnit, int offset, Calendar calendar, String format) {
        if (timeUnit.equalsIgnoreCase(PropertyAnalyzer.DAY)) {
            calendar.add(Calendar.DAY_OF_MONTH, offset);
        } else if (timeUnit.equalsIgnoreCase(PropertyAnalyzer.WEEK)){
            calendar.add(Calendar.WEEK_OF_MONTH, offset);
        } else if (timeUnit.equalsIgnoreCase(PropertyAnalyzer.MONTH)) {
            calendar.add(Calendar.MONTH, offset);
        } else {
            LOG.error("Unsupported time unit: " + timeUnit);
            return null;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(calendar.getTime());
    }

    private int estimateReplicateNum(OlapTable table) {
        int replicateNum = 3;
        long maxPartitionId = 0;
        for (Partition partition: table.getPartitions()) {
            if (partition.getId() > maxPartitionId) {
                maxPartitionId = partition.getId();
                replicateNum = table.getPartitionInfo().getReplicationNum(partition.getId());
            }
        }
        return replicateNum;
    }

    @Override
    protected void runOneCycle() {
        if (!initialize) {
            initDynamicPartitionTable();
        }
        try {
            dynamicAddPartition();
        } catch (DdlException e) {
            LOG.warn("dynamic add partition failed: " + e.getMessage());
        }
    }
}
