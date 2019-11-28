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

package org.apache.doris.catalog;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.AddPartitionClause;
import org.apache.doris.analysis.DistributionDesc;
import org.apache.doris.analysis.HashDistributionDesc;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SingleRangePartitionDesc;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.task.DynamicPartitionScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableProperty implements Writable {
    private static final Logger LOG = LogManager.getLogger(TableProperty.class);

    public static final String LAST_UPDATE_TIME = "LastUpdateTime";
    public static final String MSG = "Msg";
    public static final String STATE = "State";

    private static final String TIMESTAMP_FORMAT = "yyyyMMdd";
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private long dbId;
    private long tableId;
    private Map<String, String> dynamicProperties = new HashMap<>();

    public enum State {
        NORMAL,
        CANCELLED,
        ERROR
    }

    public TableProperty() {}

    public TableProperty(long dbId, long tableId, Map<String, String> dynamicProperties) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.dynamicProperties = dynamicProperties;
    }

    public long getTableId() {
        return tableId;
    }

    public long getDbId() {
        return dbId;
    }

    public Map<String, String> getDynamicProperties() {
        return dynamicProperties;
    }

    public String getDynamicPartitionTimeUnit() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_TIME_UNIT);
    }

    public String getDynamicPartitionPrefix() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_PREFIX);
    }

    public String getDynamicPartitionEnd() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_END);
    }

    public String getDynamicPartitionBuckets() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYNAMIC_PARTITION_BUCKETS);
    }


    public String getDynamicPartitionEnable() {
        return this.dynamicProperties.get(PropertyAnalyzer.PROPERTIES_DYANMIC_PARTITION_ENABLE);
    }

    public String getLastUpdateTime() {
        return this.dynamicProperties.get(LAST_UPDATE_TIME);
    }

    public void setState(State state) {
        this.dynamicProperties.put(STATE, state.toString());
    }

    public String getState() {
        return this.dynamicProperties.get(STATE);
    }

    public void setMsg(String msg) {
        this.dynamicProperties.put(MSG, msg);
    }

    public String getMsg() {
        String msg = this.dynamicProperties.get(MSG);
        return Strings.isNullOrEmpty(msg) ? "N/A" : msg;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        out.writeLong(tableId);
        int counter = dynamicProperties.size();
        out.writeInt(counter);
        for (Map.Entry<String, String> entry : dynamicProperties.entrySet()) {
            String dynamicPropertyName = entry.getKey();
            String dynamicPropertyValue = entry.getValue();
            Text.writeString(out, dynamicPropertyName);
            Text.writeString(out, dynamicPropertyValue);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        tableId = in.readLong();
        int counter = in.readInt();
        for (int i = 0; i < counter; i++) {
            String dynamicPropertyName = Text.readString(in);
            String dynamicPropertyValue = Text.readString(in);
            dynamicProperties.put(dynamicPropertyName, dynamicPropertyValue);
        }
    }

    public static TableProperty read(DataInput in) throws IOException {
        TableProperty info = new TableProperty();
        info.readFields(in);
        return info;
    }

    public static boolean checkDynamicPartitionTable(Table table) {
        if (!(table instanceof OlapTable) ||
                !(((OlapTable) table).getPartitionInfo().getType().equals(PartitionType.RANGE))) {
            return false;
        }
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) ((OlapTable) table).getPartitionInfo();
        String enable = ((OlapTable) table).getTableProperty().getDynamicPartitionEnable();
        return rangePartitionInfo.getPartitionColumns().size() == 1 &&
                !Strings.isNullOrEmpty(enable) && enable.equalsIgnoreCase(PropertyAnalyzer.TRUE);
    }

    public static void dynamicAddPartition() throws DdlException {
        for (Pair<Long, Long> tableInfo : DynamicPartitionScheduler.getDynamicPartitionTableInfo()) {
            Long dbId = tableInfo.first;
            Long tableId = tableInfo.second;
            Database db = Catalog.getInstance().getDb(dbId);
            if (db == null || db.getTable((tableId)) == null) {
                DynamicPartitionScheduler.removeDynamicPartitionTable(dbId, tableId);
                continue;
            }

            OlapTable table = (OlapTable) db.getTable(tableId);
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
                if (table.getPartition(validPartitionName) != null ||
                        !(table.getDefaultDistributionInfo() instanceof HashDistributionInfo)) {
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

    private static String getPartitionRange(String timeUnit, int offset, Calendar calendar, String format) {
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

    private static int estimateReplicateNum(OlapTable table) {
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
}
