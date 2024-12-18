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

import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TInvertedIndexStorageFormat;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TableProperty contains additional information about OlapTable
 * TableProperty includes properties to persistent the additional information
 * Different properties is recognized by prefix such as dynamic_partition
 * If there is different type properties is added, write a method such as buildDynamicProperty to build it.
 */
public class TableProperty implements Writable {
    private static final Logger LOG = LogManager.getLogger(TableProperty.class);

    public static final String DYNAMIC_PARTITION_PROPERTY_PREFIX = "dynamic_partition";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    // the follower variables are built from "properties"
    private DynamicPartitionProperty dynamicPartitionProperty = new DynamicPartitionProperty(Maps.newHashMap());
    private ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
    private boolean isInMemory = false;
    private short minLoadReplicaNum = -1;
    private boolean isInAtomicRestore = false;

    private String storagePolicy = "";
    private Boolean isBeingSynced = null;
    private BinlogConfig binlogConfig;

    private TStorageMedium storageMedium = null;

    /*
     * the default storage format of this table.
     * DEFAULT: depends on BE's config 'default_rowset_type'
     * V1: alpha rowset
     * V2: beta rowset
     *
     * This property should be set when creating the table, and can only be changed to V2 using Alter Table stmt.
     */
    private TStorageFormat storageFormat = TStorageFormat.DEFAULT;

    private TInvertedIndexStorageFormat invertedIndexStorageFormat = TInvertedIndexStorageFormat.DEFAULT;

    private TCompressionType compressionType = TCompressionType.LZ4F;

    private boolean enableLightSchemaChange = false;

    private boolean disableAutoCompaction = false;

    private boolean enableSingleReplicaCompaction = false;

    private boolean storeRowColumn = false;

    private boolean skipWriteIndexOnLoad = false;

    private long rowStorePageSize = PropertyAnalyzer.ROW_STORE_PAGE_SIZE_DEFAULT_VALUE;

    private String compactionPolicy = PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY;

    private long timeSeriesCompactionGoalSizeMbytes
                                    = PropertyAnalyzer.TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES_DEFAULT_VALUE;

    private long timeSeriesCompactionFileCountThreshold
                                    = PropertyAnalyzer.TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD_DEFAULT_VALUE;

    private long timeSeriesCompactionTimeThresholdSeconds
                                    = PropertyAnalyzer.TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS_DEFAULT_VALUE;

    private long timeSeriesCompactionEmptyRowsetsThreshold
                                    = PropertyAnalyzer.TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD_DEFAULT_VALUE;

    private long timeSeriesCompactionLevelThreshold
                                    = PropertyAnalyzer.TIME_SERIES_COMPACTION_LEVEL_THRESHOLD_DEFAULT_VALUE;

    private DataSortInfo dataSortInfo = new DataSortInfo();

    public TableProperty(Map<String, String> properties) {
        this.properties = properties;
    }

    public static boolean isSamePrefixProperties(Map<String, String> properties, String prefix) {
        for (String value : properties.keySet()) {
            if (!value.startsWith(prefix)) {
                return false;
            }
        }
        return true;
    }

    public TableProperty buildProperty(short opCode) {
        switch (opCode) {
            case OperationType.OP_DYNAMIC_PARTITION:
                executeBuildDynamicProperty();
                break;
            case OperationType.OP_MODIFY_REPLICATION_NUM:
                buildReplicaAllocation();
                break;
            case OperationType.OP_MODIFY_IN_MEMORY:
                buildInMemory();
                buildMinLoadReplicaNum();
                buildStorageMedium();
                buildStoragePolicy();
                buildIsBeingSynced();
                buildCompactionPolicy();
                buildTimeSeriesCompactionGoalSizeMbytes();
                buildTimeSeriesCompactionFileCountThreshold();
                buildTimeSeriesCompactionTimeThresholdSeconds();
                buildSkipWriteIndexOnLoad();
                buildEnableSingleReplicaCompaction();
                buildDisableAutoCompaction();
                buildTimeSeriesCompactionEmptyRowsetsThreshold();
                buildTimeSeriesCompactionLevelThreshold();
                break;
            default:
                break;
        }
        return this;
    }

    /**
     * Reset properties to correct values.
     *
     * @return this for chained
     */
    public TableProperty resetPropertiesForRestore(boolean reserveDynamicPartitionEnable, boolean reserveReplica,
                                                   ReplicaAllocation replicaAlloc) {
        // disable dynamic partition
        if (properties.containsKey(DynamicPartitionProperty.ENABLE)) {
            if (!reserveDynamicPartitionEnable) {
                properties.put(DynamicPartitionProperty.ENABLE, "false");
            }
            executeBuildDynamicProperty();
        }
        if (!reserveReplica) {
            setReplicaAlloc(replicaAlloc);
        }
        return this;
    }

    public TableProperty buildDynamicProperty() {
        executeBuildDynamicProperty();
        return this;
    }

    private TableProperty executeBuildDynamicProperty() {
        HashMap<String, String> dynamicPartitionProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                dynamicPartitionProperties.put(entry.getKey(), entry.getValue());
            }
        }
        dynamicPartitionProperty = new DynamicPartitionProperty(dynamicPartitionProperties);
        return this;
    }

    public TableProperty buildInMemory() {
        isInMemory = Boolean.parseBoolean(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_INMEMORY, "false"));
        return this;
    }

    public TableProperty buildInAtomicRestore() {
        isInAtomicRestore = Boolean.parseBoolean(properties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_IN_ATOMIC_RESTORE, "false"));
        return this;
    }

    public boolean isInAtomicRestore() {
        return isInAtomicRestore;
    }

    public TableProperty setInAtomicRestore() {
        properties.put(PropertyAnalyzer.PROPERTIES_IN_ATOMIC_RESTORE, "true");
        return this;
    }

    public TableProperty clearInAtomicRestore() {
        properties.remove(PropertyAnalyzer.PROPERTIES_IN_ATOMIC_RESTORE);
        return this;
    }

    public TableProperty buildEnableLightSchemaChange() {
        enableLightSchemaChange = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE, "false"));
        return this;
    }

    public TableProperty buildDisableAutoCompaction() {
        disableAutoCompaction = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_DISABLE_AUTO_COMPACTION, "false"));
        return this;
    }

    public boolean disableAutoCompaction() {
        return disableAutoCompaction;
    }

    public TableProperty buildEnableSingleReplicaCompaction() {
        enableSingleReplicaCompaction = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_ENABLE_SINGLE_REPLICA_COMPACTION, "false"));
        return this;
    }

    public boolean enableSingleReplicaCompaction() {
        return enableSingleReplicaCompaction;
    }

    public TableProperty buildStoreRowColumn() {
        storeRowColumn = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN, "false"));
        // Remove deprecated prefix and try again
        String deprecatedPrefix = "deprecated_";
        if (!storeRowColumn && PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN.startsWith(deprecatedPrefix)) {
            storeRowColumn = Boolean.parseBoolean(
                properties.getOrDefault(
                    PropertyAnalyzer.PROPERTIES_STORE_ROW_COLUMN.substring(deprecatedPrefix.length()), "false"));
        }
        return this;
    }

    public boolean storeRowColumn() {
        return storeRowColumn;
    }

    public TableProperty buildRowStorePageSize() {
        rowStorePageSize = Long.parseLong(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_ROW_STORE_PAGE_SIZE,
                                        Long.toString(PropertyAnalyzer.ROW_STORE_PAGE_SIZE_DEFAULT_VALUE)));
        return this;
    }

    public long rowStorePageSize() {
        return rowStorePageSize;
    }

    public TableProperty buildSkipWriteIndexOnLoad() {
        skipWriteIndexOnLoad = Boolean.parseBoolean(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_SKIP_WRITE_INDEX_ON_LOAD, "false"));
        return this;
    }

    public boolean skipWriteIndexOnLoad() {
        return skipWriteIndexOnLoad;
    }

    public TableProperty buildCompactionPolicy() {
        compactionPolicy = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY,
                                                                PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY);
        return this;
    }

    public String compactionPolicy() {
        return compactionPolicy;
    }

    public TableProperty buildTimeSeriesCompactionGoalSizeMbytes() {
        timeSeriesCompactionGoalSizeMbytes = Long.parseLong(properties
                    .getOrDefault(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES,
                    String.valueOf(PropertyAnalyzer.TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES_DEFAULT_VALUE)));
        return this;
    }

    public long timeSeriesCompactionGoalSizeMbytes() {
        return timeSeriesCompactionGoalSizeMbytes;
    }

    public TableProperty buildTimeSeriesCompactionFileCountThreshold() {
        timeSeriesCompactionFileCountThreshold = Long.parseLong(properties
                    .getOrDefault(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD,
                    String.valueOf(PropertyAnalyzer.TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD_DEFAULT_VALUE)));

        return this;
    }

    public long timeSeriesCompactionFileCountThreshold() {
        return timeSeriesCompactionFileCountThreshold;
    }

    public TableProperty buildTimeSeriesCompactionTimeThresholdSeconds() {
        timeSeriesCompactionTimeThresholdSeconds = Long.parseLong(properties
                    .getOrDefault(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS,
                    String.valueOf(PropertyAnalyzer.TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS_DEFAULT_VALUE)));

        return this;
    }

    public long timeSeriesCompactionTimeThresholdSeconds() {
        return timeSeriesCompactionTimeThresholdSeconds;
    }

    public TableProperty buildTimeSeriesCompactionEmptyRowsetsThreshold() {
        timeSeriesCompactionEmptyRowsetsThreshold = Long.parseLong(properties
                    .getOrDefault(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD,
                    String.valueOf(PropertyAnalyzer.TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD_DEFAULT_VALUE)));
        return this;
    }

    public long timeSeriesCompactionEmptyRowsetsThreshold() {
        return timeSeriesCompactionEmptyRowsetsThreshold;
    }

    public TableProperty buildTimeSeriesCompactionLevelThreshold() {
        timeSeriesCompactionLevelThreshold = Long.parseLong(properties
                    .getOrDefault(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD,
                    String.valueOf(PropertyAnalyzer.TIME_SERIES_COMPACTION_LEVEL_THRESHOLD_DEFAULT_VALUE)));
        return this;
    }

    public long timeSeriesCompactionLevelThreshold() {
        return timeSeriesCompactionLevelThreshold;
    }

    public TableProperty buildMinLoadReplicaNum() {
        minLoadReplicaNum = Short.parseShort(
                properties.getOrDefault(PropertyAnalyzer.PROPERTIES_MIN_LOAD_REPLICA_NUM, "-1"));
        return this;
    }

    public short getMinLoadReplicaNum() {
        return minLoadReplicaNum;
    }

    public TableProperty buildStorageMedium() {
        String storageMediumStr = properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM);
        if (Strings.isNullOrEmpty(storageMediumStr)) {
            storageMedium = null;
        } else {
            storageMedium = TStorageMedium.valueOf(storageMediumStr);
        }
        return this;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public TableProperty buildStoragePolicy() {
        storagePolicy = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY, "");
        return this;
    }

    public String getStoragePolicy() {
        return storagePolicy;
    }

    public TableProperty buildIsBeingSynced() {
        isBeingSynced = Boolean.parseBoolean(properties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED, "false"));
        return this;
    }

    public void setIsBeingSynced() {
        properties.put(PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED, "true");
        isBeingSynced = true;
    }

    public boolean isBeingSynced() {
        if (isBeingSynced == null) {
            buildIsBeingSynced();
        }
        return isBeingSynced;
    }

    public void removeInvalidProperties() {
        properties.remove(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY);
        storagePolicy = "";
        properties.remove(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH);
    }

    public TableProperty buildBinlogConfig() {
        BinlogConfig binlogConfig = new BinlogConfig();
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
            binlogConfig.setEnable(Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS)) {
            binlogConfig.setTtlSeconds(Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS)));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES)) {
            binlogConfig.setMaxBytes(Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES)));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS)) {
            binlogConfig.setMaxHistoryNums(
                    Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS)));
        }
        this.binlogConfig = binlogConfig;

        return this;
    }

    public BinlogConfig getBinlogConfig() {
        if (binlogConfig == null) {
            buildBinlogConfig();
        }
        return binlogConfig;
    }

    public void setBinlogConfig(BinlogConfig newBinlogConfig) {
        Map<String, String> binlogProperties = Maps.newHashMap();
        binlogProperties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, String.valueOf(newBinlogConfig.isEnable()));
        binlogProperties.put(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS,
                String.valueOf(newBinlogConfig.getTtlSeconds()));
        binlogProperties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES,
                String.valueOf(newBinlogConfig.getMaxBytes()));
        binlogProperties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS,
                String.valueOf(newBinlogConfig.getMaxHistoryNums()));
        modifyTableProperties(binlogProperties);
        this.binlogConfig = newBinlogConfig;
    }

    public TableProperty buildDataSortInfo() {
        HashMap<String, String> dataSortInfoProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(DataSortInfo.DATA_SORT_PROPERTY_PREFIX)) {
                dataSortInfoProperties.put(entry.getKey(), entry.getValue());
            }
        }
        dataSortInfo = new DataSortInfo(dataSortInfoProperties);
        return this;
    }

    public TableProperty buildCompressionType() {
        compressionType = TCompressionType.valueOf(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_COMPRESSION,
                TCompressionType.LZ4F.name()));
        return this;
    }

    public TableProperty buildStorageFormat() {
        storageFormat = TStorageFormat.valueOf(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT,
                TStorageFormat.DEFAULT.name()));
        return this;
    }

    public TableProperty buildInvertedIndexStorageFormat() {
        invertedIndexStorageFormat = TInvertedIndexStorageFormat.valueOf(properties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_INVERTED_INDEX_STORAGE_FORMAT,
                TInvertedIndexStorageFormat.DEFAULT.name()));
        return this;
    }

    public void modifyTableProperties(Map<String, String> modifyProperties) {
        properties.putAll(modifyProperties);
        removeDuplicateReplicaNumProperty();
    }

    public void modifyDataSortInfoProperties(DataSortInfo dataSortInfo) {
        properties.put(DataSortInfo.DATA_SORT_TYPE, String.valueOf(dataSortInfo.getSortType()));
        properties.put(DataSortInfo.DATA_SORT_COL_NUM, String.valueOf(dataSortInfo.getColNum()));
    }

    public void setReplicaAlloc(ReplicaAllocation replicaAlloc) {
        this.replicaAlloc = replicaAlloc;
        // set it to "properties" so that this info can be persisted
        properties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                replicaAlloc.toCreateStmt());
    }

    public ReplicaAllocation getReplicaAllocation() {
        return replicaAlloc;
    }

    public void modifyTableProperties(String key, String value) {
        properties.put(key, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public DynamicPartitionProperty getDynamicPartitionProperty() {
        return dynamicPartitionProperty;
    }

    public Map<String, String> getOriginDynamicPartitionProperty() {
        Map<String, String> origProp = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(DynamicPartitionProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                origProp.put(entry.getKey(), entry.getValue());
            }
        }
        return origProp;
    }

    public boolean isInMemory() {
        return isInMemory;
    }

    public boolean isAutoBucket() {
        return Boolean.parseBoolean(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_AUTO_BUCKET, "false"));
    }

    public String getEstimatePartitionSize() {
        return properties.getOrDefault(PropertyAnalyzer.PROPERTIES_ESTIMATE_PARTITION_SIZE, "");
    }

    public TStorageFormat getStorageFormat() {
        // Force convert all V1 table to V2 table
        if (TStorageFormat.V1 == storageFormat) {
            return TStorageFormat.V2;
        }
        return storageFormat;
    }

    public TInvertedIndexStorageFormat getInvertedIndexStorageFormat() {
        return invertedIndexStorageFormat;
    }

    public DataSortInfo getDataSortInfo() {
        return dataSortInfo;
    }

    public TCompressionType getCompressionType() {
        return compressionType;
    }

    public boolean getUseSchemaLightChange() {
        return enableLightSchemaChange;
    }

    public void setEnableUniqueKeyMergeOnWrite(boolean enable) {
        properties.put(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE, Boolean.toString(enable));
    }

    // In order to ensure that unique tables without the `enable_unique_key_merge_on_write` property specified
    // before version 2.1 still maintain the merge-on-read implementation after the upgrade, we will keep
    // the default value here as false.
    public boolean getEnableUniqueKeyMergeOnWrite() {
        return Boolean.parseBoolean(properties.getOrDefault(
                PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE, "false"));
    }

    public void setEnableMowLightDelete(boolean enable) {
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE, Boolean.toString(enable));
    }

    public boolean getEnableMowLightDelete() {
        return Boolean.parseBoolean(properties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE,
                Boolean.toString(PropertyAnalyzer.PROPERTIES_ENABLE_MOW_LIGHT_DELETE_DEFAULT_VALUE)));
    }

    public void setSequenceMapCol(String colName) {
        properties.put(PropertyAnalyzer.PROPERTIES_FUNCTION_COLUMN + "."
                + PropertyAnalyzer.PROPERTIES_SEQUENCE_COL, colName);
    }

    public String getSequenceMapCol() {
        return properties.get(PropertyAnalyzer.PROPERTIES_FUNCTION_COLUMN + "."
                + PropertyAnalyzer.PROPERTIES_SEQUENCE_COL);
    }

    public void setGroupCommitIntervalMs(int groupCommitIntervalMs) {
        properties.put(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_INTERVAL_MS, Integer.toString(groupCommitIntervalMs));
    }

    public int getGroupCommitIntervalMs() {
        return Integer.parseInt(properties.getOrDefault(
                PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_INTERVAL_MS,
                Integer.toString(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_INTERVAL_MS_DEFAULT_VALUE)));
    }

    public void setGroupCommitDataBytes(int groupCommitDataBytes) {
        properties.put(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_DATA_BYTES, Integer.toString(groupCommitDataBytes));
    }

    public int getGroupCommitDataBytes() {
        return Integer.parseInt(properties.getOrDefault(
            PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_DATA_BYTES,
            Integer.toString(PropertyAnalyzer.PROPERTIES_GROUP_COMMIT_DATA_BYTES_DEFAULT_VALUE)));
    }

    public void buildReplicaAllocation() {
        try {
            // Must copy the properties because "analyzeReplicaAllocation" will remove the property
            // from the properties.
            Map<String, String> copiedProperties = Maps.newHashMap(properties);
            this.replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocationWithoutCheck(
                    copiedProperties, "default");
        } catch (AnalysisException e) {
            // should not happen
            LOG.error("should not happen when build replica allocation", e);
            this.replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableProperty read(DataInput in) throws IOException {
        TableProperty tableProperty = GsonUtils.GSON.fromJson(Text.readString(in), TableProperty.class)
                .executeBuildDynamicProperty()
                .buildInMemory()
                .buildMinLoadReplicaNum()
                .buildStorageMedium()
                .buildStorageFormat()
                .buildInvertedIndexStorageFormat()
                .buildDataSortInfo()
                .buildCompressionType()
                .buildStoragePolicy()
                .buildIsBeingSynced()
                .buildBinlogConfig()
                .buildEnableLightSchemaChange()
                .buildStoreRowColumn()
                .buildRowStorePageSize()
                .buildSkipWriteIndexOnLoad()
                .buildCompactionPolicy()
                .buildTimeSeriesCompactionGoalSizeMbytes()
                .buildTimeSeriesCompactionFileCountThreshold()
                .buildTimeSeriesCompactionTimeThresholdSeconds()
                .buildDisableAutoCompaction()
                .buildEnableSingleReplicaCompaction()
                .buildTimeSeriesCompactionEmptyRowsetsThreshold()
                .buildTimeSeriesCompactionLevelThreshold()
                .buildInAtomicRestore();
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_105) {
            // get replica num from property map and create replica allocation
            String repNum = tableProperty.properties.remove(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM);
            if (!Strings.isNullOrEmpty(repNum)) {
                ReplicaAllocation replicaAlloc = new ReplicaAllocation(Short.valueOf(repNum));
                tableProperty.properties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                        replicaAlloc.toCreateStmt());
            } else {
                tableProperty.properties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                        ReplicaAllocation.DEFAULT_ALLOCATION.toCreateStmt());
            }
        }
        tableProperty.removeDuplicateReplicaNumProperty();
        tableProperty.buildReplicaAllocation();
        return tableProperty;
    }

    // For some historical reason,
    // both "dynamic_partition.replication_num" and "dynamic_partition.replication_allocation"
    // may be exist in "properties". we need remove the "dynamic_partition.replication_num", or it will always replace
    // the "dynamic_partition.replication_allocation",
    // result in unable to set "dynamic_partition.replication_allocation".
    private void removeDuplicateReplicaNumProperty() {
        if (properties.containsKey(DynamicPartitionProperty.REPLICATION_NUM)
                && properties.containsKey(DynamicPartitionProperty.REPLICATION_ALLOCATION)) {
            properties.remove(DynamicPartitionProperty.REPLICATION_NUM);
        }
    }

    public String getPropertiesString() {
        StringBuilder str = new StringBuilder("");
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (str.length() != 0) {
                str.append(", ");
            }
            str.append(entry.getKey() + " = " + entry.getValue());
        }
        return str.toString();
    }
}
