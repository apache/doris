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

package org.apache.doris.analysis;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.base.Strings;

import java.util.Map;

// clause which is used to modify table properties
public class ModifyTablePropertiesClause extends AlterTableClause {

    private Map<String, String> properties;

    public String getStoragePolicy() {
        return this.storagePolicy;
    }

    public void setStoragePolicy(String storagePolicy) {
        this.storagePolicy = storagePolicy;
    }

    private String storagePolicy;

    private boolean isBeingSynced = false;

    public void setIsBeingSynced(boolean isBeingSynced) {
        this.isBeingSynced = isBeingSynced;
    }

    public boolean isBeingSynced() {
        return isBeingSynced;
    }

    private String compactionPolicy;

    private long timeSeriesCompactionGoalSizeMbytes;

    private long timeSeriesCompactionFileCountThreshold;

    private long timeSeriesCompactionTimeThresholdSeconds;

    public void setCompactionPolicy(String compactionPolicy) {
        this.compactionPolicy = compactionPolicy;
    }

    public String compactionPolicy() {
        return compactionPolicy;
    }

    public void setTimeSeriesCompactionGoalSizeMbytes(long timeSeriesCompactionGoalSizeMbytes) {
        this.timeSeriesCompactionGoalSizeMbytes = timeSeriesCompactionGoalSizeMbytes;
    }

    public long timeSeriesCompactionGoalSizeMbytes() {
        return timeSeriesCompactionGoalSizeMbytes;
    }

    public void setTimeSeriesCompactionFileCountThreshold(long timeSeriesCompactionFileCountThreshold) {
        this.timeSeriesCompactionFileCountThreshold = timeSeriesCompactionFileCountThreshold;
    }

    public Long timeSeriesCompactionFileCountThreshold() {
        return timeSeriesCompactionFileCountThreshold;
    }

    public void setTimeSeriesCompactionTimeThresholdSeconds(long timeSeriesCompactionTimeThresholdSeconds) {
        this.timeSeriesCompactionTimeThresholdSeconds = timeSeriesCompactionTimeThresholdSeconds;
    }

    public Long timeSeriesCompactionTimeThresholdSeconds() {
        return timeSeriesCompactionTimeThresholdSeconds;
    }

    public ModifyTablePropertiesClause(Map<String, String> properties) {
        super(AlterOpType.MODIFY_TABLE_PROPERTY);
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Properties is not set");
        }

        if (properties.size() != 1
                && !TableProperty.isSamePrefixProperties(properties, TableProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)
                && !TableProperty.isSamePrefixProperties(properties, PropertyAnalyzer.PROPERTIES_BINLOG_PREFIX)) {
            throw new AnalysisException(
                    "Can only set one table property(without dynamic partition && binlog) at a time");
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
            this.needTableStable = false;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE).equalsIgnoreCase("column")) {
                throw new AnalysisException("Can only change storage type to COLUMN");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE).equalsIgnoreCase("random")) {
                throw new AnalysisException("Can only change distribution type from HASH to RANDOM");
            }
            this.needTableStable = false;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK).equalsIgnoreCase("true")) {
                throw new AnalysisException(
                        "Property " + PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK + " should be set to true");
            }
            this.needTableStable = false;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_FPP)) {
            // do nothing, these 2 properties will be analyzed when creating alter job
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT).equalsIgnoreCase("v2")) {
                throw new AnalysisException(
                        "Property " + PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT + " should be v2");
            }
        } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
            // do nothing, dynamic properties will be analyzed in SchemaChangeHandler.process
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION)) {
            ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, replicaAlloc.toCreateStmt());
        } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)
                || properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION)) {
            ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "default");
            properties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION,
                    replicaAlloc.toCreateStmt());
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            boolean isInMemory = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_INMEMORY));
            if (isInMemory == true) {
                throw new AnalysisException("Not support set 'in_memory'='true' now!");
            }
            this.needTableStable = false;
            this.opType = AlterOpType.MODIFY_TABLE_PROPERTY_SYNC;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TABLET_TYPE)) {
            throw new AnalysisException("Alter tablet type not supported");
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY)) {
            this.needTableStable = false;
            String storagePolicy = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_POLICY, "");
            if (!Strings.isNullOrEmpty(storagePolicy)
                    && properties.containsKey(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE)) {
                throw new AnalysisException(
                        "Can not set UNIQUE KEY table that enables Merge-On-write"
                                + " with storage policy(" + storagePolicy + ")");
            }
            setStoragePolicy(storagePolicy);
        } else if (properties.containsKey(PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE)) {
            throw new AnalysisException("Can not change UNIQUE KEY to Merge-On-Write mode");
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_DUPLICATE_WITHOUT_KEYS_BY_DEFAULT)) {
            throw new AnalysisException("Can not change enable_duplicate_without_keys_by_default");
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_ENABLE_LIGHT_SCHEMA_CHANGE)) {
            // do nothing, will be alter in SchemaChangeHandler.updateTableProperties
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED)) {
            this.needTableStable = false;
            setIsBeingSynced(Boolean.parseBoolean(properties.getOrDefault(
                    PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED, "false")));
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS)) {
            // do nothing, will be alter in SchemaChangeHandler.updateBinlogConfig
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY)) {
            String compactionPolicy = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_COMPACTION_POLICY, "");
            if (compactionPolicy != null
                                    && !compactionPolicy.equals(PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY)
                                    && !compactionPolicy.equals(PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY)) {
                throw new AnalysisException(
                        "Table compaction policy only support for " + PropertyAnalyzer.TIME_SERIES_COMPACTION_POLICY
                        + " or " + PropertyAnalyzer.SIZE_BASED_COMPACTION_POLICY);
            }
            this.needTableStable = false;
            setCompactionPolicy(compactionPolicy);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)) {
            long goalSizeMbytes;
            String goalSizeMbytesStr = properties
                                        .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES);
            try {
                goalSizeMbytes = Long.parseLong(goalSizeMbytesStr);
                if (goalSizeMbytes < 10) {
                    throw new AnalysisException("time_series_compaction_goal_size_mbytes can not be less than 10:"
                        + goalSizeMbytesStr);
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid time_series_compaction_goal_size_mbytes format: "
                        + goalSizeMbytesStr);
            }
            this.needTableStable = false;
            setTimeSeriesCompactionGoalSizeMbytes(goalSizeMbytes);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)) {
            long fileCountThreshold;
            String fileCountThresholdStr = properties
                                        .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD);
            try {
                fileCountThreshold = Long.parseLong(fileCountThresholdStr);
                if (fileCountThreshold < 10) {
                    throw new AnalysisException("time_series_compaction_file_count_threshold can not be less than 10:"
                                                                                        + fileCountThresholdStr);
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid time_series_compaction_file_count_threshold format: "
                                                                                + fileCountThresholdStr);
            }
            this.needTableStable = false;
            setTimeSeriesCompactionFileCountThreshold(fileCountThreshold);
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)) {
            long timeThresholdSeconds;
            String timeThresholdSecondsStr = properties
                                    .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS);
            try {
                timeThresholdSeconds = Long.parseLong(timeThresholdSecondsStr);
                if (timeThresholdSeconds < 60) {
                    throw new AnalysisException("time_series_compaction_time_threshold_seconds can not be less than 60:"
                                                                                        + timeThresholdSecondsStr);
                }
            } catch (NumberFormatException e) {
                throw new AnalysisException("Invalid time_series_compaction_time_threshold_seconds format: "
                                                                                        + timeThresholdSecondsStr);
            }
            this.needTableStable = false;
            setTimeSeriesCompactionTimeThresholdSeconds(timeThresholdSeconds);
        } else {
            throw new AnalysisException("Unknown table property: " + properties.keySet());
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PROPERTIES (");
        sb.append(new PrintableMap<String, String>(properties, "=", true, false));
        sb.append(")");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
