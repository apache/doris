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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageFormat;

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

/**  TableProperty contains additional information about OlapTable
 *  TableProperty includes properties to persistent the additional information
 *  Different properties is recognized by prefix such as dynamic_partition
 *  If there is different type properties is added, write a method such as buildDynamicProperty to build it.
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

    /*
     * the default storage format of this table.
     * DEFAULT: depends on BE's config 'default_rowset_type'
     * V1: alpha rowset
     * V2: beta rowset
     *
     * This property should be set when creating the table, and can only be changed to V2 using Alter Table stmt.
     */
    private TStorageFormat storageFormat = TStorageFormat.DEFAULT;

    private DataSortInfo dataSortInfo = new DataSortInfo();

    // remote storage resource, for cold data
    private String remoteStorageResource;

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
                break;
            default:
                break;
        }
        return this;
    }

    /**
     * Reset properties to correct values.
     * @return this for chained
     */
    public TableProperty resetPropertiesForRestore() {
        if (properties.containsKey(DynamicPartitionProperty.ENABLE)) {
            properties.put(DynamicPartitionProperty.ENABLE, "false");
            executeBuildDynamicProperty();
        }
        return this;
    }

    public TableProperty buildDynamicProperty() throws DdlException {
        if (properties.containsKey(DynamicPartitionProperty.ENABLE)
                && Boolean.valueOf(properties.get(DynamicPartitionProperty.ENABLE))
                && !Config.dynamic_partition_enable) {
            throw new DdlException("Could not create table with dynamic partition "
                    + "when fe config dynamic_partition_enable is false. "
                    + "Please ADMIN SET FRONTEND CONFIG (\"dynamic_partition_enable\" = \"true\") firstly.");
        }
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

    public TableProperty buildStorageFormat() {
        storageFormat = TStorageFormat.valueOf(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT,
                TStorageFormat.DEFAULT.name()));
        return this;
    }

    public TableProperty buildRemoteStorageResource() {
        remoteStorageResource = properties.getOrDefault(PropertyAnalyzer.PROPERTIES_REMOTE_STORAGE_RESOURCE, "");
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

    public void setRemoteStorageResource(String resourceName) {
        this.remoteStorageResource = resourceName;
        properties.put(PropertyAnalyzer.PROPERTIES_REMOTE_STORAGE_RESOURCE,
                resourceName);
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

    public TStorageFormat getStorageFormat() {
        return storageFormat;
    }

    public DataSortInfo getDataSortInfo() {
        return dataSortInfo;
    }

    public String getRemoteStorageResource() {
        return remoteStorageResource;
    }

    public void buildReplicaAllocation() {
        try {
            // Must copy the properties because "analyzeReplicaAllocation" with remove the property
            // from the properties.
            Map<String, String> copiedProperties = Maps.newHashMap(properties);
            this.replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(copiedProperties, "default");
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
                .buildStorageFormat()
                .buildDataSortInfo()
                .buildRemoteStorageResource();
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_105) {
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

    // For some historical reason, both "dynamic_partition.replication_num" and "dynamic_partition.replication_allocation"
    // may be exist in "properties". we need remove the "dynamic_partition.replication_num", or it will always replace
    // the "dynamic_partition.replication_allocation", result in unable to set "dynamic_partition.replication_allocation".
    private void removeDuplicateReplicaNumProperty() {
        if (properties.containsKey(DynamicPartitionProperty.REPLICATION_NUM)
                && properties.containsKey(DynamicPartitionProperty.REPLICATION_ALLOCATION)) {
            properties.remove(DynamicPartitionProperty.REPLICATION_NUM);
        }
    }
}
