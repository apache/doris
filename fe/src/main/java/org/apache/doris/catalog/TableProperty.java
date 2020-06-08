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

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageFormat;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**  TableProperty contains additional information about OlapTable
 *  TableProperty includes properties to persistent the additional information
 *  Different properties is recognized by prefix such as dynamic_partition
 *  If there is different type properties is added.Write a method such as buildDynamicProperty to build it.
 */
public class TableProperty implements Writable {
    public static final String DYNAMIC_PARTITION_PROPERTY_PREFIX = "dynamic_partition";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    private DynamicPartitionProperty dynamicPartitionProperty = new DynamicPartitionProperty(Maps.newHashMap());
    // table's default replication num
    private Short replicationNum = FeConstants.default_replication_num;

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
                buildDynamicProperty();
                break;
            case OperationType.OP_MODIFY_REPLICATION_NUM:
                buildReplicationNum();
                break;
            case OperationType.OP_MODIFY_IN_MEMORY:
                buildInMemory();
                break;
            default:
                break;
        }
        return this;
    }
    public TableProperty buildDynamicProperty() {
        HashMap<String, String> dynamicPartitionProperties = new HashMap<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
                dynamicPartitionProperties.put(entry.getKey(), entry.getValue());
            }
        }
        dynamicPartitionProperty = new DynamicPartitionProperty(dynamicPartitionProperties);
        return this;
    }

    public TableProperty buildReplicationNum() {
        replicationNum = Short.parseShort(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM,
                String.valueOf(FeConstants.default_replication_num)));
        return this;
    }

    public TableProperty buildInMemory() {
        isInMemory = Boolean.parseBoolean(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_INMEMORY, "false"));
        return this;
    }

    public TableProperty buildStorageFormat() {
        storageFormat = TStorageFormat.valueOf(properties.getOrDefault(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT,
                TStorageFormat.DEFAULT.name()));
        return this;
    }

    public void modifyTableProperties(Map<String, String> modifyProperties) {
        properties.putAll(modifyProperties);
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

    public Short getReplicationNum() {
        return replicationNum;
    }

    public boolean IsInMemory() {
        return isInMemory;
    }

    public TStorageFormat getStorageFormat() {
        return storageFormat;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static TableProperty read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), TableProperty.class)
                .buildDynamicProperty()
                .buildReplicationNum()
                .buildInMemory()
                .buildStorageFormat();
    }
}
