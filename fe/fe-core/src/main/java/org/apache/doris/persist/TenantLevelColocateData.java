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

package org.apache.doris.persist;

import org.apache.doris.catalog.TenantLevelColocateGroupSchema;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.resource.Tag;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PersistInfo for ColocateSlaveIndex
 */
public class TenantLevelColocateData implements Writable, GsonPostProcessable {

    @SerializedName(value = "groupSchemaList")
    private List<TenantLevelColocateGroupSchema> groupSchemaList;

    @SerializedName(value = "groupMapData")
    private Map<Long, List<List<Long>>> groupMapData;

    @SerializedName(value = "tableToMasterGroupMap")
    private Map<Long, Map<Tag, Long>> tableToMasterGroupMap;
    @SerializedName(value = "unstableMasterGroup")
    private Set<Long> unstableMasterGroup;

    @SerializedName(value = "tableToSlaveGroupMap")
    private Map<Long, Map<Tag, Long>> tableToSlaveGroupMap;
    @SerializedName(value = "unstableSlaveGroup")
    private Set<Long> unstableSlaveGroup;

    public static TenantLevelColocateData read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, TenantLevelColocateData.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public List<TenantLevelColocateGroupSchema> getGroupSchemaList() {
        return groupSchemaList;
    }

    public void setGroupSchemaList(List<TenantLevelColocateGroupSchema> groupSchemaList) {
        this.groupSchemaList = groupSchemaList;
    }

    public Map<Long, Map<Tag, Long>> getTableToMasterGroupMap() {
        return tableToMasterGroupMap;
    }

    public void setTableToMasterGroupMap(Map<Long, Map<Tag, Long>> tableToMasterGroupMap) {
        this.tableToMasterGroupMap = tableToMasterGroupMap;
    }

    public Set<Long> getUnstableMasterGroup() {
        return unstableMasterGroup;
    }

    public void setUnstableMasterGroup(Set<Long> unstableMasterGroup) {
        this.unstableMasterGroup = unstableMasterGroup;
    }

    public Map<Long, Map<Tag, Long>> getTableToSlaveGroupMap() {
        return tableToSlaveGroupMap;
    }

    public void setTableToSlaveGroupMap(Map<Long, Map<Tag, Long>> tableToSlaveGroupMap) {
        this.tableToSlaveGroupMap = tableToSlaveGroupMap;
    }

    public Set<Long> getUnstableSlaveGroup() {
        return unstableSlaveGroup;
    }

    public void setUnstableSlaveGroup(Set<Long> unstableSlaveGroup) {
        this.unstableSlaveGroup = unstableSlaveGroup;
    }

    public Map<Long, List<List<Long>>> getGroupMapData() {
        return groupMapData;
    }

    public void setGroupMapData(Map<Long, List<List<Long>>> groupMapData) {
        this.groupMapData = groupMapData;
    }

    @Override
    public String toString() {
        return "{" + "groupMapData=" + groupMapData + ", tableToMasterGroupMap=" + tableToMasterGroupMap
                + ", unstableMasterGroup=" + unstableMasterGroup + ", tableToSlaveGroupMap=" + tableToSlaveGroupMap
                + ", unstableSlaveGroup=" + unstableSlaveGroup + '}';
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (groupSchemaList == null) {
            groupSchemaList = new ArrayList<>();
        }
        if (tableToMasterGroupMap == null) {
            tableToMasterGroupMap = new HashMap<>();
        }
        if (unstableMasterGroup == null) {
            unstableMasterGroup = new HashSet<>();
        }
        if (tableToSlaveGroupMap == null) {
            tableToSlaveGroupMap = new HashMap<>();
        }
        if (unstableSlaveGroup == null) {
            unstableSlaveGroup = new HashSet<>();
        }
        if (groupMapData == null) {
            groupMapData = new HashMap<>();
        }
    }
}
