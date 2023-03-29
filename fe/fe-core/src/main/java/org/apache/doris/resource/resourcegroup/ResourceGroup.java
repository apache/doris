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

package org.apache.doris.resource.resourcegroup;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPipelineResourceGroup;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class ResourceGroup implements Writable {

    private static final Logger LOG = LogManager.getLogger(ResourceGroup.class);

    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    // version update required after alter resource group
    @SerializedName(value = "version")
    private long version;

    @SerializedName(value = "parentName")
    private String parentName;

    @SerializedName(value = "childrenNames")
    private Set<String> childrenNames;

    public ResourceGroup(long id, String name, Map<String, String> properties, String parentName) {
        this.id = id;
        this.name = name;
        this.properties = properties;
        this.parentName = parentName;
        this.childrenNames = Sets.newHashSet();
        this.version = 0;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getParentName() {
        return parentName;
    }

    public void getProcNodeData(BaseProcResult result) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            result.addRow(Lists.newArrayList(String.valueOf(id), name, entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public ResourceGroup clone() {
        ResourceGroup copied = DeepCopy.copy(this, ResourceGroup.class, FeConstants.meta_version);
        if (copied == null) {
            LOG.warn("failed to clone resource group: " + getName());
            return null;
        }
        return copied;
    }

    public TPipelineResourceGroup toThrift() {
        return new TPipelineResourceGroup().setId(id).setName(name).setProperties(properties).setVersion(version);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ResourceGroup read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ResourceGroup.class);
    }
}
