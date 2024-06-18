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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// only the following 2 formats are allowed
// *
// resource
public class ResourcePattern implements Writable, GsonPostProcessable {
    @SerializedName(value = "resourceName")
    private String resourceName;

    // just for cloud
    // GRANT USAGE_PRIV ON CLUSTER '${clusterName}' TO '${userName}';
    @SerializedName(value = "resourceType")
    private ResourceTypeEnum resourceType;

    public static ResourcePattern ALL_GENERAL;
    public static ResourcePattern ALL_CLUSTER;
    public static ResourcePattern ALL_STAGE;
    public static ResourcePattern ALL_STORAGE_VAULT;

    static {
        ALL_GENERAL = new ResourcePattern("%", ResourceTypeEnum.GENERAL);
        ALL_CLUSTER = new ResourcePattern("%", ResourceTypeEnum.CLUSTER);
        ALL_STAGE = new ResourcePattern("%", ResourceTypeEnum.STAGE);
        ALL_STORAGE_VAULT = new ResourcePattern("%", ResourceTypeEnum.STORAGE_VAULT);

        try {
            ALL_GENERAL.analyze();
            ALL_CLUSTER.analyze();
            ALL_STAGE.analyze();
            ALL_STORAGE_VAULT.analyze();
        } catch (AnalysisException e) {
            // will not happen
        }
    }

    public ResourcePattern(String resourceName, ResourceTypeEnum type) {
        // To be compatible with previous syntax
        if ("*".equals(resourceName)) {
            resourceName = "%";
        }
        this.resourceName = Strings.isNullOrEmpty(resourceName) ? "%" : resourceName;
        resourceType = type;
    }

    public void setResourceType(ResourceTypeEnum type) {
        resourceType = type;
    }

    public ResourceTypeEnum getResourceType() {
        return resourceType;
    }

    public String getResourceName() {
        return resourceName;
    }

    public PrivLevel getPrivLevel() {
        return PrivLevel.RESOURCE;
    }

    public void analyze() throws AnalysisException {
        if (!resourceName.equals("%")) {
            FeNameFormat.checkResourceName(resourceName, resourceType);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ResourcePattern)) {
            return false;
        }
        ResourcePattern other = (ResourcePattern) obj;
        return resourceName.equals(other.getResourceName()) && resourceType.equals(other.resourceType);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + resourceName.hashCode() + resourceType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return resourceName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ResourcePattern read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ResourcePattern.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // // To be compatible with previous syntax
        if ("*".equals(resourceName)) {
            resourceName = "%";
        }
    }
}
