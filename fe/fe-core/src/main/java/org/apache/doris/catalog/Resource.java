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

import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public abstract class Resource implements Writable {
    private static final Logger LOG = LogManager.getLogger(OdbcCatalogResource.class);

    public enum ResourceType {
        UNKNOWN,
        SPARK,
        ODBC_CATALOG;

        public static ResourceType fromString(String resourceType) {
            for (ResourceType type : ResourceType.values()) {
                if (type.name().equalsIgnoreCase(resourceType)) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }

    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected ResourceType type;

    public Resource() {
    }

    public Resource(String name, ResourceType type) {
        this.name = name;
        this.type = type;
    }

    public static Resource fromStmt(CreateResourceStmt stmt) throws DdlException {
        Resource resource = null;
        ResourceType type = stmt.getResourceType();
        switch (type) {
            case SPARK:
                resource = new SparkResource(stmt.getResourceName());
                break;
            case ODBC_CATALOG:
                resource = new OdbcCatalogResource(stmt.getResourceName());
                break;
            default:
                throw new DdlException("Only support Spark resource.");
        }

        resource.setProperties(stmt.getProperties());
        return resource;
    }

    public String getName() {
        return name;
    }

    public ResourceType getType() {
        return type;
    }

    /**
     * Set and check the properties in child resources
     */
    protected abstract void setProperties(Map<String, String> properties) throws DdlException;

    /**
     * Fill BaseProcResult with different properties in child resources
     * ResourceMgr.RESOURCE_PROC_NODE_TITLE_NAMES format:
     * | Name | ResourceType | Key | Value |
     */
    protected abstract void getProcNodeData(BaseProcResult result);

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Resource read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Resource.class);
    }

    @Override
    public Resource clone() {
        Resource copied = DeepCopy.copy(this, Resource.class, FeConstants.meta_version);
        if (copied == null) {
            LOG.warn("failed to clone odbc resource: " + getName());
            return null;
        }
        return copied;
    }
}

