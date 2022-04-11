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
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.common.AnalysisException;
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
        ODBC_CATALOG,
        S3;

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
        Resource resource = getResourceInstance(stmt.getResourceType(), stmt.getResourceName());
        resource.setProperties(stmt.getProperties());

        return resource;
    }

    /**
     * Get resource instance by resource name and type
     * @param type
     * @param name
     * @return
     * @throws DdlException
     */
    private static Resource getResourceInstance(ResourceType type, String name) throws DdlException {
        Resource resource;
        switch (type) {
            case SPARK:
                resource = new SparkResource(name);
                break;
            case ODBC_CATALOG:
                resource = new OdbcCatalogResource(name);
                break;
            case S3:
                resource = new S3Resource(name);
                break;
            default:
                throw new DdlException("Unknown resource type: " + type);
        }

        return resource;
    }

    public String getName() {
        return name;
    }

    public ResourceType getType() {
        return type;
    }

    /**
     * Modify properties in child resources
     * @param properties
     * @throws DdlException
     */
    public abstract void modifyProperties(Map<String, String> properties) throws DdlException;

    /**
     * Check properties in child resources
     * @param properties
     * @throws AnalysisException
     */
    public abstract void checkProperties(Map<String, String> properties) throws AnalysisException;

    protected void replaceIfEffectiveValue(Map<String, String> properties, String key, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            properties.put(key, value);
        }
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

