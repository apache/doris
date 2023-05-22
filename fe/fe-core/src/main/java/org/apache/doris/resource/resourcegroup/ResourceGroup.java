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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPipelineResourceGroup;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ResourceGroup implements Writable {
    private static final Logger LOG = LogManager.getLogger(ResourceGroup.class);

    public static final String CPU_SHARE = "cpu_share";

    public static final String MEMORY_LIMIT = "memory_limit";

    public static final String ENABLE_MEMORY_OVERCOMMIT = "enable_memory_overcommit";

    private static final ImmutableSet<String> REQUIRED_PROPERTIES_NAME = new ImmutableSet.Builder<String>().add(
            CPU_SHARE).add(MEMORY_LIMIT).build();

    private static final ImmutableSet<String> ALL_PROPERTIES_NAME = new ImmutableSet.Builder<String>().add(
            CPU_SHARE).add(MEMORY_LIMIT).add(ENABLE_MEMORY_OVERCOMMIT).build();

    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    // Version update required after alter resource group
    @SerializedName(value = "version")
    private long version;

    private double memoryLimitPercent;

    private ResourceGroup(long id, String name, Map<String, String> properties) {
        this(id, name, properties, 0);
    }

    private ResourceGroup(long id, String name, Map<String, String> properties, long version) {
        this.id = id;
        this.name = name;
        this.properties = properties;
        this.version = version;
        String memoryLimitString = properties.get(MEMORY_LIMIT);
        this.memoryLimitPercent = Double.parseDouble(memoryLimitString.substring(0, memoryLimitString.length() - 1));
        if (properties.containsKey(ENABLE_MEMORY_OVERCOMMIT)) {
            properties.put(ENABLE_MEMORY_OVERCOMMIT, properties.get(ENABLE_MEMORY_OVERCOMMIT).toLowerCase());
        }
    }

    public static ResourceGroup create(String name, Map<String, String> properties) throws DdlException {
        checkProperties(properties);
        return new ResourceGroup(Env.getCurrentEnv().getNextId(), name, properties);
    }

    public static ResourceGroup copyAndUpdate(ResourceGroup resourceGroup, Map<String, String> updateProperties)
            throws DdlException {
        Map<String, String> newProperties = new HashMap<>(resourceGroup.getProperties());
        for (Map.Entry<String, String> kv : updateProperties.entrySet()) {
            if (!Strings.isNullOrEmpty(kv.getValue())) {
                newProperties.put(kv.getKey(), kv.getValue());
            }
        }

        checkProperties(newProperties);
        return new ResourceGroup(
           resourceGroup.getId(), resourceGroup.getName(), newProperties, resourceGroup.getVersion() + 1);
    }

    private static void checkProperties(Map<String, String> properties) throws DdlException {
        for (String propertyName : properties.keySet()) {
            if (!ALL_PROPERTIES_NAME.contains(propertyName)) {
                throw new DdlException("Property " + propertyName + " is not supported.");
            }
        }
        for (String propertyName : REQUIRED_PROPERTIES_NAME) {
            if (!properties.containsKey(propertyName)) {
                throw new DdlException("Property " + propertyName + " is required.");
            }
        }

        String cpuSchedulingWeight = properties.get(CPU_SHARE);
        if (!StringUtils.isNumeric(cpuSchedulingWeight) || Long.parseLong(cpuSchedulingWeight) <= 0) {
            throw new DdlException(CPU_SHARE + " " + cpuSchedulingWeight + " requires a positive integer.");
        }

        String memoryLimit = properties.get(MEMORY_LIMIT);
        if (!memoryLimit.endsWith("%")) {
            throw new DdlException(MEMORY_LIMIT + " " + memoryLimit + " requires a percentage and ends with a '%'");
        }
        String memLimitErr = MEMORY_LIMIT + " " + memoryLimit + " requires a positive floating point number.";
        try {
            if (Double.parseDouble(memoryLimit.substring(0, memoryLimit.length() - 1)) <= 0) {
                throw new DdlException(memLimitErr);
            }
        } catch (NumberFormatException  e) {
            LOG.debug(memLimitErr, e);
            throw new DdlException(memLimitErr);
        }

        if (properties.containsKey(ENABLE_MEMORY_OVERCOMMIT)) {
            String value = properties.get(ENABLE_MEMORY_OVERCOMMIT).toLowerCase();
            if (!("true".equals(value) || "false".equals(value))) {
                throw new DdlException("The value of '" + ENABLE_MEMORY_OVERCOMMIT + "' must be true or false.");
            }
        }
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

    private long getVersion() {
        return version;
    }

    public double getMemoryLimitPercent() {
        return memoryLimitPercent;
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
