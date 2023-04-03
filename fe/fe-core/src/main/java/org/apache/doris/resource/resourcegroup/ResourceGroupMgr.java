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

import org.apache.doris.analysis.CreateResourceGroupStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPipelineResourceGroup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResourceGroupMgr implements Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(ResourceGroupMgr.class);

    public static final String DEFAULT_GROUP_NAME = "normal";

    private static final String CPU_SCHEDULING_WEIGHT = "cpu_scheduling_weight";

    private static final String CONCURRENCY_LIMIT = "concurrency_limit";

    public static final ImmutableList<String> RESOURCE_GROUP_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Id").add("Name").add("Item").add("Value")
            .build();

    private static final ImmutableSet<String> REQUIRED_PROPERTIES_NAME = new ImmutableSet.Builder<String>().add(
            CPU_SCHEDULING_WEIGHT).build();

    private static final ImmutableSet<String> ALL_PROPERTIES_NAME = new ImmutableSet.Builder<String>().add(
            CPU_SCHEDULING_WEIGHT).add(CONCURRENCY_LIMIT).build();

    @SerializedName(value = "idToResourceGroup")
    private final Map<Long, ResourceGroup> idToResourceGroup = Maps.newHashMap();

    private final Map<String, ResourceGroup> nameToResourceGroup = Maps.newHashMap();

    private final ResourceProcNode procNode = new ResourceProcNode();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public ResourceGroupMgr() {
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void init() {
        checkAndCreateDefaultGroup();
    }

    public List<TPipelineResourceGroup> getResourceGroup(String groupName) throws UserException {
        List<TPipelineResourceGroup> resourceGroups = Lists.newArrayList();
        readLock();
        try {
            ResourceGroup resourceGroup = nameToResourceGroup.get(groupName);
            if (resourceGroup == null) {
                throw new UserException("Resource group " + groupName + " does not exist");
            }
            // need to check resource group privs
            resourceGroups.add(resourceGroup.toThrift());
            while (idToResourceGroup.containsKey(resourceGroup.getParentId())) {
                resourceGroup = idToResourceGroup.get(resourceGroup.getParentId());
                resourceGroups.add(resourceGroup.toThrift());
            }
        } finally {
            readUnlock();
        }
        return resourceGroups;
    }

    private void checkAndCreateDefaultGroup() {
        ResourceGroup defaultResourceGroup;
        writeLock();
        try {
            if (nameToResourceGroup.containsKey(DEFAULT_GROUP_NAME)) {
                return;
            }
            Map<String, String> properties = Maps.newHashMap();
            properties.put(CPU_SCHEDULING_WEIGHT, "10");
            defaultResourceGroup = new ResourceGroup(Env.getCurrentEnv().getNextId(),
                    DEFAULT_GROUP_NAME, properties, -1);
            nameToResourceGroup.put(DEFAULT_GROUP_NAME, defaultResourceGroup);
            idToResourceGroup.put(defaultResourceGroup.getId(), defaultResourceGroup);
            Env.getCurrentEnv().getEditLog().logCreateResourceGroup(defaultResourceGroup);
        } finally {
            writeUnlock();
        }
        LOG.info("Create resource group success: {}", defaultResourceGroup);
    }

    public void createResourceGroup(CreateResourceGroupStmt stmt) throws DdlException {
        checkProperties(stmt.getProperties());
        ResourceGroup resourceGroup = new ResourceGroup(Env.getCurrentEnv().getNextId(),
                stmt.getResourceGroupName(),
                stmt.getProperties(), -1);
        String resourceGroupNameName = resourceGroup.getName();
        writeLock();
        try {
            if (nameToResourceGroup.putIfAbsent(resourceGroupNameName, resourceGroup) != null) {
                if (stmt.isIfNotExists()) {
                    return;
                }
                throw new DdlException("Resource group(" + resourceGroupNameName + ") already exist");
            }
            idToResourceGroup.put(resourceGroup.getId(), resourceGroup);
            Env.getCurrentEnv().getEditLog().logCreateResourceGroup(resourceGroup);
        } finally {
            writeUnlock();
        }
        LOG.info("Create resource group success: {}", resourceGroup);
    }

    private void checkProperties(Map<String, String> properties) throws DdlException {
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

        String cpuSchedulingWeight = properties.get(CPU_SCHEDULING_WEIGHT);
        if (!StringUtils.isNumeric(cpuSchedulingWeight) || Long.parseLong(cpuSchedulingWeight) <= 0) {
            throw new DdlException("cpu_scheduling_weight requires a positive integer.");
        }
        String concurrencyLimit = properties.get(CONCURRENCY_LIMIT);
        if (concurrencyLimit != null && (!StringUtils.isNumeric(concurrencyLimit)
                || Long.parseLong(concurrencyLimit) < 0)) {
            throw new DdlException("concurrency_limit requires a non-negative integers.");
        }
    }

    public void replayCreateResourceGroup(ResourceGroup resourceGroup) {
        writeLock();
        try {
            nameToResourceGroup.put(resourceGroup.getName(), resourceGroup);
            idToResourceGroup.put(resourceGroup.getId(), resourceGroup);
        } finally {
            writeUnlock();
        }
    }

    public List<List<String>> getResourcesInfo() {
        return procNode.fetchResult().getRows();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ResourceGroupMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ResourceGroupMgr.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        idToResourceGroup.forEach(
                (id, resourceGroup) -> nameToResourceGroup.put(resourceGroup.getName(), resourceGroup));
    }

    public class ResourceProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(RESOURCE_GROUP_PROC_NODE_TITLE_NAMES);
            readLock();
            try {
                for (ResourceGroup resourceGroup : idToResourceGroup.values()) {
                    // need to check resource group privs
                    resourceGroup.getProcNodeData(result);
                }
            } finally {
                readUnlock();
            }
            return result;
        }
    }
}
