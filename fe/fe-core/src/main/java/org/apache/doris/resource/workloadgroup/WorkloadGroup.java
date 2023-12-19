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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TWorkloadGroupInfo;
import org.apache.doris.thrift.TopicInfo;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WorkloadGroup implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(WorkloadGroup.class);

    public static final String CPU_SHARE = "cpu_share";

    public static final String CPU_HARD_LIMIT = "cpu_hard_limit";

    public static final String MEMORY_LIMIT = "memory_limit";

    public static final String ENABLE_MEMORY_OVERCOMMIT = "enable_memory_overcommit";

    public static final String MAX_CONCURRENCY = "max_concurrency";

    public static final String MAX_QUEUE_SIZE = "max_queue_size";

    public static final String QUEUE_TIMEOUT = "queue_timeout";

    // NOTE(wb): all property is not required, some properties default value is set in be
    // default value is as followed
    // cpu_share=1024, memory_limit=0%(0 means not limit), enable_memory_overcommit=true
    private static final ImmutableSet<String> ALL_PROPERTIES_NAME = new ImmutableSet.Builder<String>()
            .add(CPU_SHARE).add(MEMORY_LIMIT).add(ENABLE_MEMORY_OVERCOMMIT).add(MAX_CONCURRENCY)
            .add(MAX_QUEUE_SIZE).add(QUEUE_TIMEOUT).add(CPU_HARD_LIMIT).build();

    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    // Version update required after alter resource group
    @SerializedName(value = "version")
    private long version;

    private double memoryLimitPercent = 0;
    private int maxConcurrency = Integer.MAX_VALUE;
    private int maxQueueSize = 0;
    private int queueTimeout = 0;

    private int cpuHardLimit = 0;

    private WorkloadGroup(long id, String name, Map<String, String> properties) {
        this(id, name, properties, 0);
    }

    private WorkloadGroup(long id, String name, Map<String, String> properties, long version) {
        this.id = id;
        this.name = name;
        this.properties = properties;
        this.version = version;
        if (properties.containsKey(MEMORY_LIMIT)) {
            String memoryLimitString = properties.get(MEMORY_LIMIT);
            this.memoryLimitPercent = Double.parseDouble(
                    memoryLimitString.substring(0, memoryLimitString.length() - 1));
        }
        if (properties.containsKey(ENABLE_MEMORY_OVERCOMMIT)) {
            properties.put(ENABLE_MEMORY_OVERCOMMIT, properties.get(ENABLE_MEMORY_OVERCOMMIT).toLowerCase());
        }
        if (properties.containsKey(CPU_HARD_LIMIT)) {
            String cpuHardLimitStr = properties.get(CPU_HARD_LIMIT);
            if (cpuHardLimitStr.endsWith("%")) {
                cpuHardLimitStr = cpuHardLimitStr.substring(0, cpuHardLimitStr.length() - 1);
            }
            this.cpuHardLimit = Integer.parseInt(cpuHardLimitStr);
            this.properties.put(CPU_HARD_LIMIT, cpuHardLimitStr);
        }
        resetQueueProperty(properties);
    }

    private void resetQueueProperty(Map<String, String> properties) {
        if (properties.containsKey(MAX_CONCURRENCY)) {
            this.maxConcurrency = Integer.parseInt(properties.get(MAX_CONCURRENCY));
        } else {
            this.maxConcurrency = Integer.MAX_VALUE;
            properties.put(MAX_CONCURRENCY, String.valueOf(this.maxConcurrency));
        }
        if (properties.containsKey(MAX_QUEUE_SIZE)) {
            this.maxQueueSize = Integer.parseInt(properties.get(MAX_QUEUE_SIZE));
        } else {
            this.maxQueueSize = 0;
            properties.put(MAX_QUEUE_SIZE, String.valueOf(maxQueueSize));
        }
        if (properties.containsKey(QUEUE_TIMEOUT)) {
            this.queueTimeout = Integer.parseInt(properties.get(QUEUE_TIMEOUT));
        } else {
            this.queueTimeout = 0;
            properties.put(QUEUE_TIMEOUT, String.valueOf(queueTimeout));
        }
    }

    // new resource group
    public static WorkloadGroup create(String name, Map<String, String> properties) throws DdlException {
        checkProperties(properties);
        WorkloadGroup newWorkloadGroup = new WorkloadGroup(Env.getCurrentEnv().getNextId(), name, properties);
        return newWorkloadGroup;
    }

    // alter resource group
    public static WorkloadGroup copyAndUpdate(WorkloadGroup currentWorkloadGroup, Map<String, String> updateProperties)
            throws DdlException {
        Map<String, String> newProperties = new HashMap<>(currentWorkloadGroup.getProperties());
        for (Map.Entry<String, String> kv : updateProperties.entrySet()) {
            if (!Strings.isNullOrEmpty(kv.getValue())) {
                newProperties.put(kv.getKey(), kv.getValue());
            }
        }

        checkProperties(newProperties);
        WorkloadGroup newWorkloadGroup = new WorkloadGroup(
                currentWorkloadGroup.getId(), currentWorkloadGroup.getName(),
                newProperties, currentWorkloadGroup.getVersion() + 1);
        return newWorkloadGroup;
    }

    private static void checkProperties(Map<String, String> properties) throws DdlException {
        for (String propertyName : properties.keySet()) {
            if (!ALL_PROPERTIES_NAME.contains(propertyName)) {
                throw new DdlException("Property " + propertyName + " is not supported.");
            }
        }

        if (properties.containsKey(CPU_SHARE)) {
            String cpuShare = properties.get(CPU_SHARE);
            if (!StringUtils.isNumeric(cpuShare) || Long.parseLong(cpuShare) <= 0) {
                throw new DdlException(CPU_SHARE + " " + cpuShare + " requires a positive integer.");
            }
        }

        if (properties.containsKey(CPU_HARD_LIMIT)) {
            String cpuHardLimit = properties.get(CPU_HARD_LIMIT);
            if (cpuHardLimit.endsWith("%")) {
                cpuHardLimit = cpuHardLimit.substring(0, cpuHardLimit.length() - 1);
            }
            if (!StringUtils.isNumeric(cpuHardLimit) || Long.parseLong(cpuHardLimit) <= 0) {
                throw new DdlException(CPU_HARD_LIMIT + " " + cpuHardLimit + " requires a positive integer.");
            }
        }

        if (properties.containsKey(MEMORY_LIMIT)) {
            String memoryLimit = properties.get(MEMORY_LIMIT);
            if (!memoryLimit.endsWith("%")) {
                throw new DdlException(MEMORY_LIMIT + " " + memoryLimit + " requires a percentage and ends with a '%'");
            }
            String memLimitErr = MEMORY_LIMIT + " " + memoryLimit + " requires a positive floating point number.";
            try {
                if (Double.parseDouble(memoryLimit.substring(0, memoryLimit.length() - 1)) <= 0) {
                    throw new DdlException(memLimitErr);
                }
            } catch (NumberFormatException e) {
                LOG.debug(memLimitErr, e);
                throw new DdlException(memLimitErr);
            }
        }

        if (properties.containsKey(ENABLE_MEMORY_OVERCOMMIT)) {
            String value = properties.get(ENABLE_MEMORY_OVERCOMMIT).toLowerCase();
            if (!("true".equals(value) || "false".equals(value))) {
                throw new DdlException("The value of '" + ENABLE_MEMORY_OVERCOMMIT + "' must be true or false.");
            }
        }

        // check queue property
        if (properties.containsKey(MAX_CONCURRENCY)) {
            try {
                if (Integer.parseInt(properties.get(MAX_CONCURRENCY)) < 0) {
                    throw new DdlException(MAX_CONCURRENCY + " requires a positive integer");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(MAX_CONCURRENCY + " requires a positive integer");
            }
        }
        if (properties.containsKey(MAX_QUEUE_SIZE)) {
            try {
                if (Integer.parseInt(properties.get(MAX_QUEUE_SIZE)) < 0) {
                    throw new DdlException(MAX_QUEUE_SIZE + " requires a positive integer");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(MAX_QUEUE_SIZE + " requires a positive integer");
            }
        }
        if (properties.containsKey(QUEUE_TIMEOUT)) {
            try {
                if (Integer.parseInt(properties.get(QUEUE_TIMEOUT)) < 0) {
                    throw new DdlException(QUEUE_TIMEOUT + " requires a positive integer");
                }
            } catch (NumberFormatException e) {
                throw new DdlException(QUEUE_TIMEOUT + " requires a positive integer");
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

    public long getVersion() {
        return version;
    }

    public double getMemoryLimitPercent() {
        return memoryLimitPercent;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public int getQueueTimeout() {
        return queueTimeout;
    }

    public void getProcNodeData(BaseProcResult result, QueryQueue qq) {
        List<String> row = new ArrayList<>();
        row.add(String.valueOf(id));
        row.add(name);
        // skip id,name,running query,waiting query
        for (int i = 2; i < WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.size() - 2; i++) {
            String key = WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.get(i);
            if (CPU_HARD_LIMIT.equalsIgnoreCase(key)) {
                String val = properties.get(key);
                if (StringUtils.isEmpty(val)) { // cpu_hard_limit is not required
                    row.add("0%");
                } else {
                    row.add(val + "%");
                }
            } else if (CPU_SHARE.equals(key) && !properties.containsKey(key)) {
                row.add("1024");
            } else if (MEMORY_LIMIT.equals(key) && !properties.containsKey(key)) {
                row.add("0%");
            } else if (ENABLE_MEMORY_OVERCOMMIT.equals(key) && !properties.containsKey(key)) {
                row.add("true");
            } else {
                row.add(properties.get(key));
            }
        }
        row.add(qq == null ? "0" : String.valueOf(qq.getCurrentRunningQueryNum()));
        row.add(qq == null ? "0" : String.valueOf(qq.getCurrentWaitingQueryNum()));
        result.addRow(row);
    }

    public int getCpuHardLimit() {
        return cpuHardLimit;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public TPipelineWorkloadGroup toThrift() {
        return new TPipelineWorkloadGroup().setId(id);
    }

    public TopicInfo toTopicInfo() {
        TWorkloadGroupInfo tWorkloadGroupInfo = new TWorkloadGroupInfo();
        tWorkloadGroupInfo.setId(id);
        tWorkloadGroupInfo.setName(name);
        tWorkloadGroupInfo.setVersion(version);

        String cpuShareStr = properties.get(CPU_SHARE);
        if (cpuShareStr != null) {
            tWorkloadGroupInfo.setCpuShare(Long.valueOf(cpuShareStr));
        }

        String cpuHardLimitStr = properties.get(CPU_HARD_LIMIT);
        if (cpuHardLimitStr != null) {
            tWorkloadGroupInfo.setCpuHardLimit(Integer.valueOf(cpuHardLimitStr));
        }

        String memLimitStr = properties.get(MEMORY_LIMIT);
        if (memLimitStr != null) {
            tWorkloadGroupInfo.setMemLimit(memLimitStr);
        }
        String memOvercommitStr = properties.get(ENABLE_MEMORY_OVERCOMMIT);
        if (memOvercommitStr != null) {
            tWorkloadGroupInfo.setEnableMemoryOvercommit(Boolean.valueOf(memOvercommitStr));
        }
        // enable_cpu_hard_limit = true, using cpu hard limit
        // enable_cpu_hard_limit = false, using cpu soft limit
        tWorkloadGroupInfo.setEnableCpuHardLimit(Config.enable_cpu_hard_limit);

        if (Config.enable_cpu_hard_limit && cpuHardLimit <= 0) {
            LOG.warn("enable_cpu_hard_limit=true but cpuHardLimit value not illegal,"
                    + "id=" + id + ",name=" + name);
        }

        TopicInfo topicInfo = new TopicInfo();
        topicInfo.setWorkloadGroupInfo(tWorkloadGroupInfo);
        return topicInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static WorkloadGroup read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkloadGroup.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (properties.containsKey(MEMORY_LIMIT)) {
            String memoryLimitString = properties.get(MEMORY_LIMIT);
            this.memoryLimitPercent = Double.parseDouble(memoryLimitString.substring(0,
                    memoryLimitString.length() - 1));
        }

        if (properties.containsKey(CPU_HARD_LIMIT)) {
            this.cpuHardLimit = Integer.parseInt(properties.get(CPU_HARD_LIMIT));
        }

        this.resetQueueProperty(this.properties);
    }
}
