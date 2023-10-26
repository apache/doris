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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TTopicInfoType;
import org.apache.doris.thrift.TopicInfo;

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

public class WorkloadGroup implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(WorkloadGroup.class);

    public static final String CPU_SHARE = "cpu_share";

    public static final String CPU_HARD_LIMIT = "cpu_hard_limit";

    public static final String MEMORY_LIMIT = "memory_limit";

    public static final String ENABLE_MEMORY_OVERCOMMIT = "enable_memory_overcommit";

    public static final String MAX_CONCURRENCY = "max_concurrency";

    public static final String MAX_QUEUE_SIZE = "max_queue_size";

    public static final String QUEUE_TIMEOUT = "queue_timeout";

    private static final ImmutableSet<String> REQUIRED_PROPERTIES_NAME = new ImmutableSet.Builder<String>().add(
            CPU_SHARE).add(MEMORY_LIMIT).build();

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

    private double memoryLimitPercent;

    private QueryQueue queryQueue;
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
        String memoryLimitString = properties.get(MEMORY_LIMIT);
        this.memoryLimitPercent = Double.parseDouble(memoryLimitString.substring(0, memoryLimitString.length() - 1));
        if (properties.containsKey(ENABLE_MEMORY_OVERCOMMIT)) {
            properties.put(ENABLE_MEMORY_OVERCOMMIT, properties.get(ENABLE_MEMORY_OVERCOMMIT).toLowerCase());
        }
        if (properties.containsKey(CPU_HARD_LIMIT)) {
            this.cpuHardLimit = Integer.parseInt(properties.get(CPU_HARD_LIMIT));
        }
    }

    // called when first create a resource group, load from image or user new create a group
    public void initQueryQueue() {
        resetQueueProperty(properties);
        // if query queue property is not set, when use default value
        this.queryQueue = new QueryQueue(maxConcurrency, maxQueueSize, queueTimeout);
    }

    void resetQueryQueue(QueryQueue queryQueue) {
        resetQueueProperty(properties);
        this.queryQueue = queryQueue;
        this.queryQueue.resetQueueProperty(this.maxConcurrency, this.maxQueueSize, this.queueTimeout);

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

    public QueryQueue getQueryQueue() {
        return this.queryQueue;
    }

    // new resource group
    public static WorkloadGroup create(String name, Map<String, String> properties) throws DdlException {
        checkProperties(properties);
        WorkloadGroup newWorkloadGroup = new WorkloadGroup(Env.getCurrentEnv().getNextId(), name, properties);
        newWorkloadGroup.initQueryQueue();
        return newWorkloadGroup;
    }

    // alter resource group
    public static WorkloadGroup copyAndUpdate(WorkloadGroup workloadGroup, Map<String, String> updateProperties)
            throws DdlException {
        Map<String, String> newProperties = new HashMap<>(workloadGroup.getProperties());
        for (Map.Entry<String, String> kv : updateProperties.entrySet()) {
            if (!Strings.isNullOrEmpty(kv.getValue())) {
                newProperties.put(kv.getKey(), kv.getValue());
            }
        }

        checkProperties(newProperties);
        WorkloadGroup newWorkloadGroup = new WorkloadGroup(
                workloadGroup.getId(), workloadGroup.getName(), newProperties, workloadGroup.getVersion() + 1);

        // note(wb) query queue should be unique and can not be copy
        newWorkloadGroup.resetQueryQueue(workloadGroup.getQueryQueue());

        return newWorkloadGroup;
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

        if (properties.containsKey(CPU_HARD_LIMIT)) {
            String cpuHardLimit = properties.get(CPU_HARD_LIMIT);
            if (!StringUtils.isNumeric(cpuHardLimit) || Long.parseLong(cpuHardLimit) <= 0) {
                throw new DdlException(CPU_HARD_LIMIT + " " + cpuHardLimit + " requires a positive integer.");
            }
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
        } catch (NumberFormatException e) {
            LOG.debug(memLimitErr, e);
            throw new DdlException(memLimitErr);
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

    public int getCpuHardLimit() {
        return cpuHardLimit;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public TPipelineWorkloadGroup toThrift() {
        //note(wb) we need add a new key-value to properties and then transfer it to be, so need a copy here
        // see WorkloadGroupMgr.getWorkloadGroup
        HashMap<String, String> clonedHashMap = new HashMap<>();
        clonedHashMap.putAll(properties);
        return new TPipelineWorkloadGroup().setId(id).setName(name).setProperties(clonedHashMap).setVersion(version);
    }

    public TopicInfo toTopicInfo() {
        HashMap<String, String> newHashMap = new HashMap<>();
        newHashMap.put("id", String.valueOf(id));
        TopicInfo topicInfo = new TopicInfo();
        topicInfo.setTopicType(TTopicInfoType.WORKLOAD_GROUP);
        topicInfo.setInfoMap(newHashMap);
        topicInfo.setTopicKey(name);
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
        } else {
            this.memoryLimitPercent = 100;
            this.properties.put(MEMORY_LIMIT, "100%");
        }
        if (properties.containsKey(CPU_HARD_LIMIT)) {
            this.cpuHardLimit = Integer.parseInt(properties.get(CPU_HARD_LIMIT));
        }
        this.initQueryQueue();
    }
}
