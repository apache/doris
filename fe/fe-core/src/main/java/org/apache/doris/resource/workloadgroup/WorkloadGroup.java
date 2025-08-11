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
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TWgSlotMemoryPolicy;
import org.apache.doris.thrift.TWorkloadGroupInfo;
import org.apache.doris.thrift.TopicInfo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class WorkloadGroup implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(WorkloadGroup.class);

    public static final String CPU_SHARE = "cpu_share";

    public static final String CPU_HARD_LIMIT = "cpu_hard_limit";

    public static final String MEMORY_LIMIT = "memory_limit";

    public static final String ENABLE_MEMORY_OVERCOMMIT = "enable_memory_overcommit";

    public static final String WRITE_BUFFER_RATIO = "write_buffer_ratio";

    public static final String MAX_CONCURRENCY = "max_concurrency";

    public static final String MAX_QUEUE_SIZE = "max_queue_size";

    public static final String QUEUE_TIMEOUT = "queue_timeout";

    public static final String SCAN_THREAD_NUM = "scan_thread_num";

    public static final String MAX_REMOTE_SCAN_THREAD_NUM = "max_remote_scan_thread_num";

    public static final String MIN_REMOTE_SCAN_THREAD_NUM = "min_remote_scan_thread_num";

    public static final String MEMORY_LOW_WATERMARK = "memory_low_watermark";

    public static final String MEMORY_HIGH_WATERMARK = "memory_high_watermark";

    public static final String SLOT_MEMORY_POLICY = "slot_memory_policy";

    public static final String TAG = "tag";

    public static final String READ_BYTES_PER_SECOND = "read_bytes_per_second";

    public static final String REMOTE_READ_BYTES_PER_SECOND = "remote_read_bytes_per_second";

    // deprecated, use MEMORY_LOW_WATERMARK and MEMORY_HIGH_WATERMARK instead.
    public static final String SPILL_THRESHOLD_LOW_WATERMARK = "spill_threshold_low_watermark";
    public static final String SPILL_THRESHOLD_HIGH_WATERMARK = "spill_threshold_high_watermark";

    public static final String COMPUTE_GROUP = "compute_group";

    // NOTE(wb): all property is not required, some properties default value is set in be
    // default value is as followed
    // cpu_share=1024, memory_limit=0%(0 means not limit), enable_memory_overcommit=true
    private static final ImmutableSet<String> ALL_PROPERTIES_NAME = new ImmutableSet.Builder<String>()
            .add(CPU_SHARE).add(MEMORY_LIMIT).add(ENABLE_MEMORY_OVERCOMMIT).add(MAX_CONCURRENCY)
            .add(MAX_QUEUE_SIZE).add(QUEUE_TIMEOUT).add(CPU_HARD_LIMIT).add(SCAN_THREAD_NUM)
            .add(MAX_REMOTE_SCAN_THREAD_NUM).add(MIN_REMOTE_SCAN_THREAD_NUM)
            .add(MEMORY_LOW_WATERMARK).add(MEMORY_HIGH_WATERMARK)
            .add(COMPUTE_GROUP).add(READ_BYTES_PER_SECOND).add(REMOTE_READ_BYTES_PER_SECOND)
            .add(WRITE_BUFFER_RATIO).add(SLOT_MEMORY_POLICY).build();


    private static final ImmutableMap<String, String> DEPRECATED_PROPERTIES_NAME =
            new ImmutableMap.Builder<String, String>()
                    .put(SPILL_THRESHOLD_LOW_WATERMARK, MEMORY_LOW_WATERMARK)
                    .put(SPILL_THRESHOLD_HIGH_WATERMARK, MEMORY_HIGH_WATERMARK).build();


    public static final int CPU_HARD_LIMIT_DEFAULT_VALUE = 100;
    // Memory limit is a string value ending with % in BE, so it is different from other limits
    // other limit is a number.
    public static final String MEMORY_LIMIT_DEFAULT_VALUE = "100%";
    public static final int MEMORY_LOW_WATERMARK_DEFAULT_VALUE = 75;
    public static final int MEMORY_HIGH_WATERMARK_DEFAULT_VALUE = 85;

    private static final Map<String, String> ALL_PROPERTIES_DEFAULT_VALUE_MAP = Maps.newHashMap();

    static {
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(CPU_SHARE, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(CPU_HARD_LIMIT, "100");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MEMORY_LIMIT, "100%");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(ENABLE_MEMORY_OVERCOMMIT, "false");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MAX_CONCURRENCY, String.valueOf(Integer.MAX_VALUE));
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MAX_QUEUE_SIZE, "0");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(QUEUE_TIMEOUT, "0");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(SCAN_THREAD_NUM, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MAX_REMOTE_SCAN_THREAD_NUM, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MIN_REMOTE_SCAN_THREAD_NUM, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MEMORY_LOW_WATERMARK, "75%");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MEMORY_HIGH_WATERMARK, "85%");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(COMPUTE_GROUP, "");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(READ_BYTES_PER_SECOND, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(REMOTE_READ_BYTES_PER_SECOND, "-1");
    }

    public static final int WRITE_BUFFER_RATIO_DEFAULT_VALUE = 20;
    public static final String SLOT_MEMORY_POLICY_DEFAULT_VALUE = "none";
    public static final HashSet<String> AVAILABLE_SLOT_MEMORY_POLICY_VALUES = new HashSet<String>() {{
            add("none");
            add("fixed");
            add("dynamic");
        }};

    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    // Version update required after alter resource group
    @SerializedName(value = "version")
    private long version;

    private QueryQueue queryQueue;

    WorkloadGroup(long id, String name, Map<String, String> properties) {
        this(id, name, properties, 0);
    }

    private WorkloadGroup(long id, String name, Map<String, String> properties, long version) {
        this.id = id;
        this.name = name;
        this.properties = properties;
        this.version = version;

        if (properties.containsKey(WRITE_BUFFER_RATIO)) {
            String loadBufLimitStr = properties.get(WRITE_BUFFER_RATIO);
            if (loadBufLimitStr.endsWith("%")) {
                loadBufLimitStr = loadBufLimitStr.substring(0, loadBufLimitStr.length() - 1);
            }
            this.properties.put(WRITE_BUFFER_RATIO, loadBufLimitStr);
        } else {
            this.properties.put(WRITE_BUFFER_RATIO, WRITE_BUFFER_RATIO_DEFAULT_VALUE + "");
        }

        if (properties.containsKey(SLOT_MEMORY_POLICY)) {
            String slotPolicy = properties.get(SLOT_MEMORY_POLICY);
            this.properties.put(SLOT_MEMORY_POLICY, slotPolicy);
        } else {
            this.properties.put(SLOT_MEMORY_POLICY, SLOT_MEMORY_POLICY_DEFAULT_VALUE);
        }

        if (properties.containsKey(ENABLE_MEMORY_OVERCOMMIT)) {
            properties.put(ENABLE_MEMORY_OVERCOMMIT, properties.get(ENABLE_MEMORY_OVERCOMMIT).toLowerCase());
        } else {
            properties.put(ENABLE_MEMORY_OVERCOMMIT, "false");
        }

        if (properties.containsKey(CPU_HARD_LIMIT)) {
            String cpuHardLimitStr = properties.get(CPU_HARD_LIMIT);
            if (cpuHardLimitStr.endsWith("%")) {
                cpuHardLimitStr = cpuHardLimitStr.substring(0, cpuHardLimitStr.length() - 1);
            }
            this.properties.put(CPU_HARD_LIMIT, cpuHardLimitStr);
        } else {
            this.properties.put(CPU_HARD_LIMIT, CPU_HARD_LIMIT_DEFAULT_VALUE + "");
        }

        if (properties.containsKey(MEMORY_LIMIT)) {
            String memHardLimitStr = properties.get(MEMORY_LIMIT);
            // If the input is -1, it means use all memory, in this version we could change it to 100% now
            // since sum of all group's memory could be larger than 100%
            if (memHardLimitStr.equals("-1")) {
                memHardLimitStr = MEMORY_LIMIT_DEFAULT_VALUE;
            }
            this.properties.put(MEMORY_LIMIT, memHardLimitStr);
        } else {
            this.properties.put(MEMORY_LIMIT, MEMORY_LIMIT_DEFAULT_VALUE);
        }

        if (properties.containsKey(MEMORY_LOW_WATERMARK)) {
            String lowWatermarkStr = properties.get(MEMORY_LOW_WATERMARK);
            if (lowWatermarkStr.endsWith("%")) {
                lowWatermarkStr = lowWatermarkStr.substring(0, lowWatermarkStr.length() - 1);
            }
            this.properties.put(MEMORY_LOW_WATERMARK, lowWatermarkStr);
        } else {
            this.properties.put(MEMORY_LOW_WATERMARK, MEMORY_LOW_WATERMARK_DEFAULT_VALUE + "");
        }
        if (properties.containsKey(MEMORY_HIGH_WATERMARK)) {
            String highWatermarkStr = properties.get(MEMORY_HIGH_WATERMARK);
            if (highWatermarkStr.endsWith("%")) {
                highWatermarkStr = highWatermarkStr.substring(0, highWatermarkStr.length() - 1);
            }
            this.properties.put(MEMORY_HIGH_WATERMARK, highWatermarkStr);
        } else {
            this.properties.put(MEMORY_HIGH_WATERMARK, MEMORY_HIGH_WATERMARK_DEFAULT_VALUE + "");
        }
        initQueryQueue();
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
            newProperties.put(kv.getKey(), kv.getValue());
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
            String inputValue = properties.get(CPU_SHARE);
            try {
                int cpuShareI = Integer.parseInt(inputValue);
                if (cpuShareI <= 0 && cpuShareI != -1) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + CPU_SHARE + " value is -1 or a positive integer, but input value is "
                                + inputValue);
            }
        }

        if (properties.containsKey(CPU_HARD_LIMIT)) {
            String inputValue = properties.get(CPU_HARD_LIMIT);
            String cpuHardLimit = inputValue;
            try {
                boolean endWithSign = false;
                if (cpuHardLimit.endsWith("%")) {
                    cpuHardLimit = cpuHardLimit.substring(0, cpuHardLimit.length() - 1);
                    endWithSign = true;
                }

                int intVal = Integer.parseInt(cpuHardLimit);
                if (endWithSign && intVal == -1) {
                    throw new NumberFormatException();
                }
                if (!(intVal >= 1 && intVal <= 100) && -1 != intVal) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + WorkloadGroup.CPU_HARD_LIMIT
                                + " value is -1 or a positive integer between 1 and 100, but input value is "
                                + inputValue);
            }
        }

        if (properties.containsKey(MEMORY_LIMIT)) {
            String memoryLimitStr = properties.get(MEMORY_LIMIT);
            if (!memoryLimitStr.endsWith("%") && !"-1".equals(memoryLimitStr)) {
                throw new DdlException(
                        MEMORY_LIMIT + " requires a percentage value which ends with a '%' or -1, but input value is "
                                + memoryLimitStr);
            }
            if (!"-1".equals(memoryLimitStr)) {
                try {
                    double memLimitD = Double.parseDouble(memoryLimitStr.substring(0, memoryLimitStr.length() - 1));
                    if (memLimitD <= 0) {
                        throw new NumberFormatException();
                    }
                } catch (NumberFormatException e) {
                    throw new DdlException("The allowed " + MEMORY_LIMIT
                            + " value is a positive floating point number, but input value is " + memoryLimitStr);
                }
            }
        }

        if (properties.containsKey(WRITE_BUFFER_RATIO)) {
            String writeBufSizeStr = properties.get(WRITE_BUFFER_RATIO);
            String memLimitErr = WRITE_BUFFER_RATIO + " " + writeBufSizeStr
                    + " requires a positive int number.";
            if (writeBufSizeStr.endsWith("%")) {
                writeBufSizeStr = writeBufSizeStr.substring(0, writeBufSizeStr.length() - 1);
            }
            try {
                if (Integer.parseInt(writeBufSizeStr) < 0) {
                    throw new DdlException(memLimitErr);
                }
            } catch (NumberFormatException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(memLimitErr, e);
                }
                throw new DdlException(memLimitErr);
            }
        }

        if (properties.containsKey(ENABLE_MEMORY_OVERCOMMIT)) {
            String inputValue = properties.get(ENABLE_MEMORY_OVERCOMMIT);
            String value = inputValue.toLowerCase();
            if (!("true".equals(value) || "false".equals(value))) {
                throw new DdlException(
                        "The value of '" + ENABLE_MEMORY_OVERCOMMIT + "' must be true or false. but input value is "
                                + inputValue);
            }
        }

        if (properties.containsKey(SLOT_MEMORY_POLICY)) {
            String value = properties.get(SLOT_MEMORY_POLICY).toLowerCase();
            if (!AVAILABLE_SLOT_MEMORY_POLICY_VALUES.contains(value)) {
                throw new DdlException("The value of '" + SLOT_MEMORY_POLICY
                        + "' must be one of none, fixed, dynamic.");
            }
        }

        if (properties.containsKey(SCAN_THREAD_NUM)) {
            String value = properties.get(SCAN_THREAD_NUM);
            try {
                int intValue = Integer.parseInt(value);
                if (intValue <= 0 && intValue != -1) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + SCAN_THREAD_NUM + " value is -1 or a positive integer. but input value is "
                                + value);
            }
        }

        int maxRemoteScanNum = -1;
        if (properties.containsKey(MAX_REMOTE_SCAN_THREAD_NUM)) {
            String value = properties.get(MAX_REMOTE_SCAN_THREAD_NUM);
            try {
                int intValue = Integer.parseInt(value);
                if (intValue <= 0 && intValue != -1) {
                    throw new NumberFormatException();
                }
                maxRemoteScanNum = intValue;
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + MAX_REMOTE_SCAN_THREAD_NUM
                                + " value is -1 or a positive integer. but input value is " + value);
            }
        }

        int minRemoteScanNum = -1;
        if (properties.containsKey(MIN_REMOTE_SCAN_THREAD_NUM)) {
            String value = properties.get(MIN_REMOTE_SCAN_THREAD_NUM);
            try {
                int intValue = Integer.parseInt(value);
                if (intValue <= 0 && intValue != -1) {
                    throw new NumberFormatException();
                }
                minRemoteScanNum = intValue;
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + MIN_REMOTE_SCAN_THREAD_NUM
                                + " value is -1 or a positive integer. but input value is " + value);
            }
        }

        if ((maxRemoteScanNum == -1 && minRemoteScanNum != -1) || (maxRemoteScanNum != -1 && minRemoteScanNum == -1)) {
            throw new DdlException(MAX_REMOTE_SCAN_THREAD_NUM + " and " + MIN_REMOTE_SCAN_THREAD_NUM
                    + " must be specified simultaneously");
        } else if (maxRemoteScanNum < minRemoteScanNum) {
            throw new DdlException(MAX_REMOTE_SCAN_THREAD_NUM + " must bigger or equal " + MIN_REMOTE_SCAN_THREAD_NUM);
        }

        // check queue property
        if (properties.containsKey(MAX_CONCURRENCY)) {
            String inputValue = properties.get(MAX_CONCURRENCY);
            try {
                if (Integer.parseInt(inputValue) < 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + MAX_CONCURRENCY
                                + " value is an integer greater than or equal to 0, but input value is "
                                + inputValue);
            }
        }
        if (properties.containsKey(MAX_QUEUE_SIZE)) {
            String inputValue = properties.get(MAX_QUEUE_SIZE);
            try {
                if (Integer.parseInt(inputValue) < 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + MAX_QUEUE_SIZE
                                + " value is an integer greater than or equal to 0, but input value is "
                                + inputValue);
            }
        }
        if (properties.containsKey(QUEUE_TIMEOUT)) {
            String inputValue = properties.get(QUEUE_TIMEOUT);
            try {
                if (Integer.parseInt(inputValue) < 0) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + QUEUE_TIMEOUT
                                + " value is an integer greater than or equal to 0, but input value is "
                                + inputValue);
            }
        }

        int lowWaterMark = MEMORY_LOW_WATERMARK_DEFAULT_VALUE;
        if (properties.containsKey(MEMORY_LOW_WATERMARK)) {
            String lowVal = properties.get(MEMORY_LOW_WATERMARK);
            if (lowVal.endsWith("%")) {
                lowVal = lowVal.substring(0, lowVal.length() - 1);
            }
            try {
                int intValue = Integer.parseInt(lowVal);
                if ((intValue < 1 || intValue > 100)) {
                    throw new NumberFormatException();
                }
                lowWaterMark = intValue;
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + MEMORY_LOW_WATERMARK
                                + " value is an integer value between 1 and 100, but input value is "
                                + lowVal);
            }
        }

        int highWaterMark = MEMORY_HIGH_WATERMARK_DEFAULT_VALUE;
        if (properties.containsKey(MEMORY_HIGH_WATERMARK)) {
            String highVal = properties.get(MEMORY_HIGH_WATERMARK);
            if (highVal.endsWith("%")) {
                highVal = highVal.substring(0, highVal.length() - 1);
            }
            try {
                int intValue = Integer.parseInt(highVal);
                if ((intValue < 1 || intValue > 100)) {
                    throw new NumberFormatException();
                }
                highWaterMark = intValue;
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + MEMORY_HIGH_WATERMARK
                                + " value is an integer value between 1 and 100, but input value is "
                                + highVal);
            }
        }

        if (lowWaterMark > highWaterMark) {
            throw new DdlException(MEMORY_HIGH_WATERMARK + "(" + highWaterMark + ") should bigger than "
                    + MEMORY_LOW_WATERMARK + "(" + lowWaterMark + ")");
        }

        if (properties.containsKey(READ_BYTES_PER_SECOND)) {
            String readBytesVal = properties.get(READ_BYTES_PER_SECOND);
            try {
                long longVal = Long.parseLong(readBytesVal);
                if (longVal <= 0 && longVal != -1) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new DdlException(
                        "The allowed " + READ_BYTES_PER_SECOND
                                + " value should be -1 or an positive integer, but input value is "
                                + readBytesVal);
            }
        }

        if (properties.containsKey(REMOTE_READ_BYTES_PER_SECOND)) {
            String readBytesVal = properties.get(REMOTE_READ_BYTES_PER_SECOND);
            try {
                long longVal = Long.parseLong(readBytesVal);
                if (longVal <= 0 && longVal != -1) {
                    throw new NumberFormatException();
                }
            } catch (NumberFormatException e) {
                throw new DdlException("The allowed " + REMOTE_READ_BYTES_PER_SECOND
                        + " value should be -1 or an positive integer, but input value is " + readBytesVal);
            }
        }
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public QueryQueue getQueryQueue() {
        return queryQueue;
    }

    public WorkloadGroupKey getWorkloadGroupKey() {
        return WorkloadGroupKey.get(this.getComputeGroup(), this.getName());
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public long getVersion() {
        return version;
    }

    public void getProcNodeData(BaseProcResult result) {
        List<String> row = new ArrayList<>();
        row.add(String.valueOf(id));
        row.add(name);
        Pair<Integer, Integer> queryQueueDetail = queryQueue.getQueryQueueDetail();
        // skip id,name,running query,waiting query
        for (int i = 2; i < WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.size(); i++) {
            String key = WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.get(i);
            if (QueryQueue.RUNNING_QUERY_NUM.equals(key)) {
                row.add(queryQueueDetail == null ? "0" : String.valueOf(queryQueueDetail.first));
            } else if (QueryQueue.WAITING_QUERY_NUM.equals(key)) {
                row.add(queryQueueDetail == null ? "0" : String.valueOf(queryQueueDetail.second));
            } else if (COMPUTE_GROUP.equals(key)) {
                String val = properties.get(key);
                if (!StringUtils.isEmpty(val) && Config.isCloudMode()) {
                    try {
                        String cgName = ((CloudSystemInfoService) Env.getCurrentSystemInfo()).getClusterNameByClusterId(
                                val);
                        if (!StringUtils.isEmpty(cgName)) {
                            val = cgName;
                        }
                    } catch (Throwable t) {
                        LOG.debug("get compute group failed, ", t);
                    }
                }
                row.add(val);
            } else {
                String val = properties.get(key);
                if (StringUtils.isEmpty(val)) {
                    row.add(ALL_PROPERTIES_DEFAULT_VALUE_MAP.get(key));
                } else if ((CPU_HARD_LIMIT.equals(key) && !"-1".equals(val))
                        || MEMORY_LOW_WATERMARK.equals(key)
                        || MEMORY_HIGH_WATERMARK.equals(key)) {
                    row.add(val + "%");
                } else {
                    row.add(val);
                }
            }
        }
        result.addRow(row);
    }

    public double getMemoryLimitPercent() {
        String memoryStr = properties.get(MEMORY_LIMIT);
        return Double.valueOf(memoryStr.substring(0, memoryStr.length() - 1));
    }

    public String getComputeGroup() {
        String ret = properties.get(COMPUTE_GROUP);
        return StringUtils.isEmpty(ret) ? WorkloadGroupMgr.EMPTY_COMPUTE_GROUP : ret;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public TPipelineWorkloadGroup toThrift() {
        return new TPipelineWorkloadGroup().setId(id);
    }

    public static TWgSlotMemoryPolicy findSlotPolicyValueByString(String slotPolicy) {
        if (slotPolicy.equalsIgnoreCase("none")) {
            return TWgSlotMemoryPolicy.NONE;
        } else if (slotPolicy.equalsIgnoreCase("fixed")) {
            return TWgSlotMemoryPolicy.FIXED;
        } else if (slotPolicy.equalsIgnoreCase("dynamic")) {
            return TWgSlotMemoryPolicy.DYNAMIC;
        } else {
            throw new RuntimeException("Could not find policy using " + slotPolicy);
        }
    }

    public TopicInfo toTopicInfo() {
        TWorkloadGroupInfo tWorkloadGroupInfo = new TWorkloadGroupInfo();
        tWorkloadGroupInfo.setId(this.id);

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
        String writeBufferRatioStr = properties.get(WRITE_BUFFER_RATIO);
        if (writeBufferRatioStr != null) {
            tWorkloadGroupInfo.setWriteBufferRatio(Integer.parseInt(writeBufferRatioStr));
        }
        String slotMemoryPolicyStr = properties.get(SLOT_MEMORY_POLICY);
        if (slotMemoryPolicyStr != null) {
            tWorkloadGroupInfo.setSlotMemoryPolicy(findSlotPolicyValueByString(slotMemoryPolicyStr));
        }
        String memOvercommitStr = properties.get(ENABLE_MEMORY_OVERCOMMIT);
        if (memOvercommitStr != null) {
            tWorkloadGroupInfo.setEnableMemoryOvercommit(Boolean.valueOf(memOvercommitStr));
        }

        String scanThreadNumStr = properties.get(SCAN_THREAD_NUM);
        if (scanThreadNumStr != null) {
            tWorkloadGroupInfo.setScanThreadNum(Integer.parseInt(scanThreadNumStr));
        }

        String maxRemoteScanThreadNumStr = properties.get(MAX_REMOTE_SCAN_THREAD_NUM);
        if (maxRemoteScanThreadNumStr != null) {
            tWorkloadGroupInfo.setMaxRemoteScanThreadNum(Integer.parseInt(maxRemoteScanThreadNumStr));
        }

        String minRemoteScanThreadNumStr = properties.get(MIN_REMOTE_SCAN_THREAD_NUM);
        if (minRemoteScanThreadNumStr != null) {
            tWorkloadGroupInfo.setMinRemoteScanThreadNum(Integer.parseInt(minRemoteScanThreadNumStr));
        }

        String memoryLowWatermarkStr = properties.get(MEMORY_LOW_WATERMARK);
        if (memoryLowWatermarkStr != null) {
            tWorkloadGroupInfo.setMemoryLowWatermark(Integer.parseInt(memoryLowWatermarkStr));
        }

        String memoryHighWatermarkStr = properties.get(MEMORY_HIGH_WATERMARK);
        if (memoryHighWatermarkStr != null) {
            tWorkloadGroupInfo.setMemoryHighWatermark(Integer.parseInt(memoryHighWatermarkStr));
        }

        String readBytesPerSecStr = properties.get(READ_BYTES_PER_SECOND);
        if (readBytesPerSecStr != null) {
            tWorkloadGroupInfo.setReadBytesPerSecond(Long.valueOf(readBytesPerSecStr));
        }

        String remoteReadBytesPerSecStr = properties.get(REMOTE_READ_BYTES_PER_SECOND);
        if (remoteReadBytesPerSecStr != null) {
            tWorkloadGroupInfo.setRemoteReadBytesPerSecond(Long.valueOf(remoteReadBytesPerSecStr));
        }

        String cgStr = properties.get(COMPUTE_GROUP);
        if (!StringUtils.isEmpty(cgStr)) {
            tWorkloadGroupInfo.setTag(cgStr);
        }

        String totalQuerySlotCountStr = properties.get(MAX_CONCURRENCY);
        if (totalQuerySlotCountStr != null) {
            tWorkloadGroupInfo.setTotalQuerySlotCount(Integer.parseInt(totalQuerySlotCountStr));
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
        WorkloadGroup workloadGroup = GsonUtils.GSON.fromJson(json, WorkloadGroup.class);
        // "spill_threshold_low_watermark" and "spill_threshold_high_watermark"
        // are renamed and deprecated, replace them with "memory_low_watermark"
        // and "memory_high_watermark"
        for (String key : DEPRECATED_PROPERTIES_NAME.keySet()) {
            if (workloadGroup.properties.containsKey(key)) {
                String value = workloadGroup.properties.get(key);
                workloadGroup.properties.remove(key);
                workloadGroup.properties.put(DEPRECATED_PROPERTIES_NAME.get(key), value);
            }
        }
        return workloadGroup;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // Do not use -1 as default value, using 100%
        if (properties.containsKey(MEMORY_LIMIT)) {
            String memHardLimitStr = properties.get(MEMORY_LIMIT);
            // If the input is -1, it means use all memory, in this version we could change it to 100% now
            // since sum of all group's memory could be larger than 100%
            if (memHardLimitStr.equals("-1")) {
                memHardLimitStr = MEMORY_LIMIT_DEFAULT_VALUE;
            }
            this.properties.put(MEMORY_LIMIT, memHardLimitStr);
        } else {
            this.properties.put(MEMORY_LIMIT, MEMORY_LIMIT_DEFAULT_VALUE);
        }
        // The from json method just uses reflection logic to create a new workload group
        // but workload group's contructor need create other objects, like queue, so need
        // init queue here after workload group is created from json
        initQueryQueue();
    }

    private void initQueryQueue() {
        int maxConcurrency = Integer.MAX_VALUE;
        int maxQueueSize = 0;
        int queueTimeout = 0;
        if (properties.containsKey(MAX_CONCURRENCY)) {
            maxConcurrency = Integer.parseInt(properties.get(MAX_CONCURRENCY));
        } else {
            maxConcurrency = Integer.MAX_VALUE;
            properties.put(MAX_CONCURRENCY, String.valueOf(maxConcurrency));
        }
        if (properties.containsKey(MAX_QUEUE_SIZE)) {
            maxQueueSize = Integer.parseInt(properties.get(MAX_QUEUE_SIZE));
        } else {
            maxQueueSize = 0;
            properties.put(MAX_QUEUE_SIZE, String.valueOf(maxQueueSize));
        }
        if (properties.containsKey(QUEUE_TIMEOUT)) {
            queueTimeout = Integer.parseInt(properties.get(QUEUE_TIMEOUT));
        } else {
            queueTimeout = 0;
            properties.put(QUEUE_TIMEOUT, String.valueOf(queueTimeout));
        }
        queryQueue = new QueryQueue(id, maxConcurrency, maxQueueSize, queueTimeout);
    }
}
