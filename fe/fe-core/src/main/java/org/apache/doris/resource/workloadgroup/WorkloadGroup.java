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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TPipelineWorkloadGroup;
import org.apache.doris.thrift.TWorkloadGroupInfo;
import org.apache.doris.thrift.TopicInfo;

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
import java.util.Optional;
import java.util.Set;

public class WorkloadGroup implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(WorkloadGroup.class);

    public static final String CPU_SHARE = "cpu_share";

    public static final String CPU_HARD_LIMIT = "cpu_hard_limit";

    public static final String MEMORY_LIMIT = "memory_limit";

    public static final String ENABLE_MEMORY_OVERCOMMIT = "enable_memory_overcommit";

    public static final String MAX_CONCURRENCY = "max_concurrency";

    public static final String MAX_QUEUE_SIZE = "max_queue_size";

    public static final String QUEUE_TIMEOUT = "queue_timeout";

    public static final String SCAN_THREAD_NUM = "scan_thread_num";

    public static final String MAX_REMOTE_SCAN_THREAD_NUM = "max_remote_scan_thread_num";

    public static final String MIN_REMOTE_SCAN_THREAD_NUM = "min_remote_scan_thread_num";

    public static final String SPILL_THRESHOLD_LOW_WATERMARK = "spill_threshold_low_watermark";

    public static final String SPILL_THRESHOLD_HIGH_WATERMARK = "spill_threshold_high_watermark";

    public static final String TAG = "tag";

    public static final String READ_BYTES_PER_SECOND = "read_bytes_per_second";

    public static final String REMOTE_READ_BYTES_PER_SECOND = "remote_read_bytes_per_second";

    // NOTE(wb): all property is not required, some properties default value is set in be
    // default value is as followed
    // cpu_share=1024, memory_limit=0%(0 means not limit), enable_memory_overcommit=true
    private static final ImmutableSet<String> ALL_PROPERTIES_NAME = new ImmutableSet.Builder<String>()
            .add(CPU_SHARE).add(MEMORY_LIMIT).add(ENABLE_MEMORY_OVERCOMMIT).add(MAX_CONCURRENCY)
            .add(MAX_QUEUE_SIZE).add(QUEUE_TIMEOUT).add(CPU_HARD_LIMIT).add(SCAN_THREAD_NUM)
            .add(MAX_REMOTE_SCAN_THREAD_NUM).add(MIN_REMOTE_SCAN_THREAD_NUM)
            .add(SPILL_THRESHOLD_LOW_WATERMARK).add(SPILL_THRESHOLD_HIGH_WATERMARK)
            .add(TAG).add(READ_BYTES_PER_SECOND).add(REMOTE_READ_BYTES_PER_SECOND).build();

    public static final int SPILL_LOW_WATERMARK_DEFAULT_VALUE = 50;
    public static final int SPILL_HIGH_WATERMARK_DEFAULT_VALUE = 80;

    private static final Map<String, String> ALL_PROPERTIES_DEFAULT_VALUE_MAP = Maps.newHashMap();

    static {
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(CPU_SHARE, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(CPU_HARD_LIMIT, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MEMORY_LIMIT, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(ENABLE_MEMORY_OVERCOMMIT, "true");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MAX_CONCURRENCY, String.valueOf(Integer.MAX_VALUE));
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MAX_QUEUE_SIZE, "0");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(QUEUE_TIMEOUT, "0");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(SCAN_THREAD_NUM, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MAX_REMOTE_SCAN_THREAD_NUM, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(MIN_REMOTE_SCAN_THREAD_NUM, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(SPILL_THRESHOLD_LOW_WATERMARK, "50%");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(SPILL_THRESHOLD_HIGH_WATERMARK, "80%");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(TAG, "");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(READ_BYTES_PER_SECOND, "-1");
        ALL_PROPERTIES_DEFAULT_VALUE_MAP.put(REMOTE_READ_BYTES_PER_SECOND, "-1");
    }

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

    WorkloadGroup(long id, String name, Map<String, String> properties) {
        this(id, name, properties, 0);
    }

    private WorkloadGroup(long id, String name, Map<String, String> properties, long version) {
        this.id = id;
        this.name = name;
        this.properties = properties;
        this.version = version;
        if (properties.containsKey(MEMORY_LIMIT)) {
            setMemLimitPercent(properties);
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
        if (properties.containsKey(SPILL_THRESHOLD_LOW_WATERMARK)) {
            String lowWatermarkStr = properties.get(SPILL_THRESHOLD_LOW_WATERMARK);
            if (lowWatermarkStr.endsWith("%")) {
                lowWatermarkStr = lowWatermarkStr.substring(0, lowWatermarkStr.length() - 1);
            }
            this.properties.put(SPILL_THRESHOLD_LOW_WATERMARK, lowWatermarkStr);
        }
        if (properties.containsKey(SPILL_THRESHOLD_HIGH_WATERMARK)) {
            String highWatermarkStr = properties.get(SPILL_THRESHOLD_HIGH_WATERMARK);
            if (highWatermarkStr.endsWith("%")) {
                highWatermarkStr = highWatermarkStr.substring(0, highWatermarkStr.length() - 1);
            }
            this.properties.put(SPILL_THRESHOLD_HIGH_WATERMARK, highWatermarkStr);
        }
        if (properties.containsKey(TAG)) {
            this.properties.put(TAG, properties.get(TAG).toLowerCase());
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

        if (properties.containsKey(ENABLE_MEMORY_OVERCOMMIT)) {
            String inputValue = properties.get(ENABLE_MEMORY_OVERCOMMIT);
            String value = inputValue.toLowerCase();
            if (!("true".equals(value) || "false".equals(value))) {
                throw new DdlException(
                        "The value of '" + ENABLE_MEMORY_OVERCOMMIT + "' must be true or false. but input value is "
                                + inputValue);
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

        int lowWaterMark = SPILL_LOW_WATERMARK_DEFAULT_VALUE;
        if (properties.containsKey(SPILL_THRESHOLD_LOW_WATERMARK)) {
            String lowVal = properties.get(SPILL_THRESHOLD_LOW_WATERMARK);
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
                        "The allowed " + SPILL_THRESHOLD_LOW_WATERMARK
                                + " value is an integer value between 1 and 100, but input value is "
                                + lowVal);
            }
        }

        int highWaterMark = SPILL_HIGH_WATERMARK_DEFAULT_VALUE;
        if (properties.containsKey(SPILL_THRESHOLD_HIGH_WATERMARK)) {
            String highVal = properties.get(SPILL_THRESHOLD_HIGH_WATERMARK);
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
                        "The allowed " + SPILL_THRESHOLD_HIGH_WATERMARK
                                + " value is an integer value between 1 and 100, but input value is "
                                + highVal);
            }
        }

        if (lowWaterMark > highWaterMark) {
            throw new DdlException(SPILL_THRESHOLD_HIGH_WATERMARK + "(" + highWaterMark + ") should bigger than "
                    + SPILL_THRESHOLD_LOW_WATERMARK + "(" + lowWaterMark + ")");
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

        String tagStr = properties.get(TAG);
        if (!StringUtils.isEmpty(tagStr)) {
            String[] tagArr = tagStr.split(",");
            for (String tag : tagArr) {
                try {
                    FeNameFormat.checkCommonName("workload group tag", tag);
                } catch (AnalysisException e) {
                    throw new DdlException("tag format is illegal, " + tagStr);
                }
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
        Pair<Integer, Integer> queryQueueDetail = qq != null ? qq.getQueryQueueDetail() : null;
        // skip id,name,running query,waiting query
        for (int i = 2; i < WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.size(); i++) {
            String key = WorkloadGroupMgr.WORKLOAD_GROUP_PROC_NODE_TITLE_NAMES.get(i);
            if (QueryQueue.RUNNING_QUERY_NUM.equals(key)) {
                row.add(queryQueueDetail == null ? "0" : String.valueOf(queryQueueDetail.first));
            } else if (QueryQueue.WAITING_QUERY_NUM.equals(key)) {
                row.add(queryQueueDetail == null ? "0" : String.valueOf(queryQueueDetail.second));
            } else {
                String val = properties.get(key);
                if (StringUtils.isEmpty(val)) {
                    row.add(ALL_PROPERTIES_DEFAULT_VALUE_MAP.get(key));
                } else if ((CPU_HARD_LIMIT.equals(key) && !"-1".equals(val))
                        || SPILL_THRESHOLD_LOW_WATERMARK.equals(key)
                        || SPILL_THRESHOLD_HIGH_WATERMARK.equals(key)) {
                    row.add(val + "%");
                } else {
                    row.add(val);
                }
            }
        }
        result.addRow(row);
    }

    public int getCpuHardLimitWhenCalSum() {
        return cpuHardLimit == -1 ? 0 : cpuHardLimit;
    }

    public double getMemoryLimitPercentWhenCalSum() {
        return memoryLimitPercent == -1 ? 0 : memoryLimitPercent;
    }

    public double getMemoryLimitPercent() {
        return memoryLimitPercent;
    }

    public Optional<Set<String>> getTag() {
        String tagStr = properties.get(TAG);
        if (StringUtils.isEmpty(tagStr)) {
            return Optional.empty();
        }

        Set<String> tagSet = new HashSet<>();
        String[] ss = tagStr.split(",");
        for (String str : ss) {
            tagSet.add(str);
        }
        return Optional.of(tagSet);
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

        String spillLowWatermarkStr = properties.get(SPILL_THRESHOLD_LOW_WATERMARK);
        if (spillLowWatermarkStr != null) {
            tWorkloadGroupInfo.setSpillThresholdLowWatermark(Integer.parseInt(spillLowWatermarkStr));
        }

        String spillHighWatermarkStr = properties.get(SPILL_THRESHOLD_HIGH_WATERMARK);
        if (spillHighWatermarkStr != null) {
            tWorkloadGroupInfo.setSpillThresholdHighWatermark(Integer.parseInt(spillHighWatermarkStr));
        }

        String tagStr = properties.get(TAG);
        if (!StringUtils.isEmpty(tagStr)) {
            tWorkloadGroupInfo.setTag(tagStr);
        }

        String readBytesPerSecStr = properties.get(READ_BYTES_PER_SECOND);
        if (readBytesPerSecStr != null) {
            tWorkloadGroupInfo.setReadBytesPerSecond(Long.valueOf(readBytesPerSecStr));
        }

        String remoteReadBytesPerSecStr = properties.get(REMOTE_READ_BYTES_PER_SECOND);
        if (remoteReadBytesPerSecStr != null) {
            tWorkloadGroupInfo.setRemoteReadBytesPerSecond(Long.valueOf(remoteReadBytesPerSecStr));
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

    void setMemLimitPercent(Map<String, String> props) {
        String memoryLimitString = props.get(MEMORY_LIMIT);
        this.memoryLimitPercent = "-1".equals(memoryLimitString) ? -1
                : Double.parseDouble(memoryLimitString.substring(0, memoryLimitString.length() - 1));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (properties.containsKey(MEMORY_LIMIT)) {
            setMemLimitPercent(properties);
        }

        if (properties.containsKey(CPU_HARD_LIMIT)) {
            this.cpuHardLimit = Integer.parseInt(properties.get(CPU_HARD_LIMIT));
        }

        this.resetQueueProperty(this.properties);
    }
}
