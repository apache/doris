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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TBinlogConfig;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class BinlogConfig implements Writable {
    @SerializedName("enable")
    private boolean enable;

    @SerializedName("ttlSeconds")
    private long ttlSeconds;

    @SerializedName("maxBytes")
    private long maxBytes;

    @SerializedName("maxHistoryNums")
    private long maxHistoryNums;

    public static final long TTL_SECONDS = 86400L; // 1 day
    public static final long MAX_BYTES = 0x7fffffffffffffffL;
    public static final long MAX_HISTORY_NUMS = 0x7fffffffffffffffL;

    public BinlogConfig(boolean enable, long ttlSeconds, long maxBytes, long maxHistoryNums) {
        this.enable = enable;
        this.ttlSeconds = ttlSeconds;
        this.maxBytes = maxBytes;
        this.maxHistoryNums = maxHistoryNums;
    }

    public BinlogConfig(BinlogConfig config) {
        this(config.enable, config.ttlSeconds, config.maxBytes, config.maxHistoryNums);
    }

    public BinlogConfig() {
        this(false, TTL_SECONDS, MAX_BYTES, MAX_HISTORY_NUMS);
    }

    public void mergeFromProperties(Map<String, String> properties) {
        if (properties == null) {
            return;
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
            enable = Boolean.parseBoolean(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS)) {
            ttlSeconds = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS));

        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES)) {
            maxBytes = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS)) {
            maxHistoryNums = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS));
        }
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public long getTtlSeconds() {
        return ttlSeconds;
    }

    public void setTtlSeconds(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public void setMaxBytes(long maxBytes) {
        this.maxBytes = maxBytes;
    }

    public long getMaxHistoryNums() {
        return maxHistoryNums;
    }

    public void setMaxHistoryNums(long maxHistoryNums) {
        this.maxHistoryNums = maxHistoryNums;
    }

    public TBinlogConfig toThrift() {
        TBinlogConfig tBinlogConfig = new TBinlogConfig();
        tBinlogConfig.setEnable(enable);
        tBinlogConfig.setTtlSeconds(ttlSeconds);
        tBinlogConfig.setMaxBytes(maxBytes);
        tBinlogConfig.setMaxHistoryNums(maxHistoryNums);
        return tBinlogConfig;
    }

    public Map<String, String> toProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, String.valueOf(enable));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS, String.valueOf(ttlSeconds));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES, String.valueOf(maxBytes));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS, String.valueOf(maxHistoryNums));
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof BinlogConfig)) {
            return false;
        }

        BinlogConfig other = (BinlogConfig) obj;
        if (this.enable != other.enable) {
            return false;
        }
        if (this.ttlSeconds != other.ttlSeconds) {
            return false;
        }
        if (this.maxBytes != other.maxBytes) {
            return false;
        }
        return this.maxHistoryNums == other.maxHistoryNums;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static BinlogConfig read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), BinlogConfig.class);
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public void appendToShowCreateTable(StringBuilder sb) {
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE).append("\" = \"").append(enable)
                .append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS).append("\" = \"").append(ttlSeconds)
                .append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES).append("\" = \"").append(maxBytes)
                .append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS).append("\" = \"")
                .append(maxHistoryNums).append("\"");
    }

    public static BinlogConfig fromProperties(Map<String, String> properties) {
        BinlogConfig binlogConfig = new BinlogConfig();
        binlogConfig.mergeFromProperties(properties);
        return binlogConfig;
    }
}
