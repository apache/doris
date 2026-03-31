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

import org.apache.doris.common.Pair;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TBinlogConfig;
import org.apache.doris.thrift.TBinlogFormat;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class BinlogConfig {
    @SerializedName("enable")
    private boolean enable;

    @SerializedName("ttlSeconds")
    private long ttlSeconds;

    @SerializedName("maxBytes")
    private long maxBytes;

    @SerializedName("maxHistoryNums")
    private long maxHistoryNums;

    @SerializedName("binlogFormat")
    private BinlogFormat binlogFormat;

    public enum BinlogFormat {
        // record all meta update operator, and generate snapshot for write data, only used for ccr
        STATEMENT_AND_SNAPSHOT(0),
        // generate row binlog when write, used for table binlog transform
        ROW(1),
        // generate row binlog when need, calculate binlog by compaction and read snapshot,
        // used for table binlog transform
        DELTA(2);

        private final int value;

        BinlogFormat(int value) {
            this.value = value;
        }

        public int value() {
            return this.value;
        }
    }

    @SerializedName("needHistoricalValue")
    private boolean needHistoricalValue;
    public static final long NO_TTL = -1L;
    public static final long TTL_SECONDS = 86400L; // 1 day
    public static final long MAX_BYTES = 0x7fffffffffffffffL;
    public static final long MAX_HISTORY_NUMS = 0x7fffffffffffffffL;

    private static final Logger LOG = LogManager.getLogger(BinlogConfig.class);

    public BinlogConfig(boolean enable, long ttlSeconds, long maxBytes, long maxHistoryNums,
                        BinlogFormat binlogFormat, boolean needHistoricalValue) {
        this.enable = enable;
        this.ttlSeconds = ttlSeconds;
        this.maxBytes = maxBytes;
        this.maxHistoryNums = maxHistoryNums;
        this.binlogFormat = binlogFormat;
        this.needHistoricalValue = needHistoricalValue;
    }

    public BinlogConfig(BinlogConfig config) {
        this(config.enable, config.ttlSeconds, config.maxBytes, config.maxHistoryNums,
                config.getBinlogFormat(), config.needHistoricalValue);
    }

    public BinlogConfig() {
        this(false, TTL_SECONDS, MAX_BYTES, MAX_HISTORY_NUMS, BinlogFormat.STATEMENT_AND_SNAPSHOT, false);
    }

    public Pair<Boolean, String> mergeFromProperties(Map<String, String> properties) {
        return mergeFromProperties(properties, true);
    }

    public Pair<Boolean, String> mergeFromProperties(Map<String, String> properties, boolean force) {
        if (properties == null) {
            return Pair.of(true, null);
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE)) {
            boolean tmpEnable = Boolean.parseBoolean(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE));
            if (!force && this.enable != tmpEnable && binlogFormat == BinlogFormat.ROW) {
                LOG.warn("can't disable binlog when format is [Row]");
                return Pair.of(false, "can't disable binlog when format is [Row]");
            }
            enable = tmpEnable;
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS)) {
            ttlSeconds = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES)) {
            maxBytes = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES));
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS)) {
            maxHistoryNums = Long.parseLong(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS));
        }

        // before binlogFormat change
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_NEED_HISTORICAL_VALUE)) {
            boolean tmpNeedHistoricalValue = Boolean.parseBoolean(
                    properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_NEED_HISTORICAL_VALUE));
            if (!force && this.needHistoricalValue != tmpNeedHistoricalValue) {
                LOG.warn("not support change {} from {} to {}",
                        PropertyAnalyzer.PROPERTIES_BINLOG_NEED_HISTORICAL_VALUE,
                        this.needHistoricalValue, tmpNeedHistoricalValue);
                return Pair.of(false, "not support change "
                        + PropertyAnalyzer.PROPERTIES_BINLOG_NEED_HISTORICAL_VALUE
                        + " from " + this.needHistoricalValue + " to " + tmpNeedHistoricalValue);
            }
            needHistoricalValue = tmpNeedHistoricalValue;
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT)) {
            BinlogFormat tmpBinlogFormat =
                    BinlogFormat.valueOf(properties.get(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT));
            if (!force && tmpBinlogFormat != binlogFormat) {
                LOG.warn("not support change {} from {} to {}",
                        PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT,
                        binlogFormat, tmpBinlogFormat);
                return Pair.of(false, "not support change binlog format "
                        + "from " + binlogFormat + " to " + tmpBinlogFormat);
            }
            binlogFormat = tmpBinlogFormat;
        }
        return Pair.of(true, null);
    }

    public boolean getEnable() {
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

    public BinlogFormat getBinlogFormat() {
        return binlogFormat;
    }

    public void setBinlogFormat(BinlogFormat binlogFormat) {
        this.binlogFormat = binlogFormat;
    }

    public boolean getNeedHistoricalValue() {
        return needHistoricalValue;
    }

    public void setNeedHistoricalValue(boolean needHistoricalValue) {
        this.needHistoricalValue = needHistoricalValue;
    }

    public boolean isEnableForCCR() {
        return enable && binlogFormat != BinlogFormat.ROW;
    }

    public boolean isEnableForStreaming() {
        return enable && binlogFormat == BinlogFormat.ROW;
    }

    public TBinlogConfig toThrift() {
        TBinlogConfig tBinlogConfig = new TBinlogConfig();
        tBinlogConfig.setEnable(enable);
        tBinlogConfig.setTtlSeconds(ttlSeconds);
        tBinlogConfig.setMaxBytes(maxBytes);
        tBinlogConfig.setMaxHistoryNums(maxHistoryNums);
        if (binlogFormat != null) {
            tBinlogConfig.setBinlogFormat(TBinlogFormat.valueOf(binlogFormat.name()));
        }
        tBinlogConfig.setNeedHistoricalValue(needHistoricalValue);
        return tBinlogConfig;
    }

    public Map<String, String> toProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE, String.valueOf(enable));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS, String.valueOf(ttlSeconds));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES, String.valueOf(maxBytes));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS, String.valueOf(maxHistoryNums));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT, String.valueOf(binlogFormat));
        properties.put(PropertyAnalyzer.PROPERTIES_BINLOG_NEED_HISTORICAL_VALUE, String.valueOf(needHistoricalValue));
        return properties;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BinlogConfig)) {
            return false;
        }
        BinlogConfig other = (BinlogConfig) obj;
        return enable == other.enable
                && ttlSeconds == other.ttlSeconds
                && maxBytes == other.maxBytes
                && maxHistoryNums == other.maxHistoryNums
                && binlogFormat == other.binlogFormat
                && needHistoricalValue == other.needHistoricalValue;
    }

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public void appendToShowCreateTable(StringBuilder sb) {
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_ENABLE).append("\" = \"")
                .append(enable).append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_TTL_SECONDS).append("\" = \"")
                .append(ttlSeconds).append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_BYTES).append("\" = \"")
                .append(maxBytes).append("\"");
        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_MAX_HISTORY_NUMS).append("\" = \"")
                .append(maxHistoryNums).append("\"");

        sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_FORMAT).append("\" = \"")
                .append(binlogFormat).append("\"");
        if (binlogFormat == BinlogFormat.ROW) {
            sb.append(",\n\"").append(PropertyAnalyzer.PROPERTIES_BINLOG_NEED_HISTORICAL_VALUE)
                    .append("\" = \"").append(needHistoricalValue).append("\"");
        }
    }

    public static BinlogConfig fromProperties(Map<String, String> properties) {
        BinlogConfig binlogConfig = new BinlogConfig();
        binlogConfig.mergeFromProperties(properties);
        return binlogConfig;
    }
}
