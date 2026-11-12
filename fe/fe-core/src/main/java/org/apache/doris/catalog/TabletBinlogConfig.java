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

import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TBinlogConfig;
import org.apache.doris.thrift.TBinlogFormat;

import java.util.Objects;

/**
 * The binlog config every tablet of a table carries on BE and Meta Service ({@link TBinlogConfig} /
 * {@link OlapFile.BinlogConfigPB}). CCR binlog and changelog share this one struct, which selects changelog by
 * {@code binlog_format = CHANGELOG}: a tablet keeps a single binlog config, and a BE that does not see the
 * changelog format there would write the companion tablet as an ordinary tablet and lose the changes. This is
 * the only place that builds it; every table that ships a binlog config takes it from
 * {@link OlapTable#getTabletBinlogConfig()}, or from {@link #of} with the configuration being published.
 */
public final class TabletBinlogConfig {
    private final boolean enable;
    private final long ttlSeconds;
    private final long maxBytes;
    private final long maxHistoryNums;
    private final boolean changelog;
    private final boolean needHistoricalValue;
    private final long configVersion;

    private TabletBinlogConfig(boolean enable, long ttlSeconds, long maxBytes, long maxHistoryNums,
            boolean changelog, boolean needHistoricalValue, long configVersion) {
        this.enable = enable;
        this.ttlSeconds = ttlSeconds;
        this.maxBytes = maxBytes;
        this.maxHistoryNums = maxHistoryNums;
        this.changelog = changelog;
        this.needHistoricalValue = needHistoricalValue;
        this.configVersion = configVersion;
    }

    /**
     * A table with changelog ships its changelog configuration in the changelog format, base and companion
     * tablets alike; any other table ships its CCR binlog configuration in the snapshot format. A table never
     * has both (CCR and changelog are mutually exclusive).
     */
    public static TabletBinlogConfig of(BinlogConfig binlogConfig, ChangelogConfig changelogConfig) {
        if (changelogConfig.isEnable()) {
            return new TabletBinlogConfig(true, changelogConfig.getTtlSeconds(), binlogConfig.getMaxBytes(),
                    binlogConfig.getMaxHistoryNums(), true, changelogConfig.getNeedHistoricalValue(),
                    changelogConfig.getConfigVersion());
        }
        return new TabletBinlogConfig(binlogConfig.getEnable(), binlogConfig.getTtlSeconds(),
                binlogConfig.getMaxBytes(), binlogConfig.getMaxHistoryNums(), false, false, 0);
    }

    public TBinlogConfig toThrift() {
        TBinlogConfig tBinlogConfig = new TBinlogConfig();
        tBinlogConfig.setEnable(enable);
        tBinlogConfig.setTtlSeconds(ttlSeconds);
        tBinlogConfig.setMaxBytes(maxBytes);
        tBinlogConfig.setMaxHistoryNums(maxHistoryNums);
        tBinlogConfig.setBinlogFormat(changelog ? TBinlogFormat.CHANGELOG : TBinlogFormat.STATEMENT_AND_SNAPSHOT);
        tBinlogConfig.setNeedHistoricalValue(needHistoricalValue);
        tBinlogConfig.setConfigVersion(configVersion);
        return tBinlogConfig;
    }

    public OlapFile.BinlogConfigPB toProtobuf() {
        OlapFile.BinlogConfigPB.Builder binlogConfigBuilder = OlapFile.BinlogConfigPB.newBuilder();
        binlogConfigBuilder.setEnable(enable);
        binlogConfigBuilder.setTtlSeconds(ttlSeconds);
        binlogConfigBuilder.setMaxBytes(maxBytes);
        binlogConfigBuilder.setMaxHistoryNums(maxHistoryNums);
        binlogConfigBuilder.setBinlogFormat(changelog
                ? OlapFile.BinlogFormatPB.CHANGELOG : OlapFile.BinlogFormatPB.STATEMENT_AND_SNAPSHOT);
        binlogConfigBuilder.setNeedHistoricalValue(needHistoricalValue);
        binlogConfigBuilder.setConfigVersion(configVersion);
        return binlogConfigBuilder.build();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TabletBinlogConfig)) {
            return false;
        }
        TabletBinlogConfig other = (TabletBinlogConfig) obj;
        return enable == other.enable
                && ttlSeconds == other.ttlSeconds
                && maxBytes == other.maxBytes
                && maxHistoryNums == other.maxHistoryNums
                && changelog == other.changelog
                && needHistoricalValue == other.needHistoricalValue
                && configVersion == other.configVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enable, ttlSeconds, maxBytes, maxHistoryNums, changelog, needHistoricalValue,
                configVersion);
    }

    @Override
    public String toString() {
        return "TabletBinlogConfig{enable=" + enable + ", ttlSeconds=" + ttlSeconds + ", maxBytes=" + maxBytes
                + ", maxHistoryNums=" + maxHistoryNums + ", changelog=" + changelog
                + ", needHistoricalValue=" + needHistoricalValue + ", configVersion=" + configVersion + "}";
    }
}
