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

package org.apache.doris.load;

import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.sparkdpp.DppResult;
import org.apache.doris.thrift.TEtlState;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class EtlStatus implements Writable {
    public static final String DEFAULT_TRACKING_URL = FeConstants.null_string;
    @SerializedName(value = "s")
    private TEtlState state;
    @SerializedName(value = "tu")
    private String trackingUrl;
    @SerializedName(value = "st")
    private Map<String, String> stats;
    @SerializedName(value = "c")
    private Map<String, String> counters;
    // not persist
    private Map<String, Long> fileMap;

    // for spark not persist
    // 0 - 100
    private int progress;
    private String failMsg;
    private DppResult dppResult;

    public EtlStatus() {
        this.state = TEtlState.RUNNING;
        this.trackingUrl = DEFAULT_TRACKING_URL;
        this.stats = Maps.newHashMap();
        this.counters = Maps.newHashMap();
        this.fileMap = Maps.newHashMap();
        this.progress = 0;
        this.failMsg = "";
        this.dppResult = null;
    }

    public TEtlState getState() {
        return state;
    }

    public boolean setState(TEtlState state) {
        // running -> finished or cancelled
        if (this.state != TEtlState.RUNNING) {
            return false;
        }
        this.state = state;
        return true;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = Strings.nullToEmpty(trackingUrl);
    }

    public Map<String, String> getStats() {
        return stats;
    }

    public void setStats(Map<String, String> stats) {
        this.stats = stats;
    }

    public Map<String, String> getCounters() {
        return counters;
    }

    public void replaceCounter(String key, String value) {
        counters.put(key, value);
    }

    public void setCounters(Map<String, String> counters) {
        this.counters = counters;
    }

    public Map<String, Long> getFileMap() {
        return fileMap;
    }

    public void setFileMap(Map<String, Long> fileMap) {
        this.fileMap = fileMap;
    }

    public void addAllFileMap(Map<String, Long> fileMap) {
        this.fileMap.putAll(fileMap);
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public String getFailMsg() {
        return failMsg;
    }

    public void setFailMsg(String failMsg) {
        this.failMsg = failMsg;
    }

    public DppResult getDppResult() {
        return dppResult;
    }

    public void setDppResult(DppResult dppResult) {
        this.dppResult = dppResult;
    }

    public void reset() {
        this.stats.clear();
        this.counters.clear();
        this.fileMap.clear();
        this.progress = 0;
        this.failMsg = "";
        this.dppResult = null;
    }

    @Override
    public String toString() {
        return "EtlStatus{"
                + "state=" + state
                + ", trackingUrl='" + trackingUrl + '\''
                + ", stats=" + stats
                + ", counters=" + counters
                + ", fileMap=" + fileMap
                + ", progress=" + progress
                + ", failMsg='" + failMsg + '\''
                + ", dppResult='" + dppResult + '\''
                + '}';
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, state.name());
        Text.writeString(out, trackingUrl);

        int statsCount = (stats == null) ? 0 : stats.size();
        out.writeInt(statsCount);
        for (Map.Entry<String, String> entry : stats.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }

        int countersCount = (counters == null) ? 0 : counters.size();
        out.writeInt(countersCount);
        for (Map.Entry<String, String> entry : counters.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
    }

    public void readFields(DataInput in) throws IOException {
        state = TEtlState.valueOf(Text.readString(in));
        trackingUrl = Text.readString(in);

        int statsCount = in.readInt();
        for (int i = 0; i < statsCount; ++i) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            stats.put(key, value);
        }

        int countersCount = in.readInt();
        for (int i = 0; i < countersCount; ++i) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            counters.put(key, value);
        }
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof EtlStatus)) {
            return false;
        }

        EtlStatus etlTaskStatus = (EtlStatus) obj;

        // Check stats
        if (etlTaskStatus.stats == null) {
            return false;
        }
        if (stats.size() != etlTaskStatus.stats.size()) {
            return false;
        }
        for (Entry<String, String> entry : stats.entrySet()) {
            String key = entry.getKey();
            if (!etlTaskStatus.stats.containsKey(key)) {
                return false;
            }
            if (!entry.getValue().equals(etlTaskStatus.stats.get(key))) {
                return false;
            }
        }

        // Check counters
        if (etlTaskStatus.counters == null) {
            return false;
        }
        if (counters.size() != etlTaskStatus.counters.size()) {
            return false;
        }
        for (Entry<String, String> entry : counters.entrySet()) {
            String key = entry.getKey();
            if (!etlTaskStatus.counters.containsKey(key)) {
                return false;
            }
            if (!entry.getValue().equals(etlTaskStatus.counters.get(key))) {
                return false;
            }
        }

        return state.equals(etlTaskStatus.state) && trackingUrl.equals(etlTaskStatus.trackingUrl);
    }
}
