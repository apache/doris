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

package org.apache.doris.cloud;

import com.google.gson.JsonObject;

/**
 * Per-job windowed warmup statistics collected from a single BE.
 * Contains requested, finish, and fail counters for segments and indexes
 * across 3 time windows (5m, 30m, 1h).
 */
public class TableWarmUpWindowedStats {

    // requested (source BE populates these)
    public long requestedSegmentNum5m;
    public long requestedSegmentNum30m;
    public long requestedSegmentNum1h;
    public long requestedSegmentSize5m;
    public long requestedSegmentSize30m;
    public long requestedSegmentSize1h;
    public long requestedIndexNum5m;
    public long requestedIndexNum30m;
    public long requestedIndexNum1h;
    public long requestedIndexSize5m;
    public long requestedIndexSize30m;
    public long requestedIndexSize1h;
    public long lastTriggerTs;

    // finish (target BE populates these)
    public long finishSegmentNum5m;
    public long finishSegmentNum30m;
    public long finishSegmentNum1h;
    public long finishSegmentSize5m;
    public long finishSegmentSize30m;
    public long finishSegmentSize1h;
    public long finishIndexNum5m;
    public long finishIndexNum30m;
    public long finishIndexNum1h;
    public long finishIndexSize5m;
    public long finishIndexSize30m;
    public long finishIndexSize1h;
    public long lastFinishTs;

    // fail (target BE populates these)
    public long failSegmentNum5m;
    public long failSegmentNum30m;
    public long failSegmentNum1h;
    public long failSegmentSize5m;
    public long failSegmentSize30m;
    public long failSegmentSize1h;
    public long failIndexNum5m;
    public long failIndexNum30m;
    public long failIndexNum1h;
    public long failIndexSize5m;
    public long failIndexSize30m;
    public long failIndexSize1h;

    /**
     * Parse from BE JSON response.
     * JSON hierarchy: {requested|finish|fail}.{seg|idx}.{num|size}.{5m|30m|1h}
     */
    public static TableWarmUpWindowedStats fromJson(JsonObject obj) {
        TableWarmUpWindowedStats s = new TableWarmUpWindowedStats();

        JsonObject req = obj.getAsJsonObject("requested");
        if (req != null) {
            s.requestedSegmentNum5m = getWindow(req, "seg", "num", "5m");
            s.requestedSegmentNum30m = getWindow(req, "seg", "num", "30m");
            s.requestedSegmentNum1h = getWindow(req, "seg", "num", "1h");
            s.requestedSegmentSize5m = getWindow(req, "seg", "size", "5m");
            s.requestedSegmentSize30m = getWindow(req, "seg", "size", "30m");
            s.requestedSegmentSize1h = getWindow(req, "seg", "size", "1h");
            s.requestedIndexNum5m = getWindow(req, "idx", "num", "5m");
            s.requestedIndexNum30m = getWindow(req, "idx", "num", "30m");
            s.requestedIndexNum1h = getWindow(req, "idx", "num", "1h");
            s.requestedIndexSize5m = getWindow(req, "idx", "size", "5m");
            s.requestedIndexSize30m = getWindow(req, "idx", "size", "30m");
            s.requestedIndexSize1h = getWindow(req, "idx", "size", "1h");
        }

        JsonObject fin = obj.getAsJsonObject("finish");
        if (fin != null) {
            s.finishSegmentNum5m = getWindow(fin, "seg", "num", "5m");
            s.finishSegmentNum30m = getWindow(fin, "seg", "num", "30m");
            s.finishSegmentNum1h = getWindow(fin, "seg", "num", "1h");
            s.finishSegmentSize5m = getWindow(fin, "seg", "size", "5m");
            s.finishSegmentSize30m = getWindow(fin, "seg", "size", "30m");
            s.finishSegmentSize1h = getWindow(fin, "seg", "size", "1h");
            s.finishIndexNum5m = getWindow(fin, "idx", "num", "5m");
            s.finishIndexNum30m = getWindow(fin, "idx", "num", "30m");
            s.finishIndexNum1h = getWindow(fin, "idx", "num", "1h");
            s.finishIndexSize5m = getWindow(fin, "idx", "size", "5m");
            s.finishIndexSize30m = getWindow(fin, "idx", "size", "30m");
            s.finishIndexSize1h = getWindow(fin, "idx", "size", "1h");
        }

        JsonObject fail = obj.getAsJsonObject("fail");
        if (fail != null) {
            s.failSegmentNum5m = getWindow(fail, "seg", "num", "5m");
            s.failSegmentNum30m = getWindow(fail, "seg", "num", "30m");
            s.failSegmentNum1h = getWindow(fail, "seg", "num", "1h");
            s.failSegmentSize5m = getWindow(fail, "seg", "size", "5m");
            s.failSegmentSize30m = getWindow(fail, "seg", "size", "30m");
            s.failSegmentSize1h = getWindow(fail, "seg", "size", "1h");
            s.failIndexNum5m = getWindow(fail, "idx", "num", "5m");
            s.failIndexNum30m = getWindow(fail, "idx", "num", "30m");
            s.failIndexNum1h = getWindow(fail, "idx", "num", "1h");
            s.failIndexSize5m = getWindow(fail, "idx", "size", "5m");
            s.failIndexSize30m = getWindow(fail, "idx", "size", "30m");
            s.failIndexSize1h = getWindow(fail, "idx", "size", "1h");
        }

        s.lastTriggerTs = obj.has("last_trigger_ts") ? obj.get("last_trigger_ts").getAsLong() : 0;
        s.lastFinishTs = obj.has("last_finish_ts") ? obj.get("last_finish_ts").getAsLong() : 0;
        return s;
    }

    private static long getWindow(JsonObject parent, String type, String metric, String window) {
        JsonObject typeObj = parent.getAsJsonObject(type);
        if (typeObj == null) {
            return 0;
        }
        JsonObject metricObj = typeObj.getAsJsonObject(metric);
        if (metricObj == null) {
            return 0;
        }
        return metricObj.has(window) ? metricObj.get(window).getAsLong() : 0;
    }

    /** Merge stats from another BE in the same cluster (additive for counts, max for timestamps). */
    public void merge(TableWarmUpWindowedStats other) {
        requestedSegmentNum5m += other.requestedSegmentNum5m;
        requestedSegmentNum30m += other.requestedSegmentNum30m;
        requestedSegmentNum1h += other.requestedSegmentNum1h;
        requestedSegmentSize5m += other.requestedSegmentSize5m;
        requestedSegmentSize30m += other.requestedSegmentSize30m;
        requestedSegmentSize1h += other.requestedSegmentSize1h;
        requestedIndexNum5m += other.requestedIndexNum5m;
        requestedIndexNum30m += other.requestedIndexNum30m;
        requestedIndexNum1h += other.requestedIndexNum1h;
        requestedIndexSize5m += other.requestedIndexSize5m;
        requestedIndexSize30m += other.requestedIndexSize30m;
        requestedIndexSize1h += other.requestedIndexSize1h;

        finishSegmentNum5m += other.finishSegmentNum5m;
        finishSegmentNum30m += other.finishSegmentNum30m;
        finishSegmentNum1h += other.finishSegmentNum1h;
        finishSegmentSize5m += other.finishSegmentSize5m;
        finishSegmentSize30m += other.finishSegmentSize30m;
        finishSegmentSize1h += other.finishSegmentSize1h;
        finishIndexNum5m += other.finishIndexNum5m;
        finishIndexNum30m += other.finishIndexNum30m;
        finishIndexNum1h += other.finishIndexNum1h;
        finishIndexSize5m += other.finishIndexSize5m;
        finishIndexSize30m += other.finishIndexSize30m;
        finishIndexSize1h += other.finishIndexSize1h;

        failSegmentNum5m += other.failSegmentNum5m;
        failSegmentNum30m += other.failSegmentNum30m;
        failSegmentNum1h += other.failSegmentNum1h;
        failSegmentSize5m += other.failSegmentSize5m;
        failSegmentSize30m += other.failSegmentSize30m;
        failSegmentSize1h += other.failSegmentSize1h;
        failIndexNum5m += other.failIndexNum5m;
        failIndexNum30m += other.failIndexNum30m;
        failIndexNum1h += other.failIndexNum1h;
        failIndexSize5m += other.failIndexSize5m;
        failIndexSize30m += other.failIndexSize30m;
        failIndexSize1h += other.failIndexSize1h;

        lastTriggerTs = Math.max(lastTriggerTs, other.lastTriggerTs);
        lastFinishTs = Math.max(lastFinishTs, other.lastFinishTs);
    }
}
