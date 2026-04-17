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
 * across 3 time windows (5m, 30m, 2h).
 */
public class TableWarmUpWindowedStats {

    // requested (source BE populates these)
    public long requestedSegmentNum5m;
    public long requestedSegmentNum30m;
    public long requestedSegmentNum2h;
    public long requestedSegmentSize5m;
    public long requestedSegmentSize30m;
    public long requestedSegmentSize2h;
    public long requestedIndexNum5m;
    public long requestedIndexNum30m;
    public long requestedIndexNum2h;
    public long requestedIndexSize5m;
    public long requestedIndexSize30m;
    public long requestedIndexSize2h;
    public long lastTriggerTs;

    // finish (target BE populates these)
    public long finishSegmentNum5m;
    public long finishSegmentNum30m;
    public long finishSegmentNum2h;
    public long finishSegmentSize5m;
    public long finishSegmentSize30m;
    public long finishSegmentSize2h;
    public long finishIndexNum5m;
    public long finishIndexNum30m;
    public long finishIndexNum2h;
    public long finishIndexSize5m;
    public long finishIndexSize30m;
    public long finishIndexSize2h;
    public long lastFinishTs;

    // fail (target BE populates these)
    public long failSegmentNum5m;
    public long failSegmentNum30m;
    public long failSegmentNum2h;
    public long failSegmentSize5m;
    public long failSegmentSize30m;
    public long failSegmentSize2h;
    public long failIndexNum5m;
    public long failIndexNum30m;
    public long failIndexNum2h;
    public long failIndexSize5m;
    public long failIndexSize30m;
    public long failIndexSize2h;

    /**
     * Parse from BE JSON response.
     * JSON hierarchy: {requested|finish|fail}.{seg|idx}.{num|size}.{5m|30m|2h}
     */
    public static TableWarmUpWindowedStats fromJson(JsonObject obj) {
        TableWarmUpWindowedStats s = new TableWarmUpWindowedStats();

        JsonObject req = obj.getAsJsonObject("requested");
        if (req != null) {
            s.requestedSegmentNum5m = getWindow(req, "seg", "num", "5m");
            s.requestedSegmentNum30m = getWindow(req, "seg", "num", "30m");
            s.requestedSegmentNum2h = getWindow(req, "seg", "num", "2h");
            s.requestedSegmentSize5m = getWindow(req, "seg", "size", "5m");
            s.requestedSegmentSize30m = getWindow(req, "seg", "size", "30m");
            s.requestedSegmentSize2h = getWindow(req, "seg", "size", "2h");
            s.requestedIndexNum5m = getWindow(req, "idx", "num", "5m");
            s.requestedIndexNum30m = getWindow(req, "idx", "num", "30m");
            s.requestedIndexNum2h = getWindow(req, "idx", "num", "2h");
            s.requestedIndexSize5m = getWindow(req, "idx", "size", "5m");
            s.requestedIndexSize30m = getWindow(req, "idx", "size", "30m");
            s.requestedIndexSize2h = getWindow(req, "idx", "size", "2h");
        }

        JsonObject fin = obj.getAsJsonObject("finish");
        if (fin != null) {
            s.finishSegmentNum5m = getWindow(fin, "seg", "num", "5m");
            s.finishSegmentNum30m = getWindow(fin, "seg", "num", "30m");
            s.finishSegmentNum2h = getWindow(fin, "seg", "num", "2h");
            s.finishSegmentSize5m = getWindow(fin, "seg", "size", "5m");
            s.finishSegmentSize30m = getWindow(fin, "seg", "size", "30m");
            s.finishSegmentSize2h = getWindow(fin, "seg", "size", "2h");
            s.finishIndexNum5m = getWindow(fin, "idx", "num", "5m");
            s.finishIndexNum30m = getWindow(fin, "idx", "num", "30m");
            s.finishIndexNum2h = getWindow(fin, "idx", "num", "2h");
            s.finishIndexSize5m = getWindow(fin, "idx", "size", "5m");
            s.finishIndexSize30m = getWindow(fin, "idx", "size", "30m");
            s.finishIndexSize2h = getWindow(fin, "idx", "size", "2h");
        }

        JsonObject fail = obj.getAsJsonObject("fail");
        if (fail != null) {
            s.failSegmentNum5m = getWindow(fail, "seg", "num", "5m");
            s.failSegmentNum30m = getWindow(fail, "seg", "num", "30m");
            s.failSegmentNum2h = getWindow(fail, "seg", "num", "2h");
            s.failSegmentSize5m = getWindow(fail, "seg", "size", "5m");
            s.failSegmentSize30m = getWindow(fail, "seg", "size", "30m");
            s.failSegmentSize2h = getWindow(fail, "seg", "size", "2h");
            s.failIndexNum5m = getWindow(fail, "idx", "num", "5m");
            s.failIndexNum30m = getWindow(fail, "idx", "num", "30m");
            s.failIndexNum2h = getWindow(fail, "idx", "num", "2h");
            s.failIndexSize5m = getWindow(fail, "idx", "size", "5m");
            s.failIndexSize30m = getWindow(fail, "idx", "size", "30m");
            s.failIndexSize2h = getWindow(fail, "idx", "size", "2h");
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
        requestedSegmentNum2h += other.requestedSegmentNum2h;
        requestedSegmentSize5m += other.requestedSegmentSize5m;
        requestedSegmentSize30m += other.requestedSegmentSize30m;
        requestedSegmentSize2h += other.requestedSegmentSize2h;
        requestedIndexNum5m += other.requestedIndexNum5m;
        requestedIndexNum30m += other.requestedIndexNum30m;
        requestedIndexNum2h += other.requestedIndexNum2h;
        requestedIndexSize5m += other.requestedIndexSize5m;
        requestedIndexSize30m += other.requestedIndexSize30m;
        requestedIndexSize2h += other.requestedIndexSize2h;

        finishSegmentNum5m += other.finishSegmentNum5m;
        finishSegmentNum30m += other.finishSegmentNum30m;
        finishSegmentNum2h += other.finishSegmentNum2h;
        finishSegmentSize5m += other.finishSegmentSize5m;
        finishSegmentSize30m += other.finishSegmentSize30m;
        finishSegmentSize2h += other.finishSegmentSize2h;
        finishIndexNum5m += other.finishIndexNum5m;
        finishIndexNum30m += other.finishIndexNum30m;
        finishIndexNum2h += other.finishIndexNum2h;
        finishIndexSize5m += other.finishIndexSize5m;
        finishIndexSize30m += other.finishIndexSize30m;
        finishIndexSize2h += other.finishIndexSize2h;

        failSegmentNum5m += other.failSegmentNum5m;
        failSegmentNum30m += other.failSegmentNum30m;
        failSegmentNum2h += other.failSegmentNum2h;
        failSegmentSize5m += other.failSegmentSize5m;
        failSegmentSize30m += other.failSegmentSize30m;
        failSegmentSize2h += other.failSegmentSize2h;
        failIndexNum5m += other.failIndexNum5m;
        failIndexNum30m += other.failIndexNum30m;
        failIndexNum2h += other.failIndexNum2h;
        failIndexSize5m += other.failIndexSize5m;
        failIndexSize30m += other.failIndexSize30m;
        failIndexSize2h += other.failIndexSize2h;

        lastTriggerTs = Math.max(lastTriggerTs, other.lastTriggerTs);
        lastFinishTs = Math.max(lastFinishTs, other.lastFinishTs);
    }
}
