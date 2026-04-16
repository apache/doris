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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Per-Job aggregated warmup statistics.
 * Aggregates requested (from source cluster) and finished/failed (from target cluster)
 * across all matched tables, then computes gap = requested - finished.
 */
public class JobWarmUpStats {
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    // Aggregated requested
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

    // Aggregated finished
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

    // Aggregated failed
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
    public long lastFinishTs;

    // gap = requested - finished
    public long gapSegmentNum5m;
    public long gapSegmentNum30m;
    public long gapSegmentNum2h;
    public long gapSegmentSize5m;
    public long gapSegmentSize30m;
    public long gapSegmentSize2h;
    public long gapIndexNum5m;
    public long gapIndexNum30m;
    public long gapIndexNum2h;
    public long gapIndexSize5m;
    public long gapIndexSize30m;
    public long gapIndexSize2h;

    /** Accumulate requested stats from a table in the source cluster. */
    public void mergeRequested(TableWarmUpWindowedStats t) {
        requestedSegmentNum5m += t.requestedSegmentNum5m;
        requestedSegmentNum30m += t.requestedSegmentNum30m;
        requestedSegmentNum2h += t.requestedSegmentNum2h;
        requestedSegmentSize5m += t.requestedSegmentSize5m;
        requestedSegmentSize30m += t.requestedSegmentSize30m;
        requestedSegmentSize2h += t.requestedSegmentSize2h;
        requestedIndexNum5m += t.requestedIndexNum5m;
        requestedIndexNum30m += t.requestedIndexNum30m;
        requestedIndexNum2h += t.requestedIndexNum2h;
        requestedIndexSize5m += t.requestedIndexSize5m;
        requestedIndexSize30m += t.requestedIndexSize30m;
        requestedIndexSize2h += t.requestedIndexSize2h;
        lastTriggerTs = Math.max(lastTriggerTs, t.lastTriggerTs);
    }

    /** Accumulate finished/failed stats from a table in the target cluster. */
    public void mergeFinished(TableWarmUpWindowedStats t) {
        finishSegmentNum5m += t.finishSegmentNum5m;
        finishSegmentNum30m += t.finishSegmentNum30m;
        finishSegmentNum2h += t.finishSegmentNum2h;
        finishSegmentSize5m += t.finishSegmentSize5m;
        finishSegmentSize30m += t.finishSegmentSize30m;
        finishSegmentSize2h += t.finishSegmentSize2h;
        finishIndexNum5m += t.finishIndexNum5m;
        finishIndexNum30m += t.finishIndexNum30m;
        finishIndexNum2h += t.finishIndexNum2h;
        finishIndexSize5m += t.finishIndexSize5m;
        finishIndexSize30m += t.finishIndexSize30m;
        finishIndexSize2h += t.finishIndexSize2h;
        failSegmentNum5m += t.failSegmentNum5m;
        failSegmentNum30m += t.failSegmentNum30m;
        failSegmentNum2h += t.failSegmentNum2h;
        failSegmentSize5m += t.failSegmentSize5m;
        failSegmentSize30m += t.failSegmentSize30m;
        failSegmentSize2h += t.failSegmentSize2h;
        failIndexNum5m += t.failIndexNum5m;
        failIndexNum30m += t.failIndexNum30m;
        failIndexNum2h += t.failIndexNum2h;
        failIndexSize5m += t.failIndexSize5m;
        failIndexSize30m += t.failIndexSize30m;
        failIndexSize2h += t.failIndexSize2h;
        lastFinishTs = Math.max(lastFinishTs, t.lastFinishTs);
    }

    /** Compute gap = requested - finished for all window/metric combinations. */
    public void computeGap() {
        gapSegmentNum5m = requestedSegmentNum5m - finishSegmentNum5m;
        gapSegmentNum30m = requestedSegmentNum30m - finishSegmentNum30m;
        gapSegmentNum2h = requestedSegmentNum2h - finishSegmentNum2h;
        gapSegmentSize5m = requestedSegmentSize5m - finishSegmentSize5m;
        gapSegmentSize30m = requestedSegmentSize30m - finishSegmentSize30m;
        gapSegmentSize2h = requestedSegmentSize2h - finishSegmentSize2h;
        gapIndexNum5m = requestedIndexNum5m - finishIndexNum5m;
        gapIndexNum30m = requestedIndexNum30m - finishIndexNum30m;
        gapIndexNum2h = requestedIndexNum2h - finishIndexNum2h;
        gapIndexSize5m = requestedIndexSize5m - finishIndexSize5m;
        gapIndexSize30m = requestedIndexSize30m - finishIndexSize30m;
        gapIndexSize2h = requestedIndexSize2h - finishIndexSize2h;
    }

    /** Serialize to SyncStats JSON for SHOW WARM UP JOB output. */
    public String toJsonString() {
        JsonObject root = new JsonObject();

        // seg_num
        JsonObject segNum = new JsonObject();
        segNum.addProperty("requested_5m", requestedSegmentNum5m);
        segNum.addProperty("finish_5m", finishSegmentNum5m);
        segNum.addProperty("gap_5m", gapSegmentNum5m);
        segNum.addProperty("fail_5m", failSegmentNum5m);
        segNum.addProperty("requested_30m", requestedSegmentNum30m);
        segNum.addProperty("finish_30m", finishSegmentNum30m);
        segNum.addProperty("gap_30m", gapSegmentNum30m);
        segNum.addProperty("fail_30m", failSegmentNum30m);
        segNum.addProperty("requested_2h", requestedSegmentNum2h);
        segNum.addProperty("finish_2h", finishSegmentNum2h);
        segNum.addProperty("gap_2h", gapSegmentNum2h);
        segNum.addProperty("fail_2h", failSegmentNum2h);
        root.add("seg_num", segNum);

        // seg_size
        JsonObject segSize = new JsonObject();
        segSize.addProperty("requested_5m", humanReadableSize(requestedSegmentSize5m));
        segSize.addProperty("finish_5m", humanReadableSize(finishSegmentSize5m));
        segSize.addProperty("gap_5m", humanReadableSize(gapSegmentSize5m));
        segSize.addProperty("fail_5m", humanReadableSize(failSegmentSize5m));
        segSize.addProperty("requested_30m", humanReadableSize(requestedSegmentSize30m));
        segSize.addProperty("finish_30m", humanReadableSize(finishSegmentSize30m));
        segSize.addProperty("gap_30m", humanReadableSize(gapSegmentSize30m));
        segSize.addProperty("fail_30m", humanReadableSize(failSegmentSize30m));
        segSize.addProperty("requested_2h", humanReadableSize(requestedSegmentSize2h));
        segSize.addProperty("finish_2h", humanReadableSize(finishSegmentSize2h));
        segSize.addProperty("gap_2h", humanReadableSize(gapSegmentSize2h));
        segSize.addProperty("fail_2h", humanReadableSize(failSegmentSize2h));
        root.add("seg_size", segSize);

        // idx_num
        JsonObject idxNum = new JsonObject();
        idxNum.addProperty("requested_5m", requestedIndexNum5m);
        idxNum.addProperty("finish_5m", finishIndexNum5m);
        idxNum.addProperty("gap_5m", gapIndexNum5m);
        idxNum.addProperty("fail_5m", failIndexNum5m);
        idxNum.addProperty("requested_30m", requestedIndexNum30m);
        idxNum.addProperty("finish_30m", finishIndexNum30m);
        idxNum.addProperty("gap_30m", gapIndexNum30m);
        idxNum.addProperty("fail_30m", failIndexNum30m);
        idxNum.addProperty("requested_2h", requestedIndexNum2h);
        idxNum.addProperty("finish_2h", finishIndexNum2h);
        idxNum.addProperty("gap_2h", gapIndexNum2h);
        idxNum.addProperty("fail_2h", failIndexNum2h);
        root.add("idx_num", idxNum);

        // idx_size
        JsonObject idxSize = new JsonObject();
        idxSize.addProperty("requested_5m", humanReadableSize(requestedIndexSize5m));
        idxSize.addProperty("finish_5m", humanReadableSize(finishIndexSize5m));
        idxSize.addProperty("gap_5m", humanReadableSize(gapIndexSize5m));
        idxSize.addProperty("fail_5m", humanReadableSize(failIndexSize5m));
        idxSize.addProperty("requested_30m", humanReadableSize(requestedIndexSize30m));
        idxSize.addProperty("finish_30m", humanReadableSize(finishIndexSize30m));
        idxSize.addProperty("gap_30m", humanReadableSize(gapIndexSize30m));
        idxSize.addProperty("fail_30m", humanReadableSize(failIndexSize30m));
        idxSize.addProperty("requested_2h", humanReadableSize(requestedIndexSize2h));
        idxSize.addProperty("finish_2h", humanReadableSize(finishIndexSize2h));
        idxSize.addProperty("gap_2h", humanReadableSize(gapIndexSize2h));
        idxSize.addProperty("fail_2h", humanReadableSize(failIndexSize2h));
        root.add("idx_size", idxSize);

        // timestamps
        root.addProperty("last_trigger_ts", formatEpochMs(lastTriggerTs));
        root.addProperty("last_finish_ts", formatEpochMs(lastFinishTs));

        return root.toString();
    }

    private static String humanReadableSize(long bytes) {
        if (bytes < 0) {
            return "-" + humanReadableSize(-bytes);
        }
        if (bytes < 1024) {
            return bytes + " B";
        }
        double kb = bytes / 1024.0;
        if (kb < 1024) {
            return String.format("%.1f KB", kb);
        }
        double mb = kb / 1024.0;
        if (mb < 1024) {
            return String.format("%.1f MB", mb);
        }
        double gb = mb / 1024.0;
        if (gb < 1024) {
            return String.format("%.1f GB", gb);
        }
        double tb = gb / 1024.0;
        return String.format("%.1f TB", tb);
    }

    private static String formatEpochMs(long epochMs) {
        if (epochMs <= 0) {
            return "";
        }
        try {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneId.systemDefault())
                    .format(TIME_FMT);
        } catch (Exception e) {
            return "";
        }
    }
}
