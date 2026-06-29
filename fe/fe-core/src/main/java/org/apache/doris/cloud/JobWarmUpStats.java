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

import org.apache.doris.monitor.unit.ByteSizeValue;

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
    private static final DateTimeFormatter DATETIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Aggregated requested
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

    // Aggregated finished
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

    // Aggregated failed
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
    public long lastFinishTs;
    // Aggregated from target BEs. FE takes the minimum positive target progress watermark so the
    // slowest target BE decides how far the job has caught up to source-side triggers.
    public long progressTriggerTs;

    // gap = requested - finished
    public long gapSegmentNum5m;
    public long gapSegmentNum30m;
    public long gapSegmentNum1h;
    public long gapSegmentSize5m;
    public long gapSegmentSize30m;
    public long gapSegmentSize1h;
    public long gapIndexNum5m;
    public long gapIndexNum30m;
    public long gapIndexNum1h;
    public long gapIndexSize5m;
    public long gapIndexSize30m;
    public long gapIndexSize1h;
    // Source last trigger timestamp minus target progress watermark. A caught-up target reports its
    // latest finished trigger as progress, so this value naturally becomes 0.
    public long triggerGapMs;

    /** Accumulate requested stats from a table in the source cluster. */
    public void mergeRequested(TableWarmUpWindowedStats t) {
        requestedSegmentNum5m += t.requestedSegmentNum5m;
        requestedSegmentNum30m += t.requestedSegmentNum30m;
        requestedSegmentNum1h += t.requestedSegmentNum1h;
        requestedSegmentSize5m += t.requestedSegmentSize5m;
        requestedSegmentSize30m += t.requestedSegmentSize30m;
        requestedSegmentSize1h += t.requestedSegmentSize1h;
        requestedIndexNum5m += t.requestedIndexNum5m;
        requestedIndexNum30m += t.requestedIndexNum30m;
        requestedIndexNum1h += t.requestedIndexNum1h;
        requestedIndexSize5m += t.requestedIndexSize5m;
        requestedIndexSize30m += t.requestedIndexSize30m;
        requestedIndexSize1h += t.requestedIndexSize1h;
        lastTriggerTs = Math.max(lastTriggerTs, t.lastTriggerTs);
    }

    /** Accumulate finished/failed stats from a table in the target cluster. */
    public void mergeFinished(TableWarmUpWindowedStats t) {
        finishSegmentNum5m += t.finishSegmentNum5m;
        finishSegmentNum30m += t.finishSegmentNum30m;
        finishSegmentNum1h += t.finishSegmentNum1h;
        finishSegmentSize5m += t.finishSegmentSize5m;
        finishSegmentSize30m += t.finishSegmentSize30m;
        finishSegmentSize1h += t.finishSegmentSize1h;
        finishIndexNum5m += t.finishIndexNum5m;
        finishIndexNum30m += t.finishIndexNum30m;
        finishIndexNum1h += t.finishIndexNum1h;
        finishIndexSize5m += t.finishIndexSize5m;
        finishIndexSize30m += t.finishIndexSize30m;
        finishIndexSize1h += t.finishIndexSize1h;
        failSegmentNum5m += t.failSegmentNum5m;
        failSegmentNum30m += t.failSegmentNum30m;
        failSegmentNum1h += t.failSegmentNum1h;
        failSegmentSize5m += t.failSegmentSize5m;
        failSegmentSize30m += t.failSegmentSize30m;
        failSegmentSize1h += t.failSegmentSize1h;
        failIndexNum5m += t.failIndexNum5m;
        failIndexNum30m += t.failIndexNum30m;
        failIndexNum1h += t.failIndexNum1h;
        failIndexSize5m += t.failIndexSize5m;
        failIndexSize30m += t.failIndexSize30m;
        failIndexSize1h += t.failIndexSize1h;
        lastFinishTs = Math.max(lastFinishTs, t.lastFinishTs);
        progressTriggerTs = minPositive(progressTriggerTs, t.progressTriggerTs);
    }

    /** Compute gap = requested - finished for all window/metric combinations. */
    public void computeGap() {
        gapSegmentNum5m = requestedSegmentNum5m - finishSegmentNum5m;
        gapSegmentNum30m = requestedSegmentNum30m - finishSegmentNum30m;
        gapSegmentNum1h = requestedSegmentNum1h - finishSegmentNum1h;
        gapSegmentSize5m = requestedSegmentSize5m - finishSegmentSize5m;
        gapSegmentSize30m = requestedSegmentSize30m - finishSegmentSize30m;
        gapSegmentSize1h = requestedSegmentSize1h - finishSegmentSize1h;
        gapIndexNum5m = requestedIndexNum5m - finishIndexNum5m;
        gapIndexNum30m = requestedIndexNum30m - finishIndexNum30m;
        gapIndexNum1h = requestedIndexNum1h - finishIndexNum1h;
        gapIndexSize5m = requestedIndexSize5m - finishIndexSize5m;
        gapIndexSize30m = requestedIndexSize30m - finishIndexSize30m;
        gapIndexSize1h = requestedIndexSize1h - finishIndexSize1h;
        triggerGapMs = lastTriggerTs > 0 && progressTriggerTs > 0
                ? Math.max(0, lastTriggerTs - progressTriggerTs) : 0;
    }

    /** Serialize compact 30m SyncStats summary for SHOW WARM UP JOB list output. */
    public String toSummaryJsonString() {
        JsonObject root = new JsonObject();
        root.addProperty("window", "30m");
        long srcSize = requestedSegmentSize30m + requestedIndexSize30m;
        long dstSize = finishSegmentSize30m + finishIndexSize30m;
        root.addProperty("src_size", humanReadableSize(srcSize));
        root.addProperty("dst_size", humanReadableSize(dstSize));
        root.addProperty("gap_size", humanReadableSize(srcSize - dstSize));
        // Compact SHOW WARM UP JOB output still exposes the active incremental warm-up time lag.
        root.addProperty("trigger_gap_ms", triggerGapMs);
        return root.toString();
    }

    /** Serialize detailed SyncStats JSON for SHOW WARM UP JOB WHERE ID = ... output. */
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
        segNum.addProperty("requested_1h", requestedSegmentNum1h);
        segNum.addProperty("finish_1h", finishSegmentNum1h);
        segNum.addProperty("gap_1h", gapSegmentNum1h);
        segNum.addProperty("fail_1h", failSegmentNum1h);
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
        segSize.addProperty("requested_1h", humanReadableSize(requestedSegmentSize1h));
        segSize.addProperty("finish_1h", humanReadableSize(finishSegmentSize1h));
        segSize.addProperty("gap_1h", humanReadableSize(gapSegmentSize1h));
        segSize.addProperty("fail_1h", humanReadableSize(failSegmentSize1h));
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
        idxNum.addProperty("requested_1h", requestedIndexNum1h);
        idxNum.addProperty("finish_1h", finishIndexNum1h);
        idxNum.addProperty("gap_1h", gapIndexNum1h);
        idxNum.addProperty("fail_1h", failIndexNum1h);
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
        idxSize.addProperty("requested_1h", humanReadableSize(requestedIndexSize1h));
        idxSize.addProperty("finish_1h", humanReadableSize(finishIndexSize1h));
        idxSize.addProperty("gap_1h", humanReadableSize(gapIndexSize1h));
        idxSize.addProperty("fail_1h", humanReadableSize(failIndexSize1h));
        root.add("idx_size", idxSize);

        // timestamps
        root.addProperty("last_trigger_ts", formatEpochMs(lastTriggerTs));
        root.addProperty("last_finish_ts", formatEpochMs(lastFinishTs));
        root.addProperty("progress_trigger_ts", formatEpochMs(progressTriggerTs));
        root.addProperty("trigger_gap_ms", triggerGapMs);

        return root.toString();
    }

    private static long minPositive(long current, long candidate) {
        if (current <= 0) {
            return Math.max(candidate, 0);
        }
        if (candidate <= 0) {
            return current;
        }
        return Math.min(current, candidate);
    }

    private static String humanReadableSize(long bytes) {
        if (bytes < 0) {
            return "-" + new ByteSizeValue(-bytes).toString();
        }
        return new ByteSizeValue(bytes).toString();
    }

    private static String formatEpochMs(long epochMs) {
        if (epochMs <= 0) {
            return "";
        }
        try {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneId.systemDefault())
                    .format(DATETIME_FMT);
        } catch (Exception e) {
            return "";
        }
    }
}
