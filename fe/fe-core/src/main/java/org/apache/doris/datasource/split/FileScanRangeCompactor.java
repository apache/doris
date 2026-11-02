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

package org.apache.doris.datasource.split;

import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reduces repeated file-scan wrappers only at the transport boundary, after scan assignment. */
public final class FileScanRangeCompactor {
    private FileScanRangeCompactor() {
    }

    /**
     * Compact only after fetching: maxBatchSize counts the original split descriptions, not envelopes.
     * Every range comes from FileQueryScanNode.splitToScanRange, so file-specific attributes remain in
     * TFileRangeDesc. Keep adjacent envelopes separate when their scan parameters or locations differ.
     */
    public static List<TScanRangeLocations> compactSplitBatch(List<TScanRangeLocations> scanRanges) {
        if (scanRanges.size() < 2) {
            return scanRanges;
        }
        List<TScanRangeLocations> compacted = new ArrayList<>();
        TScanRangeLocations previous = null;
        TFileScanRange previousFiles = null;
        boolean copied = false;
        for (TScanRangeLocations current : scanRanges) {
            TFileScanRange files = current.getScanRange().getExtScanRange().getFileScanRange();
            if (previous != null
                    && Objects.equals(previous.getLocations(), current.getLocations())
                    && canMerge(previousFiles, files)) {
                if (!copied) {
                    // Keep compaction local to this response; leave the dequeued entries unchanged.
                    previous = new TScanRangeLocations(previous);
                    previousFiles = previous.getScanRange().getExtScanRange().getFileScanRange();
                    compacted.set(compacted.size() - 1, previous);
                    copied = true;
                }
                previousFiles.getRanges().addAll(files.getRanges());
            } else {
                compacted.add(current);
                previous = current;
                previousFiles = files;
                copied = false;
            }
        }
        return compacted;
    }

    /**
     * Compact synchronous file scans within each already assigned instance. BE limits the number of
     * local scanners by the number of wrappers, so retain at least its scanner-concurrency upper bound:
     * max_file_scanners_concurrency * parallel_instance (the BE may divide this by operator parallelism).
     * Non-file scans and the assignment's original lists are left unchanged.
     */
    public static Map<Integer, List<TScanRangeParams>> compactInstanceScanRanges(
            Map<Integer, List<TScanRangeParams>> scanRanges,
            Map<Integer, TFileScanRangeParams> fileScanParams, TQueryOptions options) {
        if (fileScanParams.isEmpty()) {
            return scanRanges;
        }
        int scannersPerInstance = options.getMaxFileScannersConcurrency() > 0
                ? options.getMaxFileScannersConcurrency() : 16;
        long minimumWrappers = (long) scannersPerInstance * options.getParallelInstance();
        Map<Integer, List<TScanRangeParams>> compacted = new LinkedHashMap<>();
        for (Map.Entry<Integer, List<TScanRangeParams>> entry : scanRanges.entrySet()) {
            List<TScanRangeParams> ranges = entry.getValue();
            compacted.put(entry.getKey(), fileScanParams.containsKey(entry.getKey())
                    ? compactAssignedRanges(ranges, minimumWrappers) : ranges);
        }
        return compacted;
    }

    private static List<TScanRangeParams> compactAssignedRanges(
            List<TScanRangeParams> ranges, long minimumWrappers) {
        // Floor division ensures compaction never leaves fewer wrappers than the BE can scan in parallel.
        int maximumGroupSize = (int) Math.max(1, ranges.size() / minimumWrappers);
        if (maximumGroupSize == 1) {
            return ranges;
        }
        List<TScanRangeParams> compacted = new ArrayList<>();
        TScanRangeParams previous = null;
        TFileScanRange previousFiles = null;
        int groupSize = 0;
        for (TScanRangeParams current : ranges) {
            TFileScanRange files = current.getScanRange().getExtScanRange().getFileScanRange();
            if (previous != null && groupSize < maximumGroupSize
                    && previous.isSetVolumeId() == current.isSetVolumeId()
                    && previous.getVolumeId() == current.getVolumeId()
                    && canMerge(previousFiles, files)) {
                if (groupSize == 1) {
                    previous = new TScanRangeParams(previous);
                    previousFiles = previous.getScanRange().getExtScanRange().getFileScanRange();
                    compacted.set(compacted.size() - 1, previous);
                }
                previousFiles.getRanges().addAll(files.getRanges());
                groupSize++;
            } else {
                compacted.add(current);
                previous = current;
                previousFiles = files;
                groupSize = 1;
            }
        }
        return compacted;
    }

    private static boolean canMerge(TFileScanRange left, TFileScanRange right) {
        // A split-source reference is a distinct stream, not a list of file ranges to concatenate.
        return !left.isSetSplitSource() && !right.isSetSplitSource()
                && Objects.equals(left.getParams(), right.getParams());
    }
}
