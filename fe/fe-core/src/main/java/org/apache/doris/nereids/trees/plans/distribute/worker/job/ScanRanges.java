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

package org.apache.doris.nereids.trees.plans.distribute.worker.job;

import org.apache.doris.thrift.TPaloScanRange;
import org.apache.doris.thrift.TScanRangeParams;

import com.google.common.collect.Lists;

import java.util.List;

/**ScanRanges */
public class ScanRanges implements Splittable<ScanRanges> {
    // usually, it's tablets
    public final List<TScanRangeParams> params;
    // size corresponding to tablets one by one
    public final List<Long> bytes;
    public long totalBytes;

    public ScanRanges() {
        this(Lists.newArrayList(), Lists.newArrayList());
    }

    /** ScanRanges */
    public ScanRanges(List<TScanRangeParams> params, List<Long> bytes) {
        this.params = params;
        this.bytes = bytes;
        long totalBytes = 0;
        for (Long size : bytes) {
            totalBytes += size;
        }
        this.totalBytes = totalBytes;
    }

    public void addScanRanges(ScanRanges scanRanges) {
        this.params.addAll(scanRanges.params);
        this.bytes.addAll(scanRanges.bytes);
        this.totalBytes += scanRanges.totalBytes;
    }

    public void addScanRange(TScanRangeParams params, long bytes) {
        this.params.add(params);
        this.bytes.add(bytes);
        this.totalBytes += bytes;
    }

    @Override
    public int itemSize() {
        return params.size();
    }

    @Override
    public void addItem(ScanRanges other, int index) {
        addScanRange(other.params.get(index), other.bytes.get(index));
    }

    @Override
    public ScanRanges newSplittable() {
        return new ScanRanges();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        toString(str, "");
        return str.toString();
    }

    /** toString */
    public void toString(StringBuilder str, String prefix) {
        str.append("ScanRanges(bytes: " + totalBytes + ", ranges: [");
        if (params.isEmpty()) {
            str.append("])");
            return;
        }
        str.append("\n");
        for (int i = 0; i < params.size(); i++) {
            str.append(prefix).append("  " + toString(params.get(i)) + ", bytes: " + bytes.get(i));
            if (i + 1 < params.size()) {
                str.append(",\n");
            }
        }
        str.append("\n").append(prefix).append("])");
    }

    private String toString(TScanRangeParams scanRange) {
        TPaloScanRange paloScanRange = scanRange.getScanRange().getPaloScanRange();
        if (paloScanRange != null) {
            return "tablet " + paloScanRange.getTabletId();
        } else {
            return scanRange.toString();
        }
    }
}
