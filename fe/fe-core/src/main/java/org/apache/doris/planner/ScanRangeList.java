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

package org.apache.doris.planner;

import org.apache.doris.thrift.TScanRange;

import lombok.Data;

import java.util.List;

/**
 * This class represents all the scan ranges for a scan node (including external scan node, olap scan node etc.)
 * Each element in scanRanges is a range of the scan task. For example, a tablet for OlapScanNode,
 * one or several file blocks for ExternalFileScanNode.
 * ScanNode need to generate this scanRanges during init or finalize stage,
 * and the Coordinator will get the scanRanges generated and assign each TScanRange a BE node to execute it.
 */
@Data
public class ScanRangeList {
    private List<TScanRange> scanRanges;

    public void addToScanRanges(TScanRange range) {
        if (scanRanges == null) {
            scanRanges = new java.util.ArrayList<>();
        }
        scanRanges.add(range);
    }

    public int getScanRangeSize() {
        if (scanRanges == null) {
            return 0;
        }
        return scanRanges.size();
    }
}
