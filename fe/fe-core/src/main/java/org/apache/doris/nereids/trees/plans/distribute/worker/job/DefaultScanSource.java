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

import org.apache.doris.planner.ScanNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** DefaultScanSource */
public class DefaultScanSource extends ScanSource {
    // for example:
    //   1. use OlapScanNode(tableName=`tbl1`) to scan with tablet: [tablet 10001, tablet 10002]
    //   2. use OlapScanNode(tableName=`tbl2`) to scan with tablet: [tablet 10003, tablet 10004]
    public final Map<ScanNode, ScanRanges> scanNodeToScanRanges;

    public DefaultScanSource(Map<ScanNode, ScanRanges> scanNodeToScanRanges) {
        this.scanNodeToScanRanges = scanNodeToScanRanges;
    }

    public static DefaultScanSource empty() {
        return new DefaultScanSource(ImmutableMap.of());
    }

    @Override
    public int maxParallel(List<ScanNode> scanNodes) {
        if (scanNodes.size() == 1) {
            ScanRanges scanRanges = scanNodeToScanRanges.get(scanNodes.get(0));
            if (scanRanges != null) {
                // max parallel is the scan ranges(tablets) num
                return Math.max(scanRanges.params.size(), 1);
            }
        }
        return 1;
    }

    @Override
    public List<ScanSource> parallelize(List<ScanNode> scanNodes, int instanceNum) {
        Preconditions.checkArgument(scanNodes.size() == 1,
                "Only support parallelize one ScanNode, but meet " + scanNodes.size() + " ScanNodes");

        ScanNode scanNode = scanNodes.get(0);
        ScanRanges scanRanges = scanNodeToScanRanges.get(scanNode);
        if (scanRanges == null) {
            return ImmutableList.of();
        }

        List<ScanRanges> scanRangesPerInstance = scanRanges.split(instanceNum);

        ImmutableList.Builder<ScanSource> instancesSource
                = ImmutableList.builderWithExpectedSize(scanRangesPerInstance.size());
        for (ScanRanges oneInstanceScanRanges : scanRangesPerInstance) {
            DefaultScanSource oneInstanceScanSource
                    = new DefaultScanSource(ImmutableMap.of(scanNode, oneInstanceScanRanges));
            instancesSource.add(oneInstanceScanSource);
        }
        return instancesSource.build();
    }

    @Override
    public boolean isEmpty() {
        return scanNodeToScanRanges.isEmpty();
    }

    @Override
    public void toString(StringBuilder str, String prefix) {
        toString(scanNodeToScanRanges, str, prefix);
    }

    /** toString */
    public static void toString(Map<ScanNode, ScanRanges> scanNodeToScanRanges, StringBuilder str, String prefix) {
        if (scanNodeToScanRanges.isEmpty()) {
            str.append("[]");
            return;
        }
        int i = 0;
        String nextIndent = prefix + "    ";
        str.append("[\n");
        for (Entry<ScanNode, ScanRanges> entry : scanNodeToScanRanges.entrySet()) {
            ScanNode scanNode = entry.getKey();
            ScanRanges scanRanges = entry.getValue();
            str.append(prefix).append("  {\n")
                    .append(prefix).append("    scanNode: ").append(scanNode).append(",\n")
                    .append(prefix).append("    scanRanges: ");

            scanRanges.toString(str, nextIndent);
            str.append("\n").append(prefix).append("  }");

            if (++i < scanNodeToScanRanges.size()) {
                str.append(",\n");
            }
        }
        str.append("\n").append(prefix).append("]");
    }
}
