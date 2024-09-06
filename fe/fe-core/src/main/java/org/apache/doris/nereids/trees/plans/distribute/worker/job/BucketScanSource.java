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

import org.apache.doris.common.util.ListUtil;
import org.apache.doris.planner.ScanNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** BucketScanSource */
public class BucketScanSource extends ScanSource {
    // for example:
    //   1. bucket 0 use OlapScanNode(tableName=`tbl`) to scan with tablet: [tablet 10001, tablet 10003]
    //   2. bucket 1 use OlapScanNode(tableName=`tbl`) to scan with tablet: [tablet 10002, tablet 10004]
    public final Map<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanNodeToTablets;

    public BucketScanSource(Map<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanNodeToTablets) {
        this.bucketIndexToScanNodeToTablets = bucketIndexToScanNodeToTablets;
    }

    @Override
    public int maxParallel(List<ScanNode> scanNodes) {
        // maxParallel is buckets num
        return bucketIndexToScanNodeToTablets.size();
    }

    @Override
    public List<ScanSource> parallelize(List<ScanNode> scanNodes, int instanceNum) {
        // current state, no any instance, we only known how many buckets
        // this worker should process, and the data that this buckets should process:
        //
        // [
        //   bucket 0: {
        //     scanNode1: ScanRanges([tablet_10001, tablet_10004, tablet_10007]),
        //     scanNode2: ScanRanges([tablet_10010, tablet_10013, tablet_10016])
        //   },
        //   bucket 1: {
        //     scanNode1: ScanRanges([tablet_10002, tablet_10005, tablet_10008]),
        //     scanNode2: ScanRanges([tablet_10011, tablet_10014, tablet_10017])
        //   },
        //   bucket 3: {
        //     scanNode1: ScanRanges([tablet_10003, tablet_10006, tablet_10009]),
        //     scanNode2: ScanRanges([tablet_10012, tablet_10015, tablet_10018])
        //   }
        // ]
        List<Entry<Integer, Map<ScanNode, ScanRanges>>> bucketIndexToScanRanges
                = Lists.newArrayList(bucketIndexToScanNodeToTablets.entrySet());

        // separate buckets to instanceNum groups.
        // for example:
        // [
        //   // instance 1 process two buckets
        //   [
        //     bucket 0: {
        //       scanNode1: ScanRanges([tablet_10001, tablet_10004, tablet_10007]),
        //       scanNode2: ScanRanges([tablet_10010, tablet_10013, tablet_10016])
        //     },
        //     bucket 3: {
        //       scanNode1: ScanRanges([tablet_10003, tablet_10006, tablet_10009]),
        //       scanNode2: ScanRanges([tablet_10012, tablet_10015, tablet_10018])
        //     }
        //   ],
        //   // instance 2 process one bucket
        //   [
        //     bucket 1: {
        //       scanNode1: ScanRanges([tablet_10002, tablet_10005, tablet_10008]),
        //       scanNode2: ScanRanges([tablet_10011, tablet_10014, tablet_10017])
        //     }
        //   ]
        // ]
        List<List<Entry<Integer, Map<ScanNode, ScanRanges>>>> scanBucketsPerInstance
                = ListUtil.splitBySize(bucketIndexToScanRanges, instanceNum);

        // rebuild BucketScanSource for each instance
        ImmutableList.Builder<ScanSource> instancesScanSource = ImmutableList.builder();
        for (List<Entry<Integer, Map<ScanNode, ScanRanges>>> oneInstanceScanBuckets : scanBucketsPerInstance) {
            Map<Integer, Map<ScanNode, ScanRanges>> bucketsScanSources = Maps.newLinkedHashMap();
            for (Entry<Integer, Map<ScanNode, ScanRanges>> bucketIndexToScanNodeToScanRange : oneInstanceScanBuckets) {
                Integer bucketIndex = bucketIndexToScanNodeToScanRange.getKey();
                Map<ScanNode, ScanRanges> scanNodeToScanRanges = bucketIndexToScanNodeToScanRange.getValue();
                bucketsScanSources.put(bucketIndex, scanNodeToScanRanges);
            }

            instancesScanSource.add(new BucketScanSource(
                    bucketsScanSources
            ));
        }
        return instancesScanSource.build();
    }

    /** getBucketIndexToScanRanges */
    public Map<Integer, ScanRanges> getBucketIndexToScanRanges(ScanNode scanNode) {
        Map<Integer, ScanRanges> bucketIndexToScanRanges = Maps.newLinkedHashMap();
        for (Entry<Integer, Map<ScanNode, ScanRanges>> entry : bucketIndexToScanNodeToTablets.entrySet()) {
            Integer bucketIndex = entry.getKey();
            Map<ScanNode, ScanRanges> scanNodeToScanRanges = entry.getValue();
            ScanRanges scanRanges = scanNodeToScanRanges.get(scanNode);
            if (scanRanges != null) {
                bucketIndexToScanRanges.put(bucketIndex, scanRanges);
            }
        }

        return bucketIndexToScanRanges;
    }

    /** toString */
    public void toString(StringBuilder str, String prefix) {
        int i = 0;
        String nextIndent = prefix + "  ";
        str.append("[\n");
        for (Entry<Integer, Map<ScanNode, ScanRanges>> entry : bucketIndexToScanNodeToTablets.entrySet()) {
            Integer bucketId = entry.getKey();
            Map<ScanNode, ScanRanges> scanNodeToScanRanges = entry.getValue();
            str.append(prefix).append("  bucket ").append(bucketId).append(": ");
            DefaultScanSource.toString(scanNodeToScanRanges, str, nextIndent);
            if (++i < bucketIndexToScanNodeToTablets.size()) {
                str.append(",\n");
            }
        }
        str.append("\n").append(prefix).append("]");
    }

    @Override
    public boolean isEmpty() {
        return bucketIndexToScanNodeToTablets.isEmpty();
    }
}
