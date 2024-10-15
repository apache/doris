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

package org.apache.doris.nereids.trees.plans.distribute.worker;

import org.apache.doris.nereids.trees.plans.distribute.worker.job.UnassignedScanBucketOlapTableJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.UninstancedScanSource;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TScanRangeLocation;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;

import java.util.List;
import java.util.Map;

/** ScanWorkerSelector */
public interface ScanWorkerSelector {
    DistributedPlanWorkerManager getWorkerManager();

    DistributedPlanWorker selectMinWorkloadWorker(List<DistributedPlanWorker> workers);

    // for a scan node, select replica for each scan range(denote tablet if the ScanNode is OlapScanNode),
    // use the replica location to build a worker execute the instance
    Map<DistributedPlanWorker, UninstancedScanSource> selectReplicaAndWorkerWithoutBucket(ScanNode scanNode);

    // return
    //   key:   Worker, the backend which will process this fragment
    //   value.key: Integer, the bucket index, from 0 to (bucket_num - 1)
    //              for example, create table statement contains: distributed by hash(id) buckets 10,
    //              the bucket index will from 0 to 9
    //   value.value.key:   ScanNode, which ScanNode the worker will process scan task
    //   value.value.value: ScanRanges, the tablets in current bucket,
    //                      for example, colocate table `tbl` has 2 range partitions:
    //                      p1 values[(1), (10)) and p2 values[(10), 11) with integer partition column part,
    //                      and distributed by hash(id) buckets 10. And, so, there has 10 buckets from bucket 0 to
    //                      bucket 9, and every bucket contains two tablets, because there are two partitions.
    Map<DistributedPlanWorker, UninstancedScanSource> selectReplicaAndWorkerWithBucket(
            UnassignedScanBucketOlapTableJob unassignedJob);

    static TScanRangeParams buildScanReplicaParams(
            TScanRangeLocations tabletLocation, TScanRangeLocation replicaLocation) {
        TScanRangeParams replicaParam = new TScanRangeParams();
        replicaParam.scan_range = tabletLocation.scan_range;
        // Volume is optional, so we need to set the value and the is-set bit
        replicaParam.setVolumeId(replicaLocation.volume_id);
        return replicaParam;
    }
}
