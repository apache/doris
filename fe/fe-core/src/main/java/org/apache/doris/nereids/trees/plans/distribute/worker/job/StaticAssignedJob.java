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

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/** StaticAssignedJob */
public class StaticAssignedJob implements AssignedJob {
    private final int indexInUnassignedJob;
    private final UnassignedJob unassignedJob;
    private final TUniqueId instanceId;
    private final DistributedPlanWorker worker;
    private final ScanSource scanSource;

    public StaticAssignedJob(
            int indexInUnassignedJob, TUniqueId instanceId, UnassignedJob unassignedJob, DistributedPlanWorker worker,
            ScanSource scanSource) {
        this.indexInUnassignedJob = indexInUnassignedJob;
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId can not be null");
        this.unassignedJob = Objects.requireNonNull(unassignedJob, "unassignedJob can not be null");
        this.worker = worker;
        this.scanSource = Objects.requireNonNull(scanSource, "scanSource can not be null");
    }

    @Override
    public int indexInUnassignedJob() {
        return indexInUnassignedJob;
    }

    @Override
    public TUniqueId instanceId() {
        return instanceId;
    }

    @Override
    public UnassignedJob unassignedJob() {
        return unassignedJob;
    }

    @Override
    public DistributedPlanWorker getAssignedWorker() {
        return worker;
    }

    @Override
    public ScanSource getScanSource() {
        return scanSource;
    }

    @Override
    public String toString() {
        return toString(true);
    }

    @Override
    public String toString(boolean showUnassignedJob) {
        StringBuilder scanSourceString = new StringBuilder();
        if (!scanSource.isEmpty()) {
            scanSource.toString(scanSourceString, "  ");
        } else {
            scanSourceString = new StringBuilder("[]");
        }
        StringBuilder str = new StringBuilder(getClass().getSimpleName()).append("(");
        if (showUnassignedJob) {
            str.append("\n  unassignedJob: ").append(unassignedJob).append(",");
        }
        str.append("\n  index: " + indexInUnassignedJob)
                .append(",\n  instanceId: " + DebugUtil.printId(instanceId))
                .append(",\n  worker: " + worker);
        for (Entry<String, String> kv : extraInfo().entrySet()) {
            str.append(",\n  ").append(kv.getKey()).append(": ").append(kv.getValue());
        }

        return str
                .append(",\n  scanSource: " + scanSourceString)
                .append("\n)")
                .toString();
    }

    protected Map<String, String> extraInfo() {
        return ImmutableMap.of();
    }
}
