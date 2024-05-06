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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlanFragment.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.nereids.worker.Worker;
import org.apache.doris.nereids.worker.job.AssignedJob;
import org.apache.doris.nereids.worker.job.ScanSource;
import org.apache.doris.nereids.worker.job.StaticAssignedJob;
import org.apache.doris.nereids.worker.job.UnassignedJob;
import org.apache.doris.nereids.worker.job.WorkerScanSource;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

/** NereidsSpecifyInstances */
public abstract class NereidsSpecifyInstances<S extends ScanSource> {
    public final List<WorkerScanSource<S>> workerScanSources;

    public NereidsSpecifyInstances(List<WorkerScanSource<S>> workerScanSources) {
        this.workerScanSources = Objects.requireNonNull(workerScanSources,
                "workerScanSources can not be null");
    }

    public List<AssignedJob> buildAssignedJobs(UnassignedJob unassignedJob) {
        List<AssignedJob> instances = Lists.newArrayListWithCapacity(workerScanSources.size());
        int instanceNum = 0;
        for (WorkerScanSource<S> workerToScanSource : workerScanSources) {
            Worker worker = workerToScanSource.worker;
            ScanSource scanSource = workerToScanSource.scanSource;
            StaticAssignedJob assignedJob = new StaticAssignedJob(instanceNum++, unassignedJob, worker, scanSource);
            instances.add(assignedJob);
        }
        return instances;
    }
}
