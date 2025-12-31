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

package org.apache.doris.qe;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.PipelineDistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.AssignedJob;
import org.apache.doris.nereids.trees.plans.distribute.worker.job.LocalShuffleBucketJoinAssignedJob;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

public class LocalShuffleWithBucketJoinTest extends TestWithFeService {
    private static final int beNum = 3;
    private static final int bucketNum = 20;

    @Override
    protected int backendNum() {
        return beNum;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("CREATE TABLE `test_tbl` (\n"
                + "  `id` int\n"
                + ") ENGINE=OLAP\n"
                + "distributed by hash(id) buckets " + bucketNum + "\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ")");
    }

    @Test
    public void test() throws Exception {
        int parallelPipelineTaskNum = 10;
        executeNereidsSql("set parallel_pipeline_task_num=" + parallelPipelineTaskNum);
        StmtExecutor stmtExecutor = executeNereidsSql(
                "explain distributed plan select * from test_tbl a join test_tbl b on a.id = b.id");
        NereidsPlanner planner = (NereidsPlanner) stmtExecutor.planner();
        List<DistributedPlan> distributedPlans = planner.getDistributedPlans().valueList();
        Assertions.assertEquals(1, distributedPlans.size());
        PipelineDistributedPlan distributedPlan
                = (PipelineDistributedPlan) distributedPlans.get(0);
        List<AssignedJob> instances = distributedPlan.getInstanceJobs();
        Assertions.assertEquals(beNum * parallelPipelineTaskNum, instances.size());
        Assertions.assertTrue(instances.stream().allMatch(LocalShuffleBucketJoinAssignedJob.class::isInstance));

        long assignedBucketInstanceNum = instances.stream().map(LocalShuffleBucketJoinAssignedJob.class::cast)
                .filter(a -> !a.getAssignedJoinBucketIndexes().isEmpty())
                .count();
        Assertions.assertEquals(bucketNum, assignedBucketInstanceNum);

        Multimap<DistributedPlanWorker, AssignedJob> workerToInstances = ArrayListMultimap.create();
        for (AssignedJob instance : instances) {
            workerToInstances.put(instance.getAssignedWorker(), instance);
        }
        for (Collection<AssignedJob> instancePerBe : workerToInstances.asMap().values()) {
            Assertions.assertEquals(parallelPipelineTaskNum, instancePerBe.size());
        }
    }
}
