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

import org.apache.doris.common.Status;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.trees.plans.distribute.DistributedPlan;
import org.apache.doris.nereids.trees.plans.distribute.FragmentIdMapping;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/** NereidsCoordinator */
public class NereidsCoordinator implements CoordInterface {
    private NereidsPlanner nereidsPlanner;
    private FragmentIdMapping<DistributedPlan> distributedPlans;

    public NereidsCoordinator(NereidsPlanner nereidsPlanner) {
        this.nereidsPlanner = Objects.requireNonNull(nereidsPlanner, "nereidsPlanner can not be null");
        this.distributedPlans = Objects.requireNonNull(
                nereidsPlanner.getDistributedPlans(), "distributedPlans can not be null"
        );
    }

    @Override
    public void exec() throws Exception {
        // build fragment from leaf to root
    }

    @Override
    public RowBatch getNext() throws Exception {
        RowBatch rowBatch = new RowBatch();
        rowBatch.setEos(true);
        return rowBatch;
    }

    @Override
    public void cancel(Status cancelReason) {

    }

    @Override
    public List<TNetworkAddress> getInvolvedBackends() {
        return ImmutableList.of();
    }
}
