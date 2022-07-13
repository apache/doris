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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribution;
import org.apache.doris.planner.DataPartition;

import com.google.common.collect.Lists;

/**
 * Spec of data distribution.
 */
public class DistributionSpec {

    private DataPartition dataPartition;

    // TODO: why exist?
    public DistributionSpec() {
    }

    public DistributionSpec(DataPartition dataPartition) {
        this.dataPartition = dataPartition;
    }

    /**
     * TODO: need read ORCA.
     * Whether other `DistributionSpec` is satisfied the current `DistributionSpec`.
     *
     * @param other another DistributionSpec.
     */
    public boolean meet(DistributionSpec other) {
        return false;
    }

    public DataPartition getDataPartition() {
        return dataPartition;
    }

    public void setDataPartition(DataPartition dataPartition) {
        this.dataPartition = dataPartition;
    }

    public GroupExpression addEnforcer(Group child) {
        PhysicalDistribution distribution = new PhysicalDistribution(
                new DistributionSpec(dataPartition), child.getLogicalProperties(), new GroupPlan(child));
        return new GroupExpression(distribution, Lists.newArrayList(child));
    }

    // TODO
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
