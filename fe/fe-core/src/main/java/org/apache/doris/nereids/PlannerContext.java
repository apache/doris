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

package org.apache.doris.nereids;

import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Context used in all stage in Nereids.
 */
public class PlannerContext {
    private final OptimizerContext optimizerContext;
    private final ConnectContext connectContext;
    private final PhysicalProperties physicalProperties;
    private double costUpperBound;
    private Set<Slot> neededSlots;

    /**
     * Constructor of OptimizationContext.
     *
     * @param optimizerContext context includes all data struct used in memo
     * @param connectContext connect context of this query
     * @param physicalProperties target physical properties
     */
    public PlannerContext(
            OptimizerContext optimizerContext,
            ConnectContext connectContext,
            PhysicalProperties physicalProperties) {
        this.optimizerContext = optimizerContext;
        this.connectContext = connectContext;
        this.physicalProperties = physicalProperties;
        this.costUpperBound = Double.MAX_VALUE;
        this.neededSlots = Sets.newHashSet();
    }

    public double getCostUpperBound() {
        return costUpperBound;
    }

    public void setCostUpperBound(double costUpperBound) {
        this.costUpperBound = costUpperBound;
    }

    public OptimizerContext getOptimizerContext() {
        return optimizerContext;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public PhysicalProperties getPhysicalProperties() {
        return physicalProperties;
    }

    public Set<Slot> getNeededAttributes() {
        return neededSlots;
    }
}
