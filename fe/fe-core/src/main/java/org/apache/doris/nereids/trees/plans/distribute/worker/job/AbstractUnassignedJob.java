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

import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.ScanNode;

import com.google.common.collect.ListMultimap;

import java.util.List;
import java.util.Objects;

/** AbstractUnassignedJob */
public abstract class AbstractUnassignedJob
        extends AbstractTreeNode<UnassignedJob> implements UnassignedJob {
    protected final PlanFragment fragment;
    protected final List<ScanNode> scanNodes;
    protected final ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob;

    /** AbstractUnassignedJob */
    public AbstractUnassignedJob(PlanFragment fragment, List<ScanNode> scanNodes,
            ListMultimap<ExchangeNode, UnassignedJob> exchangeToChildJob) {
        super(Utils.fastToImmutableList(exchangeToChildJob.values()));
        this.fragment = Objects.requireNonNull(fragment, "fragment can not be null");
        this.scanNodes = Utils.fastToImmutableList(
                Objects.requireNonNull(scanNodes, "scanNodes can not be null")
        );
        this.exchangeToChildJob
                = Objects.requireNonNull(exchangeToChildJob, "exchangeToChildJob can not be null");
    }

    @Override
    public PlanFragment getFragment() {
        return fragment;
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    @Override
    public ListMultimap<ExchangeNode, UnassignedJob> getExchangeToChildJob() {
        return exchangeToChildJob;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public UnassignedJob withChildren(List<UnassignedJob> children) {
        throw new UnsupportedOperationException();
    }
}
