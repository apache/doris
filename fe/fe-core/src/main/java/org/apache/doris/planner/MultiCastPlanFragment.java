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

package org.apache.doris.planner;

import org.apache.doris.thrift.TResultSinkType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/*
 * MultiCast plan fragment.
 */
public class MultiCastPlanFragment extends PlanFragment {
    private final List<ExchangeNode> destNodeList = Lists.newArrayList();

    public List<ExchangeNode> getDestNodeList() {
        return destNodeList;
    }

    public MultiCastPlanFragment(PlanFragment planFragment) {
        super(planFragment.getFragmentId(), planFragment.getPlanRoot(), planFragment.getDataPartition());
        // Use random, only send to self
        //this.outputPartition = DataPartition.RANDOM;
        this.children.addAll(planFragment.getChildren());
    }

    public List<PlanFragment> getDestFragmentList() {
        return destNodeList.stream().map(PlanNode::getFragment).collect(Collectors.toList());
    }

    public ExchangeNode getDestNode(int index) {
        return destNodeList.get(index);
    }

    public void createDataSink(TResultSinkType resultSinkType) {
        if (sink != null) {
            return;
        }

        Preconditions.checkState(!destNodeList.isEmpty(), "MultiCastPlanFragment don't support return result");

        MultiCastDataSink multiCastDataSink = new MultiCastDataSink();
        this.sink = multiCastDataSink;

        for (ExchangeNode f : destNodeList) {
            DataStreamSink streamSink = new DataStreamSink(f.getId());
            streamSink.setPartition(DataPartition.RANDOM);
            streamSink.setFragment(this);
            //streamSink.setOutputColumnIds(f.getProjectList());
            multiCastDataSink.getDataStreamSinks().add(streamSink);
            multiCastDataSink.getDestinations().add(Lists.newArrayList());
        }
    }

    @Override
    public PlanFragment getDestFragment() {
        Preconditions.checkState(false);
        return null;
    }

    @Override
    public void setDestination(ExchangeNode destNode) {
        Preconditions.checkState(false);
    }

    @Override
    public void setParallelExecNum(int parallelExecNum) {
        Preconditions.checkState(false);
    }

    @Override
    public int getNumNodes() {
        Preconditions.checkState(false);
        return 0;
    }

    @Override
    public void setOutputPartition(DataPartition outputPartition) {
        Preconditions.checkState(false);
    }

    //@Override
    //public void setSink(DataSink sink) {
    //    Preconditions.checkState(false);
    //}

}
