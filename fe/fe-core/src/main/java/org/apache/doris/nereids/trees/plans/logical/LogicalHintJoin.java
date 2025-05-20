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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.DummyPlan;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * LogicalHintJoin
 */
public class LogicalHintJoin extends DummyPlan {

    private final DistributeHint distributeHint;

    public LogicalHintJoin(DummyPlan leftChild, DummyPlan rightChild, DistributeHint distributeHint) {
        super(ImmutableList.of(leftChild, rightChild));
        this.distributeHint = distributeHint;
    }

    @Override
    public void collectParamsForLeadingHint(List<String> params) {
        children.get(0).collectParamsForLeadingHint(params);
        if (distributeHint.distributeType != DistributeType.NONE) {
            if (distributeHint.distributeType == DistributeType.BROADCAST_RIGHT) {
                params.add(LeadingHint.BROADCAST);
            } else if (distributeHint.distributeType == DistributeType.SHUFFLE_RIGHT) {
                params.add(LeadingHint.SHUFFLE);
            }
        }
        if (children.get(1) instanceof LogicalHintJoin) {
            params.add("(");
            children.get(1).collectParamsForLeadingHint(params);
            params.add(")");
        } else {
            children.get(1).collectParamsForLeadingHint(params);
        }
    }

    @Override
    public DummyPlan withChildren(List<DummyPlan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalHintJoin(children.get(0), children.get(1), distributeHint);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalHintJoin", "hint", distributeHint.getExplainString());
    }
}
