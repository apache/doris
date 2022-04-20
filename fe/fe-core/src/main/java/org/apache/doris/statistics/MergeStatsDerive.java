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

package org.apache.doris.statistics;

import com.google.common.base.Preconditions;
import org.apache.doris.planner.MergeNode;
import org.apache.doris.planner.PlanNode;


public class MergeStatsDerive extends BaseStatsDerive {

    public void init(PlanNode node) {
        Preconditions.checkState(node instanceof MergeNode);
        rowCount = ((MergeNode)node).getConstExprLists().size();
    }
    @Override
    public StatsDeriveResult deriveStats() {
        return new StatsDeriveResult(deriveRowCount(), deriveColumnToDataSize(), deriveColumnToNdv());
    }

    @Override
    protected long deriveRowCount() {
        for (StatsDeriveResult child : childrenStatsResult) {
            // ignore missing child rowCount info in the hope it won't matter enough
            // to change the planning outcome
            if (child.getRowCount() > 0) {
                rowCount += child.getRowCount();
            }
        }
        applyConjunctsSelectivity();
        capRowCountAtLimit();
        return rowCount;
    }
}
