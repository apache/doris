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

import org.apache.doris.analysis.TupleId;
import org.apache.doris.statistics.StatisticalType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import com.google.common.base.Preconditions;

import java.util.ArrayList;

/**
 * Node that returns an empty result set. Used for planning query blocks with a constant
 * predicate evaluating to false or a limit 0. The result set will have zero rows, but
 * the row descriptor must still include a materialized tuple so that the backend can
 * construct a valid row empty batch.
 */
public class EmptySetNode extends PlanNode {

    public EmptySetNode(PlanNodeId id, ArrayList<TupleId> tupleIds) {
        super(id, tupleIds, "EMPTYSET", StatisticalType.EMPTY_SET_NODE);
        cardinality = 0L;
        Preconditions.checkArgument(tupleIds.size() > 0);
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.EMPTY_SET_NODE;
    }

    @Override
    public int getNumInstances() {
        return 1;
    }
}
