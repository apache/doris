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

import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.JoinOperator;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.statistics.StatisticalType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public abstract class JoinNodeBase extends PlanNode {

    protected final TableRef innerRef;
    protected final JoinOperator joinOp;
    protected final boolean isMark;
    protected ExprSubstitutionMap vSrcToOutputSMap;
    protected List<TupleDescriptor> vIntermediateTupleDescList;

    public JoinNodeBase(PlanNodeId id, String planNodeName, StatisticalType statisticalType,
            PlanNode outer, PlanNode inner, TableRef innerRef) {
        super(id, planNodeName, statisticalType);
        this.innerRef = innerRef;
        tblRefIds.addAll(outer.getTblRefIds());
        tblRefIds.addAll(inner.getTblRefIds());
        children.add(outer);
        children.add(inner);

        // Inherits all the nullable tuple from the children
        // Mark tuples that form the "nullable" side of the outer join as nullable.
        nullableTupleIds.addAll(outer.getNullableTupleIds());
        nullableTupleIds.addAll(inner.getNullableTupleIds());

        joinOp = innerRef.getJoinOp();
        if (joinOp.equals(JoinOperator.FULL_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getOutputTupleIds());
            nullableTupleIds.addAll(inner.getOutputTupleIds());
        } else if (joinOp.equals(JoinOperator.LEFT_OUTER_JOIN)) {
            nullableTupleIds.addAll(inner.getOutputTupleIds());
        } else if (joinOp.equals(JoinOperator.RIGHT_OUTER_JOIN)) {
            nullableTupleIds.addAll(outer.getOutputTupleIds());
        }
        this.isMark = this.innerRef != null && innerRef.isMark();
    }

    public boolean isMarkJoin() {
        return isMark;
    }

    public JoinOperator getJoinOp() {
        return joinOp;
    }

    /**
     * Only for Nereids.
     */
    public JoinNodeBase(PlanNodeId id, String planNodeName,
                        StatisticalType statisticalType, JoinOperator joinOp, boolean isMark) {
        super(id, planNodeName, statisticalType);
        this.innerRef = null;
        this.joinOp = joinOp;
        this.isMark = isMark;
    }

    /**
     * If parent wants to get join node tupleids,
     * it will call this function instead of read properties directly.
     * The reason is that the tuple id of outputTupleDesc the real output tuple id for join node.
     * <p>
     * If you read the properties of @tupleids directly instead of this function,
     * it reads the input id of the current node.
     */
    @Override
    public ArrayList<TupleId> getTupleIds() {
        Preconditions.checkState(tupleIds != null);
        if (outputTupleDesc != null) {
            return Lists.newArrayList(outputTupleDesc.getId());
        }
        return tupleIds;
    }

    @Override
    public List<TupleId> getOutputTupleIds() {
        if (outputTupleDesc != null) {
            return Lists.newArrayList(outputTupleDesc.getId());
        }
        switch (joinOp) {
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                return getChild(0).getOutputTupleIds();
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                return getChild(1).getOutputTupleIds();
            default:
                return tupleIds;
        }
    }

    @Override
    public int getNumInstances() {
        return Math.max(children.get(0).getNumInstances(), children.get(1).getNumInstances());
    }

    /**
     * Used by nereids.
     */
    public void setvIntermediateTupleDescList(List<TupleDescriptor> vIntermediateTupleDescList) {
        this.vIntermediateTupleDescList = vIntermediateTupleDescList;
    }
}
